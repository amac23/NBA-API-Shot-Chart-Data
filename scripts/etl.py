import argparse
from datetime import date, timedelta
import json
import sqlite3
from contextlib import closing
import pandas as pd
import requests
from time import sleep
from random import randint


def parseArguments():
    # argparse to get the ds the run
    parser = argparse.ArgumentParser()
    parser.add_argument('-ds',help='date used to run etl (YYYY-MM-DD)',type=date.fromisoformat,default=date.today() - timedelta(1))
    parser.add_argument('-league',help='league used to run etl (NBA,WNBA,GLEAGUE)',choices=['NBA','WNBA','GLEAGUE'],default='NBA')
    args = parser.parse_args()
    return args

def getSeason(ds,league_name):
    ds_year = ds.year
    if league_name in ['NBA','GLEAGUE']:
        if ds.month <= 9:
            ds_year -= 1
    ds_season = str(ds_year) + '-' + str(ds_year+1)[-2:]
    return ds_season

def getData(url,params):
    # headers for calling the api
    headers = {
        'Host': 'stats.nba.com',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:72.0) Gecko/20100101 Firefox/72.0',
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'x-nba-stats-origin': 'stats',
        'x-nba-stats-token': 'true',
        'Connection': 'keep-alive',
        'Referer': 'https://stats.nba.com/',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }
    sleep(randint(1,5))
    response = requests.get(url,params=params,headers=headers)
    response_json = json.loads(response.content)

    headers = response_json['resultSets'][0]['headers']
    rows = response_json['resultSets'][0]['rowSet']

    df = pd.DataFrame(rows, columns=headers).rename(str.lower,axis='columns')
    print(df.head())

    return df

def insertQuery(db_name,query,values):
    with closing(sqlite3.connect(db_name)) as connection:
        with connection:
            with closing(connection.cursor()) as cursor:
                print(query)
                cursor.execute(query,values)

def readQuery(db_name,query,values):
    with closing(sqlite3.connect(db_name)) as connection:
        with connection:
            df = pd.read_sql_query(sql=query, con=connection, params=values)
            print(query)
            print(df.head())
            return df

def getID(db_name,table,field,value):
    query = f"""
    SELECT
        id
    FROM {table}
    WHERE
        {field} = ?;
    """
    values = (table,field,value)
    with closing(sqlite3.connect(db_name)) as connection:
        with connection:
            df = pd.read_sql_query(sql=query, con=connection, params=[value])
            print(query)
            print(df.head())
            return df['id'][0]

def insertYear(db_name,season_id,season_name):
    query = """
    SELECT
        id
    FROM seasons
    WHERE id = ?;
    """
    seasons_df = readQuery(db_name,query,[season_id])

    if seasons_df.empty:
        query = """
        INSERT INTO seasons
        (id, season_name)
        VALUES
        (?,?);
        """
        insertQuery(db_name,query,[season_id,season_name])

def insertTeams(db_name,season_id,season_name,league_id):
    url = 'https://stats.nba.com/stats/commonteamyears'
    params = {
        'LeagueID':league_id,
    }
    df = getData(url,params)
    df = df[(df['min_year'] <= str(season_id)) & (df['max_year'] >= str(season_id))][['team_id']]

    query = """
    SELECT
        id AS team_id
    FROM teams;
    """
    teams_df = readQuery(db_name,query,[])

    query = """
    SELECT
        team_id
    FROM league_season_teams
    WHERE
        league_id = ?
        AND season_id = ?;
    """
    league_season_teams_df = readQuery(db_name,query,[league_id,season_id])

    df = df.merge(teams_df, on='team_id', how='left', indicator=True).rename(columns={'_merge':'teams'})
    df = df.merge(league_season_teams_df, on='team_id', how='left', indicator=True).rename(columns={'_merge':'league_season_teams'})

    for index,row in df.iterrows():
        team_id = row['team_id']
        teams = row['teams']
        league_season_teams = row['league_season_teams']

        if teams == 'left_only':
            #insert to teams
            query = """
            INSERT INTO teams
            (id)
            VALUES
            (?);
            """
            insertQuery(db_name,query,[team_id])

        if league_season_teams == 'left_only':
            #get team info and insert to league_season_teams
            url = 'https://stats.nba.com/stats/teaminfocommon'
            params = {
                'TeamID':str(team_id),
                'LeagueID':league_id,
                'Season': season_name,
            }
            team_info_df = getData(url,params)

            query = """
            INSERT INTO league_season_teams
            (league_id,season_id,team_id,team_city,team_name,team_abbreviation,team_conference,team_division,team_code)
            VALUES
            (?,?,?,?,?,?,?,?,?);
            """
            insertQuery(db_name,query,[league_id,season_id,team_id,team_info_df['team_city'][0],team_info_df['team_name'][0],team_info_df['team_abbreviation'][0],team_info_df['team_conference'][0],team_info_df['team_division'][0],team_info_df['team_code'][0]])

def insertGames(db_name,ds,season_id,season_name,league_id):
    query = """
    SELECT
        season_type_name
    FROM season_types;
    """
    season_type_names = readQuery(db_name,query,[])['season_type_name'].to_list()
    print(season_type_names)

    for season_type_name in season_type_names:
        season_type_id = int(getID(db_name,'season_types','season_type_name',season_type_name))
        print(season_type_id)

        url = 'https://stats.nba.com/stats/leaguegamelog'
        params = {
            'Counter':0,
            'Direction':'ASC',
            'LeagueID':league_id,
            'PlayerOrTeam':'T',
            'Season':season_name,
            'SeasonType':season_type_name,
            'Sorter':'DATE',
            'DateFrom':ds,
            'DateTo':ds,
        }
        df = getData(url,params)

        if not df.empty:
            # delete from all games tables
            query = """
            DELETE
            FROM game_shot_charts
            WHERE
                game_id IN (
                    SELECT
                        id
                    FROM games
                    WHERE
                        games.game_date = ?
                        AND games.league_id = ?
                        AND games.season_id = ?
                        AND games.season_type_id = ?
                );
            """
            insertQuery(db_name,query,[ds,league_id,season_id,season_type_id])

            query = """
            DELETE
            FROM game_events
            WHERE
                game_id IN (
                    SELECT
                        id
                    FROM games
                    WHERE
                        games.game_date = ?
                        AND games.league_id = ?
                        AND games.season_id = ?
                        AND games.season_type_id = ?
                );
            """
            insertQuery(db_name,query,[ds,league_id,season_id,season_type_id])

            query = """
            DELETE
            FROM game_team_stats
            WHERE
                game_id IN (
                    SELECT
                        id
                    FROM games
                    WHERE
                        games.game_date = ?
                        AND games.league_id = ?
                        AND games.season_id = ?
                        AND games.season_type_id = ?
                );
            """
            insertQuery(db_name,query,[ds,league_id,season_id,season_type_id])

            query = """
            DELETE
            FROM games
            WHERE
                games.game_date = ?
                AND games.league_id = ?
                AND games.season_id = ?
                AND games.season_type_id = ?;
            """
            insertQuery(db_name,query,[ds,league_id,season_id,season_type_id])

            for index, row in df.iterrows():
                game_id = row['game_id']
                home_away = 'home'
                if '@' in row['matchup']:
                    home_away = 'away'
                    query = """
                    INSERT INTO games
                    (id,league_id,season_id,season_type_id,game_date)
                    VALUES
                    (?,?,?,?,?);
                    """
                    insertQuery(db_name,query,[game_id,league_id,season_id,season_type_id,ds])

                    # insert into game_team_stats
                    query = """
                    INSERT INTO game_team_stats
                    (game_id,team_id,home_away,win_loss,fgm,fga,fg_pct,fg3m,fg3a,fg3_pct,ftm,fta,ft_pct,oreb,dreb,reb,ast,stl,blk,tov,pf,pts,plus_minus)
                    VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """
                    insertQuery(db_name,query,[game_id,row['team_id'],home_away,row['wl'],row['fgm'],row['fga'],row['fg_pct'],row['fg3m'],row['fg3a'],row['fg3_pct'],row['ftm'],row['fta'],row['ft_pct'],row['oreb'],row['dreb'],row['reb'],row['ast'],row['stl'],row['blk'],row['tov'],row['pf'],row['pts'],row['plus_minus']])

                    # call playbyplay
                    url = 'https://stats.nba.com/stats/playbyplayv2'
                    params = {
                        'GameID':game_id,
                        'StartPeriod':'0',
                        'EndPeriod':'0',
                    }
                    pbp_df = getData(url,params)

                    # check for players and insert...
                    query = """
                    SELECT
                        id AS player_id
                    FROM players;
                    """
                    players_df = readQuery(db_name,query,[])

                    pbp_players_df = pd.concat(
                        [
                            pbp_df[pbp_df['person1type'].isin([4,5])][['player1_id']].rename(columns={'player1_id':'player_id'}),
                            pbp_df[pbp_df['person2type'].isin([4,5])][['player2_id']].rename(columns={'player2_id':'player_id'}),
                            pbp_df[pbp_df['person3type'].isin([4,5])][['player3_id']].rename(columns={'player3_id':'player_id'}),
                        ],
                        ignore_index=True
                    ).drop_duplicates()

                    pbp_players_df = pbp_players_df.merge(players_df, on='player_id', how='left', indicator=True).rename(columns={'_merge':'players'})
                    pbp_players_df = pbp_players_df[pbp_players_df['players'] == 'left_only']

                    for index, row in pbp_players_df.iterrows():
                        player_id = row['player_id']

                        url = 'https://stats.nba.com/stats/commonplayerinfo'
                        params = {
                            'LeagueID':league_id,'PlayerID':player_id
                        }
                        player_df = getData(url,params)

                        query = """
                        INSERT INTO players
                        (id,first_name,last_name,birthdate,school,country,draft_year,draft_round,draft_number)
                        VALUES
                        (?,?,?,?,?,?,?,?,?);
                        """
                        insertQuery(db_name,query,[player_id,player_df['first_name'][0],player_df['last_name'][0],player_df['birthdate'][0],player_df['school'][0],player_df['country'][0],player_df['draft_year'][0],player_df['draft_round'][0],player_df['draft_number'][0]])

                    # insert into game_events
                    for index, row in pbp_df.iterrows():
                        query = """
                        INSERT INTO game_events
                        (game_id,event_number,event_message_type,event_message_action_type,period,play_clock,home_description,neutral_description,visitor_description,score,score_margin,person_1_type,person_1_id,person_1_team_id,person_2_type,person_2_id,person_2_team_id,person_3_type,person_3_id,person_3_team_id)
                        VALUES
                        (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                        """
                        insertQuery(db_name,query,[game_id,row['eventnum'],row['eventmsgtype'],row['eventmsgactiontype'],row['period'],row['pctimestring'],row['homedescription'],row['neutraldescription'],row['visitordescription'],row['score'],row['scoremargin'],row['person1type'],row['player1_id'],row['player1_team_id'],row['person2type'],row['player2_id'],row['player2_team_id'],row['person3type'],row['player3_id'],row['player3_team_id'],])

                else:
                    # insert into game_team_stats
                    query = """
                    INSERT INTO game_team_stats
                    (game_id,team_id,home_away,win_loss,fgm,fga,fg_pct,fg3m,fg3a,fg3_pct,ftm,fta,ft_pct,oreb,dreb,reb,ast,stl,blk,tov,pf,pts,plus_minus)
                    VALUES
                    (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                    """
                    insertQuery(db_name,query,[game_id,row['team_id'],home_away,row['wl'],row['fgm'],row['fga'],row['fg_pct'],row['fg3m'],row['fg3a'],row['fg3_pct'],row['ftm'],row['fta'],row['ft_pct'],row['oreb'],row['dreb'],row['reb'],row['ast'],row['stl'],row['blk'],row['tov'],row['pf'],row['pts'],row['plus_minus']])

            # call shotchart
            url = 'https://stats.nba.com/stats/shotchartdetail'
            params = {
                'ContextMeasure': 'FGA',
                'LastNGames': 0,
                'LeagueID': league_id,
                'Month': 0,
                'OpponentTeamID': 0,
                'Period': 0,
                'PlayerID': 0,
                'SeasonType': season_type_name,
                'TeamID': 0,
                'VsDivision': '',
                'VsConference': '',
                'SeasonSegment': '',
                'RookieYear': '',
                'PlayerPosition': '',
                'Outcome': '',
                'Location': '',
                'GameSegment': '',
                'GameID': '',
                'DateFrom': ds,
                'DateTo': ds
            }
            sc_df = getData(url,params)

            # insert into game_shot_charts
            for index, row in sc_df.iterrows():
                query = """
                INSERT INTO game_shot_charts
                (game_id,game_events_event_number,player_id,team_id,period,minutes_remaining,seconds_remaining,event_type,action_type,shot_type,shot_zone_basic,shot_zone_area,shot_zone_range,shot_distance,loc_x,loc_y,shot_attempted_flag,shot_made_flag)
                VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);
                """
                insertQuery(db_name,query,[row['game_id'],row['game_event_id'],row['player_id'],row['team_id'],row['period'],row['minutes_remaining'],row['seconds_remaining'],row['event_type'],row['action_type'],row['shot_type'],row['shot_zone_basic'],row['shot_zone_area'],row['shot_zone_range'],row['shot_distance'],row['loc_x'],row['loc_y'],row['shot_attempted_flag'],row['shot_made_flag'],])

            # exit loop because found the season type that had games
            break



if __name__ == '__main__':
    # name of sqlite3 database
    db_name = './assets/data/nba_stats.db'

    args = parseArguments()

    # league_id for NBA
    league_name = args.league
    league_id = getID(db_name,'leagues','league_name',league_name)
    print(league_id)

    # get the ds from args used to run file and get the active season of the ds
    ds = args.ds
    season_name = getSeason(ds,league_name)
    season_id = int(season_name[0:4])
    print(ds,season_name,season_id)
    # insert the season into the seasons table if not already there
    insertYear(db_name,season_id,season_name)

    # WNBA season is only one year, so the season_name isn't YYYY-YY, just YYYY
    if league_name == 'WNBA':
        season_name = str(season_id)

    # insert the teams into the teams and league_season_teams tables if not already there
    insertTeams(db_name,season_id,season_name,league_id)

    # insert into games
    insertGames(db_name,ds,season_id,season_name,league_id)
