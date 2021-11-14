import sqlite3
from contextlib import closing
from os.path import exists
from os import remove

db_name = './assets/data/nba_stats.db'

if exists(db_name):
    remove(db_name)

def runQueries(db_name,query_list):
    with closing(sqlite3.connect(db_name)) as connection:
        with connection:
            with closing(connection.cursor()) as cursor:
                for query in query_list:
                    print(query)
                    cursor.execute(query)


query_list = [
"""
/*
    This is manually inserted at the bottom of this script
    There are 5 season types in the NBA API:
    1 = Pre Season
    2 = Regular Season
    3 = All Star
    4 = Playoffs
    5 = Showcase (only for g league currently)
    I don't include all star as I'm only intereted in the others
*/
CREATE TABLE season_types
(
    id INTEGER PRIMARY KEY,
    season_type_name TEXT
);
""",
"""
/*
    This is manually inserted at the beginning of each ETL run
    The id is the 4 digit year
    The season_name is YYYY-YY
    NOTE: For WNBA they only play a season within 1 calendar year,
    so API calls use the 4 digit id instead of the season_name
*/
CREATE TABLE seasons
(
    id INTEGER PRIMARY KEY,
    season_name TEXT
);
""",
"""
/*
    This table is manually inserted at the bottom of this script
    The leagues are:
    00 = NBA
    10 = WNBA
    20 = GLEAGUE
*/
CREATE TABLE leagues
(
    id TEXT PRIMARY KEY,
    league_name TEXT
);
""",
"""
/*
    These are the team_ids of each team in the NBA API
    These get populated from the list of teams in the games data
    It sources from the commonteamyears endpoint
*/
CREATE TABLE teams
(
    id INTEGER PRIMARY KEY
);
""",
"""
/*
    This is metadata about a player
    It sources from the commonplayerinfo endpoint
*/
CREATE TABLE players
(
    id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    birthdate DATETIME,
    school TEXT,
    country TEXT,
    draft_year INTEGER,
    draft_round INTEGER,
    draft_number INTEGER
);
""",
"""
/*
    This is a table with information about teams
    It is unique at the league_id, season_id, team_id level
    This allows for teams moving or changing names between for seasons
    It sources from the teaminfocommon endpoint
*/
CREATE TABLE league_season_teams
(
    id INTEGER PRIMARY KEY,
    league_id TEXT,
    season_id INTEGER,
    team_id INTEGER,
    team_city TEXT,
    team_name TEXT,
    team_abbreviation TEXT,
    team_conference TEXT,
    team_division TEXT,
    team_code TEXT,
    FOREIGN KEY (league_id) REFERENCES leagues(id),
    FOREIGN KEY (season_id) REFERENCES seasons(id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);
""",
"""
/*
    This table acts as a game header table
    It shows which league, what season and type, and the date of the game
    This sources from the leaguegamelog endpoint
*/
CREATE TABLE games
(
    id TEXT PRIMARY KEY,
    league_id TEXT,
    season_id INTEGER,
    season_type_id INTEGER,
    game_date DATE,
    FOREIGN KEY (league_id) REFERENCES leagues(id),
    FOREIGN KEY (season_id) REFERENCES seasons(id),
    FOREIGN KEY (season_type_id) REFERENCES season_types(id)
);
""",
"""
/*
    This table also sources from the leaguegamelog endpoint
    It however, has 2 rows for each game
    1 row for the home team and 1 row for the road team
    It has the stats for that team in the row
*/
CREATE TABLE game_team_stats
(
    id INTEGER PRIMARY KEY,
    game_id TEXT,
    team_id INTEGER,
    home_away TEXT,
    win_loss TEXT,
    fgm INTEGER,
    fga INTEGER,
    fg_pct DECIMAL,
    fg3m INTEGER,
    fg3a INTEGER,
    fg3_pct DECIMAL,
    ftm INTEGER,
    fta INTEGER,
    ft_pct DECIMAL,
    oreb INTEGER,
    dreb INTEGER,
    reb INTEGER,
    ast INTEGER,
    stl INTEGER,
    blk INTEGER,
    tov INTEGER,
    pf INTEGER,
    pts INTEGER,
    plus_minus INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(id),
    FOREIGN KEY (team_id) REFERENCES teams(id)
);
""",
"""
/*
    This table holds the play by play of the game
    It is a pretty cool table
    One future improvement:
        create an enum of event_message type and action_type
    It sources from the playbyplayv2 endpoint
*/
CREATE TABLE game_events
(
    id INTEGER PRIMARY KEY,
    game_id TEXT,
    event_number INTEGER,
    event_message_type INTEGER,
    event_message_action_type INTEGER,
    period INTEGER,
    play_clock TEXT,
    home_description TEXT,
    neutral_description TEXT,
    visitor_description TEXT,
    score VARCHAR,
    score_margin INTEGER,
    person_1_type INTEGER,
    person_1_id INTEGER,
    person_1_team_id INTEGER,
    person_2_type INTEGER,
    person_2_id INTEGER,
    person_2_team_id INTEGER,
    person_3_type INTEGER,
    person_3_id INTEGER,
    person_3_team_id INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(id),
    FOREIGN KEY (person_1_id) REFERENCES players(id),
    FOREIGN KEY (person_1_team_id) REFERENCES teams(id),
    FOREIGN KEY (person_2_id) REFERENCES players(id),
    FOREIGN KEY (person_2_team_id) REFERENCES teams(id),
    FOREIGN KEY (person_3_id) REFERENCES players(id),
    FOREIGN KEY (person_3_team_id) REFERENCES teams(id)
);
""",
"""
/*
    This table holds the shot details from each game
    It sources from the shotchartdetail endpoint
*/
CREATE TABLE game_shot_charts
(
    id INTEGER PRIMARY KEY,
    game_id TEXT,
    game_events_event_number INTEGER,
    player_id INTEGER,
    team_id INTEGER,
    period INTEGER,
    minutes_remaining INTEGER,
    seconds_remaining INTEGER,
    event_type TEXT,
    action_type TEXT,
    shot_type TEXT,
    shot_zone_basic TEXT,
    shot_zone_area TEXT,
    shot_zone_range TEXT,
    shot_distance INTEGER,
    loc_x INTEGER,
    loc_y INTEGER,
    shot_attempted_flag INTEGER,
    shot_made_flag INTEGER,
    FOREIGN KEY (game_id) REFERENCES games(id),
    FOREIGN KEY (game_events_event_number) REFERENCES game_play_by_play_events(event_number)
);
""",
"""
INSERT INTO season_types (id, season_type_name) VALUES (1,'Pre Season'),(2,'Regular Season'),(4,'Playoffs'),(5,'Showcase');
""",
"""
INSERT INTO leagues (id, league_name) VALUES ('00','NBA'),('10','WNBA'),('20','GLEAGUE')
""",
]

runQueries(db_name,query_list)
