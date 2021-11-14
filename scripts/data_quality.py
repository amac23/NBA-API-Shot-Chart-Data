import json
import sqlite3
from contextlib import closing
import pandas as pd

def readQuery(db_name,query,values):
    with closing(sqlite3.connect(db_name)) as connection:
        with connection:
            df = pd.read_sql_query(sql=query, con=connection, params=values)
            print(query)
            return df

db_name = './assets/data/nba_stats.db'

# Is the number of teams right for each league in a season?
query = """
SELECT
    league_id,
    season_id,
    count(team_id) AS team_count,
    CASE WHEN league_id = '00' THEN 30 -- NBA
         WHEN league_id = '10' THEN 12 -- WNBA
         WHEN league_id = '20' THEN 28 -- GLEAGUE
    END AS expected_team_count
FROM league_season_teams
WHERE season_id = 2021
GROUP BY
    league_id,
    season_id
"""
print(readQuery(db_name,query,[]))

# Is the number of games right for each league for a set of dates?
query = """
SELECT
    league_id,
    season_id,
    season_type_id,
    count(id) AS game_count,
    CASE WHEN league_id = '00' THEN 173 -- NBA regular season games
         WHEN league_id = '10' THEN 17 -- WNBA playoff games
         WHEN league_id = '20' THEN 6 -- GLEAGUE showcase games
    END AS expected_game_count
FROM games
WHERE
    (league_id == '00' AND game_date BETWEEN '2021-10-19' AND '2021-11-11')
    OR
    (league_id == '10' AND game_date BETWEEN '2021-09-23' AND '2021-10-17')
    OR
    (league_id == '20' AND game_date = '2021-11-11')
GROUP BY
    league_id,
    season_id,
     season_type_id
"""
print(readQuery(db_name,query,[]))

# Does the number of games match the number of games in the stats tables
query = """
SELECT
    'games' AS data_source,
    COUNT(id) AS game_count
FROM games

UNION ALL

SELECT
    'game_team_stats' AS data_source,
    COUNT(DISTINCT game_id) AS game_count
FROM game_team_stats

UNION ALL

SELECT
    'game_events' AS data_source,
    COUNT(DISTINCT game_id) AS game_count
FROM game_events

UNION ALL

SELECT
    'game_shot_charts' AS data_source,
    COUNT(DISTINCT game_id) AS game_count
FROM game_shot_charts
"""
print(readQuery(db_name,query,[]))

# Does the sum of scores in the game_team_stats equal
# the sum of scores in the game_events
query = """
SELECT
    'game_team_stats' AS data_source,
    sum(pts) AS score
FROM game_team_stats

UNION ALL

SELECT
    'game_events' AS data_source,
    SUM(
        CASE WHEN event_message_type = 1 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%3PT%' THEN 3
        WHEN event_message_type = 1 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE '%3PT%' THEN 2
        WHEN event_message_type = 3 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%' THEN 1
        ELSE 0 END
    ) AS score
FROM game_events
"""
print(readQuery(db_name,query,[]))

# THE ABOVE CHECK IS A LITTLE OFF???
# AFTER LOOKING SOME FT ATTEMPTS IN G LEAGUE SAY 2 OR 3 PTS???
# https://bleacherreport.com/articles/2855449-g-league-to-test-single-free-throw-worth-1-2-or-3-points-depending-on-shot
# AAAAHHHH - really didn't know about that rule change
# See below for updated logic for g league free throws
query = """
SELECT
    'game_team_stats' AS data_source,
    sum(pts) AS score
FROM game_team_stats

UNION ALL

SELECT
    'game_events' AS data_source,
    SUM(
        CASE
            WHEN event_message_type = 1
                AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%3PT%' THEN 3
            WHEN event_message_type = 1
                AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE '%3PT%' THEN 2
            WHEN event_message_type = 3
                AND league_id <> '20'
                AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%' THEN 1
            WHEN event_message_type = 3
                 AND league_id = '20'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%1PT%'THEN 1
            WHEN event_message_type = 3
                 AND league_id = '20'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%2PT%'THEN 2
            WHEN event_message_type = 3
                 AND league_id = '20'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%3PT%'THEN 3
        ELSE 0 END
    ) AS score
FROM game_events
INNER JOIN games
    ON game_events.game_id = games.id
"""
print(readQuery(db_name,query,[]))

# Closer, but not quite, here is a list of games that is off
query = """
WITH game_team_stats_cte AS
(
    SELECT
        game_id,
        'game_team_stats' AS data_source,
        sum(pts) AS score
    FROM game_team_stats
    GROUP BY
        game_id
),
game_events_cte AS
(
    SELECT
        game_id,
        'game_events' AS data_source,
        SUM(
            CASE
                WHEN event_message_type = 1
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%3PT%' THEN 3
                WHEN event_message_type = 1
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE '%3PT%' THEN 2
                WHEN event_message_type = 3
                    AND league_id <> '20'
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%' THEN 1
                WHEN event_message_type = 3
                    AND league_id = '20'
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%1PT%'THEN 1
                WHEN event_message_type = 3
                    AND league_id = '20'
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%2PT%'THEN 2
                WHEN event_message_type = 3
                    AND league_id = '20'
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                    AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%3PT%'THEN 3
            ELSE 0 END
        ) AS score
    FROM game_events
    INNER JOIN games
        ON game_events.game_id = games.id
    GROUP BY
        game_id
)
SELECT
    game_team_stats_cte.game_id,
    games.league_id,
    game_team_stats_cte.score AS gts_score,
    game_events_cte.score AS ge_score
FROM game_team_stats_cte
INNER JOIN game_events_cte
    ON game_team_stats_cte.game_id = game_events_cte.game_id
INNER JOIN games
    ON game_team_stats_cte.game_id = games.id
WHERE
    game_team_stats_cte.score <> game_events_cte.score

"""
print(readQuery(db_name,query,[]))

# There is one NBA game and 6 G League games...
query = """
SELECT
    game_id,
    event_number,
    COUNT(*) AS event_count
FROM game_events
GROUP BY
    game_id,
    event_number
HAVING
    COUNT(*) > 1
"""
print(readQuery(db_name,query,[]))

# THE NBA GAME IS A DUPLICATE EVENT NUMBER - SHOULD DELETE ONE
# THE G LEAGUE GAMES NOT SURE YET....
# LOOKS LIKE IN THE LAST 2 MINUTES OF THE GAME THEY DO NORMAL FTS
# IN THE BOX SCORE THAT DOESN'T SHOW AS 1PT... NEED TO UPDATE CASE STATEMENT

query = """
SELECT
    'game_team_stats' AS data_source,
    sum(pts) AS score
FROM game_team_stats

UNION ALL

SELECT
    'game_events' AS data_source,
    SUM(
        CASE
            WHEN event_message_type = 1
                AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%3PT%' THEN 3
            WHEN event_message_type = 1
                AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE '%3PT%' THEN 2
            WHEN event_message_type = 3
                AND league_id <> '20'
                AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%' THEN 1
            WHEN event_message_type = 3
                 AND league_id = '20'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%1PT%'THEN 1
            WHEN event_message_type = 3
                 AND league_id = '20'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%2PT%'THEN 2
            WHEN event_message_type = 3
                 AND league_id = '20'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) LIKE '%3PT%'THEN 3
            WHEN event_message_type = 3
                 AND league_id = '20'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE 'MISS%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE '%1PT%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE '%2PT%'
                 AND (COALESCE(home_description,'') || COALESCE(visitor_description,'')) NOT LIKE '%3PT%' THEN 1
        ELSE 0 END
    ) AS score
FROM game_events
INNER JOIN games
    ON game_events.game_id = games.id
"""
print(readQuery(db_name,query,[]))

# AWESOME THAT DID IT!!!! NOW ONLY OFF BY THE 3 PTS THAT IS A DUPLICATE
