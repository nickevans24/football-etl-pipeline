# -*- coding: utf-8 -*-
"""
Created on Mon Jul 22 17:55:00 2024

@author: skybl
"""

import os
import requests
import pandas as pd
import numpy as np
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv
import time
from iteration_utilities import unique_everseen
from fixtures_yesterday import fixtures_yesterday

load_dotenv()
RAPIDAPI_KEY = os.getenv('RAPIDAPI_KEY')
HOST = os.getenv('HOST_AIVEN')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE_AIVEN')
MYSQL_USERNAME = os.getenv('MYSQL_USERNAME_AIVEN')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD_AIVEN')
MYSQL_PORT = os.getenv('MYSQL_PORT_AIVEN')

fixtures = fixtures_yesterday()

def replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text

def get_matches(params):

    headers = {
        'X-RapidAPI-key': RAPIDAPI_KEY,
        'X-RapidAPI-host': 'api-football-v1.p.rapidapi.com'}

    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"

    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()

    except requests.exceptions.HTTPError as http_error_message:
        print(f"Http Error: {http_error_message}")

    except requests.exceptions.ConnectionError as connection_error_message:
        print(f"Connection Error: {connection_error_message}")

def get_all_teams_matches(fixtures):
    all_matches = []
    for fixture in fixtures:
        params = {
            "id": fixture
        }
        matches = get_matches(params)

        if matches:
            all_matches.extend(matches['response'])

    time.sleep(0.2)

    return all_matches

def process_fixtures(data):
    matches = []
    seen_fixture_ids = set()

    for response in data:
        fixture = response['fixture']
        fixture_id = fixture['id']

        if fixture_id in seen_fixture_ids:
            continue

        seen_fixture_ids.add(fixture_id)

        referee = fixture['referee']
        date = fixture['date']
        league = response['league']
        league_id = league['id']
        league_name = league['name']
        league_country = league['country']
        season = league['season']
        fixture_round = league['round']
        home_team = response['teams']['home']
        home_team_id = home_team['id']
        home_team_name = home_team['name']
        away_team = response['teams']['away']
        away_team_id = away_team['id']
        away_team_name = away_team['name']
        goals = response['goals']
        home_goals = goals['home']
        away_goals = goals['away']
        halftime = response['score']['halftime']
        home_goals_half = halftime['home']
        away_goals_half = halftime['away']

        matches.append({
            'fixture_id': fixture_id,
            'referee': referee,
            'date': date,
            'league_id': str(league_id),
            'league_name': league_name,
            'league_country': league_country,
            'season': season,
            'fixture_round': fixture_round,
            'home_team_id': home_team_id,
            'home_team_name': home_team_name,
            'away_team_id': away_team_id,
            'away_team_name': away_team_name,
            'home_goals': home_goals,
            'away_goals': away_goals,
            'home_goals_half': home_goals_half,
            'away_goals_half': away_goals_half
            })

    return matches

def get_statistics_fixtues(matches):
    all_statistics = []
    url_stats = "https://api-football-v1.p.rapidapi.com/v3/fixtures/statistics"

    headers = {
        'X-RapidAPI-key': RAPIDAPI_KEY,
        'X-RapidAPI-host': 'api-football-v1.p.rapidapi.com'}

    rep = {' ': '_', '%':'pct'}
    types_pct = ['ball_possession', 'passes_pct']

    for fixture in matches:
        params = {
            "fixture":fixture['fixture_id']
            }
        statistics = get_matches(url_stats, headers, params)

        if statistics:
            for response in statistics['response']:
                fixture_id = fixture['fixture_id']

                home_team_data = statistics['response'][0]
                away_team_data = statistics['response'][1]

                home_team_statistics = home_team_data['statistics']
                away_team_statistics = away_team_data['statistics']

                home_stats = {}
                away_stats = {}

                for stat in home_team_statistics:

                    if stat['value'] is not None:
                        stats_type_formatted = replace_all(stat['type'].lower(), rep)

                        if stats_type_formatted in types_pct:
                            home_stats[f"home_{stats_type_formatted}"] = int(stat['value'].replace('%','')) / 100

                        elif stats_type_formatted == 'expected_goals':
                            home_stats[f"home_{stats_type_formatted}"] = float(stat['value'])

                        else:
                            home_stats[f"home_{stats_type_formatted}"] = stat['value']

                    else:
                       stats_type_formatted = replace_all(stat['type'].lower(), rep)
                       home_stats[f"home_{stats_type_formatted}"] = None

                for stat in away_team_statistics:

                    if stat['value'] is not None:
                        stats_type_formatted = replace_all(stat['type'].lower(), rep)

                        if stats_type_formatted in types_pct:
                            away_stats[f"away_{stats_type_formatted}"] = int(stat['value'].replace('%','')) / 100

                        elif stats_type_formatted == 'expected_goals':
                            away_stats[f"away_{stats_type_formatted}"] = float(stat['value'])

                        else:
                            away_stats[f"away_{stats_type_formatted}"] = stat['value']
                    else:
                       stats_type_formatted = replace_all(stat['type'].lower(), rep)
                       away_stats[f"away_{stats_type_formatted}"] = None

                combined_stats = {
                    'fixture_id': fixture_id,
                    **home_stats,
                    **away_stats
                    }

                all_statistics.append(combined_stats)

    time.sleep(0.2)

    all_statistics = list(unique_everseen(all_statistics))
    return all_statistics

def matches_dataframe(matches, statistics):

    df_matches = pd.DataFrame(matches)
    df_statistics = pd.DataFrame(statistics)

    df = df_matches.merge(df_statistics, left_on='fixture_id', right_on='fixture_id')

    #expected goals not available for earlier years
    if 'home_expected_goals' not in df.columns:
        df['home_expected_goals'] = np.nan
    if 'away_expected_goals' not in df.columns:
        df['away_expected_goals'] = np.nan

    df['date'] = pd.to_datetime(df['date']).dt.date
    df.sort_values(by=['date'], inplace=True)
    df = df[df['league_id'].isin(['39', '40', '41', '42', '61', '62', '78', '79', '140', '141', '135', '136', '94', '95', '88', '89', '144', '145'])]
    df = df.applymap(lambda x: 0 if pd.isna(x) else x)

    if 'home_goals_prevented' in df.columns:
        del df['home_goals_prevented']
    if 'away_goals_prevented' in df.columns:
        del df['away_goals_prevented']

    return df

def create_db_connection(HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT):

    db_connection = None
    try:
        db_connection = mysql.connector.connect(
            host = HOST,
            user = MYSQL_USERNAME,
            password = MYSQL_PASSWORD,
            database = MYSQL_DATABASE,
            port = MYSQL_PORT
        )
        print("MySQL Database Connection Successful")

    except Error as e:
        print(f"Database Connection Error: '{e}'")

    return db_connection

def insert_into_table(db_connection, df):

    cursor = db_connection.cursor()

    INSERT_DATA_SQL_QUERY = """
    INSERT INTO matches (`fixture_id`, `referee`, `date`, `league_id`, `league_name`, `league_country`, `season`, `fixture_round`, `home_team_id`, `home_team_name`, `away_team_id`,
                         `away_team_name`, `home_goals`, `away_goals`, `home_goals_half`, `away_goals_half`, `home_shots_on_goal`, `home_shots_off_goal`, `home_total_shots`, `home_blocked_shots`, `home_shots_insidebox`, `home_shots_outsidebox`,
                         `home_fouls`, `home_corner_kicks`, `home_offsides`, `home_ball_possession`, `home_yellow_cards`, `home_red_cards`, `home_goalkeeper_saves`, `home_total_passes`, `home_passes_accurate`,
                         `home_passes_pct`, `home_expected_goals`, `away_shots_on_goal`, `away_shots_off_goal`, `away_total_shots`, `away_blocked_shots`, `away_shots_insidebox`, `away_shots_outsidebox`,
                         `away_fouls`, `away_corner_kicks`, `away_offsides`, `away_ball_possession`, `away_yellow_cards`, `away_red_cards`, `away_goalkeeper_saves`, `away_total_passes`, `away_passes_accurate`,
                         `away_passes_pct`, `away_expected_goals`)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        `fixture_id` = `fixture_id`
    """

    df = df.where(pd.notnull(df), None)

    print(f"Number of columns in DataFrame: {df.shape[1]}")
    print(f"Number of placeholders in SQL query: {INSERT_DATA_SQL_QUERY.count('%s')}")
    print("DataFrame columns:", df.columns.tolist())

    data_values_as_tuples = [tuple(x) for x in df.to_numpy()]

    try:
        cursor.executemany(INSERT_DATA_SQL_QUERY, data_values_as_tuples)
        db_connection.commit()
        print("Data inserted correctly")
    except Exception as e:
        print("Error while inserting data:", e)
        db_connection.rollback()

def run_data_pipeline():

    db_connection = create_db_connection(HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT)
    data = get_all_teams_matches(fixtures)

    if data:
        matches = process_fixtures(data)
        statistics = get_statistics_fixtues(matches)
        df = matches_dataframe(matches, statistics)

    else:
        print("No data available or error occured")
        return


    if db_connection is not None:
        df = matches_dataframe(matches, statistics)
        insert_into_table(db_connection, df)
        db_connection.close()

        return df

if __name__ == "__main__":
    run_data_pipeline()