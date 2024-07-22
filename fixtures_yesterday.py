# -*- coding: utf-8 -*-
"""
Created on Mon Jun 10 12:08:02 2024

@author: skybl
"""

import requests
import pandas as pd
from datetime import datetime, timedelta

def get_yesterdays_date(frmt='%Y-%m-%d', string=True):
    yesterday = datetime.today() - timedelta(1)
    if string:
        return yesterday.strftime(frmt)
    return yesterday

def get_yestderdays_fixtures():
    date = get_yesterdays_date()
    
    url = "https://api-football-v1.p.rapidapi.com/v3/fixtures"
    headers = {
    	"x-rapidapi-key": "fb81f9eb15msh471dd34079cb82ep125bb7jsn7038e3e78ff4",
    	"x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }    
    params = {"date":date}
    
    try:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    
    except requests.exceptions.HTTPError as http_error_message:
        print(f"HTTP Error: {http_error_message}")
        
    except requests.exceptions.ConnectionError as connection_error_message:
        print(f"Connection Error: {connection_error_message}")
        
    except requests.exceptions.Timeout as timeout_error_message:
        print(f"Timeout Error: {timeout_error_message}")
        
    except requests.exceptions.RequestException as other_error_message:
        print(f"Unknown Error: {other_error_message}")

def get_league_ids(filepath):
    df = pd.read_csv(filepath)
    league_ids = df['league_id'].to_list()
    return league_ids

def process_fixtures(response):
    leagues = get_league_ids('league_identifiers.csv')

    fixtures = []
    for fixture_data in response['response']:
    
        fixture = fixture_data['fixture']
        fixture_id = fixture['id']
        league = fixture_data['league']
        league_id = league['id']
        
        if league_id in leagues:
            fixtures.append(str(fixture_id))
    
    if fixtures:
        return fixtures
    
    else:
        return "No fixtures available"
    
def fixtures_yesterday():
    response = get_yestderdays_fixtures()
    if response and 'response' in response and response['response']:
        return process_fixtures(response)
    
if __name__ == "__main__":
    fixtures = fixtures_yesterday()
