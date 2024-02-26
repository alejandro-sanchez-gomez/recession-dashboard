import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import requests
from bs4 import BeautifulSoup

api_key = os.environ.get("api_key")

def get_data_stlouis(series_id): 

    data = 0
    url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + series_id + "&api_key=" + api_key

    try:
        get_request = requests.get(url)
        get_request.raise_for_status() 
        
        data = BeautifulSoup(get_request.content, 'html.parser')

    except requests.exceptions.HTTPError as errh:
        print("ERROR") 
        print(errh.args[0]) 
    except requests.exceptions.ConnectionError as conerr: 
        print("Connection error") 
    except requests.exceptions.RequestException as errex:
        print('ERRROR: GET request failed with an status code of ' + str(get_request.status_code))

    return data

def get_data_ustreasury(key, year): 

    data = 0
    url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=" + key + "&field_tdr_date_value=" + str(year)

    try:
        get_request = requests.get(url)
        get_request.raise_for_status() 

        data = BeautifulSoup(get_request.content, features="lxml-xml")

    except requests.exceptions.HTTPError as errh:
        print("ERROR") 
        print(errh.args[0]) 
    except requests.exceptions.ConnectionError as conerr: 
        print("Connection error") 
    except requests.exceptions.RequestException as errex:
        print('ERRROR: GET request failed with an status code of ' + str(get_request.status_code))

    return data
