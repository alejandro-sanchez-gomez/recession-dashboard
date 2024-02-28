import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum

class API_CLASSES(Enum):
    STLOUIS = 0
    USTREASURY = 1

class API_DATA_DISPATCHER(ABC):

    def get_data(api_class, id):

        data = 0
        match api_class:
            case 0:
                data = STLOUIS.get_data(id)
            case 1:
                data = USTREASURY.get_data(id)
        return data

class API(ABC):
     
    @abstractmethod
    def __init__(self, name): 
        pass

    @abstractmethod
    def get_data(id): 
        pass

class STLOUIS(API):

    def __init__(self, name = "STLOUIS"):
        self.name = name

    def get_data(id):
        api_key = os.environ.get("api_key")
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + id + "&api_key=" + api_key

        try:
            response = requests.get(url)
            response.raise_for_status() 
            data = BeautifulSoup(response.content, 'html.parser')
        except requests.exceptions.HTTPError as errh:
            print("ERROR") 
            print(errh.args[0]) 
        except requests.exceptions.ConnectionError as conerr: 
            print("Connection error") 
        except requests.exceptions.RequestException as errex:
            print('ERRROR: GET request failed with an status code of ' + str(response.status_code))

        return data
    
class USTREASURY(API):

    def __init__(self, name = "USTREASURY"):
        self.name = name

    def get_data(id): 

        current_year = datetime.now().year
        current_year = str(current_year)
        url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=" + id + "&field_tdr_date_value=" + current_year

        try:
            response = requests.get(url)
            response.raise_for_status() 
            data = BeautifulSoup(response.content, features="lxml-xml")
        except requests.exceptions.HTTPError as errh:
            print("ERROR") 
            print(errh.args[0]) 
        except requests.exceptions.ConnectionError as conerr: 
            print("Connection error") 
        except requests.exceptions.RequestException as errex:
            print('ERRROR: GET request failed with an status code of ' + str(response.status_code))

        return data
