import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
import pandas as pd 
import re


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
    def request_data(id): 
        pass
    
    @abstractmethod
    def transform_data(data):
        pass

    @abstractmethod
    def get_data(data):
        pass

class STLOUIS(API):

    def __init__(self, name = "STLOUIS"):
        self.name = name

    def request_data(id):
        api_key = os.environ.get("api_key")
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + id + "&api_key=" + api_key

        try:
            response = requests.get(url)
            response.raise_for_status() 
            data = BeautifulSoup(response.content, "lxml-xml")
        
        except requests.exceptions.HTTPError as errh:
            print("ERROR") 
            print(errh.args[0]) 
        except requests.exceptions.ConnectionError as conerr: 
            print("Connection error") 
        except requests.exceptions.RequestException as errex:
            print('ERRROR: GET request failed with an status code of ' + str(response.status_code))

        return data
    
    def transform_data(data):

        cols = ["date", "value"] 
        rows = [] 

        for elem in data.find_all("observation"):
            
            rows.append({
                "date": elem.get("date"),
                "value": elem.get("value")
            }) 
  
        df = pd.DataFrame(rows, columns=cols) 

        return df
    
    def get_data(id):

        data = STLOUIS.request_data(id)
        df = STLOUIS.transform_data(data)

        return df
    
class USTREASURY(API):

    def __init__(self, name = "USTREASURY"):
        self.name = name

    def request_data(id): 

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
    
    def transform_data(data):

        cols = ["date", "bc_1month", "bc_3month", "bc_6month", "bc_1year", "bc_3year", "bc_7year"]  
        rows = [] 

        for elem in data.find_all("content"):
            rows.append({
                "date": elem.find("d:NEW_DATE"),
                "bc_1month": elem.find("d:BC_1MONTH"),
                "bc_3month": elem.find("d:BC_3MONTH"),
                "bc_6month": elem.find("d:BC_6MONTH"),
                "bc_1year": elem.find("d:BC_1YEAR"),
                "bc_3year": elem.find("d:BC_3YEAR"),
                "bc_7year": elem.find("d:BC_7YEAR")
            }) 
        
        df = pd.DataFrame(rows, columns=cols) 
        df = df.astype(str)

        for n in df.columns:
            df[n] = df[n].str.replace('<.*?>', '', regex=True)
        
        df['date'] = df['date'].str.replace('T00:00:00', '')

        return df
    
    def get_data(id):

        data = USTREASURY.request_data(id)
        df = USTREASURY.transform_data(data)

        return df
