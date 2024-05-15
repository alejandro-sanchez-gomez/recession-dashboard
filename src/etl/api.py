
#!/usr/bin/env python3

import os
import requests
from bs4 import BeautifulSoup
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
import pandas as pd 

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
    def request_data(id): 
        pass
    
    @abstractmethod
    def transform_data(id, data):
        pass

    @abstractmethod
    def get_data(id):
        pass

class STLOUIS(API):

    def request_data(id):

        api_key = os.environ.get("FRED_API_KEY")
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
    
    def transform_data(id, data):

        cols = ["date", id] 
        rows = [] 

        for elem in data.find_all("observation"):
            
            rows.append({
                "date": elem.get("date"),
                id: elem.get("value")
            }) 
  
        df = pd.DataFrame(rows, columns=cols) 

        df["date"] = pd.to_datetime(df["date"])
        df[id] = pd.to_numeric(df[id], errors = 'coerce')

        if(id == "USREC"):
            df = df.iloc[769:]

        return df
    
    def get_data(id):

        data = STLOUIS.request_data(id)
        df = STLOUIS.transform_data(id, data)

        return df
    
class USTREASURY(API):

    def request_data(id): 

        last_year = datetime.now().year
        current_year = 1990
        data = BeautifulSoup(features="lxml-xml")
        while(current_year < last_year):
            year = str(current_year)  
            url = "https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data=" + id + "&field_tdr_date_value=" + year
            print(year)
            try:
                response = requests.get(url)
                response.raise_for_status() 
                page = BeautifulSoup(response.content, features="lxml-xml")
            except requests.exceptions.HTTPError as errh:
                print("ERROR") 
                print(errh.args[0]) 
            except requests.exceptions.ConnectionError as conerr: 
                print("Connection error") 
            except requests.exceptions.RequestException as errex:
                print('ERRROR: GET request failed with an status code of ' + str(response.status_code))
            data.append(page)
            current_year = current_year + 1
        return data
    
    def transform_data(id, data):

        cols = ["date", "bc_3month", "bc_6month", "bc_1year", "bc_10year", "bc_30year"]  
        rows = [] 

        for elem in data.find_all("content"):
            rows.append({
                "date": elem.find("d:NEW_DATE"),
                "bc_3month": elem.find("d:BC_3MONTH"),
                "bc_6month": elem.find("d:BC_6MONTH"),
                "bc_1year": elem.find("d:BC_1YEAR"),
                "bc_10year": elem.find("d:BC_10YEAR"),
                "bc_30year": elem.find("d:BC_30YEAR")
            }) 
        
        df = pd.DataFrame(rows, columns=cols) 
        df = df.astype(str)

        for n in df.columns:
            df[n] = df[n].str.replace('<.*?>', '', regex=True)
        
        df['date'] = df['date'].str.replace('T00:00:00', '')

        df["date"] = pd.to_datetime(df["date"])
        df["bc_3month"] = pd.to_numeric(df["bc_3month"], errors = 'coerce')
        df["bc_6month"] = pd.to_numeric(df["bc_6month"], errors = 'coerce')
        df["bc_1year"] = pd.to_numeric(df["bc_1year"], errors = 'coerce')
        df["bc_10year"] = pd.to_numeric(df["bc_10year"], errors = 'coerce')
        df["bc_30year"] = pd.to_numeric(df["bc_30year"], errors = 'coerce')

        return df
    
    def get_data(id):

        data = USTREASURY.request_data(id)
        df = USTREASURY.transform_data(id, data)

        return df
