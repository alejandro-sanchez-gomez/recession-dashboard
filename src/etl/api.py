#!/usr/bin/env python3

"""Provides the necessary classes and methods need to extract the data for our system.
- API_CLASSES: Enum class used as a catalogue of the coded APIs. 
- API_DATA_DISPATCHER: Interface that provides methods to get data from the coded APIs through its catalogue.
- API: Abstract class used to implement APIs.
- STLOUIS: Implementation of the API class that extracts data from the FRED repository.
- USTREASURY: Implementation of the API class that extracts data from the U.S. Treasury repository.
"""
import os # access Window's system environmental variables in order to authentificate to Azure Blob Storage.
import requests # makes HTTP requests to extract data from web repositories.
import pandas as pd # transforms extracted data into workable formats.
from bs4 import BeautifulSoup # parses XML data from HTTP's requests.
from concurrent.futures import ThreadPoolExecutor, as_completed # provides modules for making parallel HTTP requests.
from abc import ABC, abstractmethod # enables the use Oriented Object Programming to Python by providing tools to create interfaces and abstract classes.
from enum import Enum # enables the use Oriented Object Programming to Python by providing tools to create enum classes.
from datetime import datetime # extracts current datetime.

__author__ = "Alejandro Sánchez Gómez"
__copyright__ = "Copyright 2024, Recession Dashboard"
__license__ = "MIT license"
__version__ = "1.0.0"
__maintainer__ = "Alejandro Sánchez Gómez"
__email__ = "alejandro@recession-dashboard.com"
__status__ = "Production"

class API_CLASSES(Enum):
    """Enum class used as a catalogue of the coded APIs."""
    STLOUIS = 0
    USTREASURY = 1

class API_DATA_DISPATCHER(ABC):
    """Interface that provides methods to get data from the coded APIs through its catalogue."""
    def get_data(api_class, id):
        """Extracts and transforms the data from the web in a useful format."""
        data = 0
        match api_class:
            case 0:
                data = STLOUIS.get_data(id)
            case 1:
                data = USTREASURY.get_data(id)
        return data

class API(ABC):
    """Abstract class used to implement APIs."""
    @abstractmethod
    def request_data(id): 
        """Extracts and parses the data from the web.
        id: Name used by the APIs to select the desired timeseries data.
        """
        pass # requires implementation.
    
    @abstractmethod
    def transform_data(id, data):
        """Transforms the extracted data into a useful format.
        id: Name of the timeseries data that we have just extracted.
        data: Data we have just extracted.
        """
        pass # requires implementation.

    @abstractmethod
    def get_data(id):
        """Executes request_data and transform_data methods into a single method.
        id: Name used by the APIs to select the desired timeseries data.
        """
        pass # requires implementation.

class STLOUIS(API):
    """Implementation of the API class that extracts data from the FRED repository."""
    def request_data(id):
        """Implementation of request_data for the FRED repository. 
        Returns XML data tree.
        """
        api_key = os.environ.get("FRED_API_KEY") # key needed in order to use the FRED API.
        url = "https://api.stlouisfed.org/fred/series/observations?series_id=" + id + "&api_key=" + api_key # URL to get access to the FRED API.
        try: # requests data from the FRED API. If it's not able to do it, raises a RequestException.
            response = requests.get(url) # requests data from the FRED API.
            response.raise_for_status() # raises the status of the HTTP request.
            data = BeautifulSoup(response.content, "lxml-xml") # parses the HTTP request data into XML.
            print(f"Database {id} successfully fetched!")
            return data
        except requests.exceptions.RequestException as e:
            print(f"ERROR fetching {id}: {e}")
            return None
        
    def transform_data(id, data):
        """Implementation of transform_data for the FRED repository. 
        Returns a pandas dataframe.
        """
        cols = ["date", id] # future columns of the pandas dataframe that has to be returned.
        rows = [] # future rows of the pandas dataframe that has to be returned.

        for elem in data.find_all("observation"): # iterates through the XML tree and extracts the data under the "observation" label.
            rows.append({  # appends the extracted data to rows of the future pandas dataframe.
                "date": elem.get("date"),
                id: elem.get("value")
            }) 
            
        df = pd.DataFrame(rows, columns=cols) # creates the pandas dataframe from the extracted data.
        df["date"] = pd.to_datetime(df["date"]) # converts the datatype of the "date" column to datetime.
        df[id] = pd.to_numeric(df[id], errors = 'coerce') # converts the column "id" to numeric.
        if(id == "USREC"): df = df.iloc[769:] # removes unnecessary rows from USREC dataframe.

        return df
    
    def get_data(id):
        """Implementation of get_data for the FRED repository. 
        Returns a pandas dataframe.
        """
        data = STLOUIS.request_data(id) 
        df = STLOUIS.transform_data(id, data)

        return df
  
def fetch_ustreasury(url, year):
    """External method created to assist the method transform_data from the USTREASURY class. 
    url: URL to get access to the U.S.Treasury API.
    year: Year of the Trasury Yield Rate data that we want to extract. 
    Returns XML data tree. 
    """
    try: # requests data from the U.S.Treasury API. If it's not able to do it, raises a RequestException.
        response = requests.get(url) # requests data from the U.S.Treasury API.
        response.raise_for_status() # raises the status of the HTTP request.
        return year, response.content
    except requests.exceptions.RequestException as e:
        print(f"ERROR fetching {year}: {e}")
        return year, None

class USTREASURY(API):
    """Implementation of the API class that extracts data from the U.S. Treasury repository."""
    def request_data(id):
        """Implementation of request_data for the U.S.Treasury repository. In order to maximize efficiency, the HTTP request will be performed through parallelization.
        Returns XML data tree.
        """
        last_year = datetime.now().year # gets current year.
        initial_year = 1990 # year when the older Treasury Yield Rate timeseries data is recorded.
        urls = [ # stores all HTTP requests into a single list by year.
            (f"https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml?data={id}&field_tdr_date_value={year}", year)
            for year in range(initial_year, last_year + 1)
        ]
        data = BeautifulSoup(features="lxml-xml") # initializing an empty XML data tree.
        total = len(urls) # total of requests we will need in order to calculate the fetching progress.
        completed = 0 # initializing number of requests completed in order to calculate the fetching progress.

        with ThreadPoolExecutor(max_workers=10) as executor: # start of the request parallelization process with a maximum number of parallel threads of 10.
            
            future_to_url = {executor.submit(fetch_ustreasury, url, year): (url, year) for url, year in urls} # collects all HTTP requests done through parallelization.

            for future in as_completed(future_to_url): # iterates through the completed HTTP requests in order to parse it into an XML data tree.
                completed += 1 # increases the number of completed requests int order to calculate the fetching progress.
                url, year = future_to_url[future] # extracts the year of the completed request.

                try: # check if the HTTP request is empty or not. If it's empty, raises a generic Exception.
                    year, content = future.result() # checks year and content of the HTTP request.
                    if content:
                        page = BeautifulSoup(content, features="lxml-xml") # parses the HTTP request into a XML data tree.
                        data.append(page) # appends the parsed data to previous XML data tree.
                except Exception as exc:
                    print(f"Generated an exception for {year}: {exc}")
                    return None

                print(f"Database {id} fetching progress: {completed}/{total} ({(completed/total)*100:.2f}%)")
        
        print(f"Database {id} successfully fetched!")
        return data

    
    def transform_data(id, data):
        """Implementation of transform_data for the U.S.Treasury repository. 
        Returns a pandas dataframe.
        """
        cols = ["date", "bc_3month", "bc_6month", "bc_1year", "bc_10year", "bc_30year"]  # future columns of the pandas dataframe that has to be returned.
        rows = [] # future rows of the pandas dataframe that has to be returned.

        for elem in data.find_all("content"): # iterates through the XML tree and extracts the data under the "content" label.
            rows.append({ # appends the extracted data to rows of the future pandas dataframe.
                "date": elem.find("d:NEW_DATE"),
                "bc_3month": elem.find("d:BC_3MONTH"),
                "bc_6month": elem.find("d:BC_6MONTH"),
                "bc_1year": elem.find("d:BC_1YEAR"),
                "bc_10year": elem.find("d:BC_10YEAR"),
                "bc_30year": elem.find("d:BC_30YEAR")
            }) 
        
        df = pd.DataFrame(rows, columns=cols) # creates the pandas dataframe from the extracted data.

        df = df.astype(str) # converts the entire dataframe to string type in order to transform the rows content to a workable one.
        for n in df.columns:df[n] = df[n].str.replace('<.*?>', '', regex=True) # using regular expressions, removes all unnecessary chars from the rows.
        df['date'] = df['date'].str.replace('T00:00:00', '') # removes the string "T00:00:00" from the rows.

        df["date"] = pd.to_datetime(df["date"]) # converts the datatype of the "date" column to datetime.
        df["bc_3month"] = pd.to_numeric(df["bc_3month"], errors = 'coerce') # converts the column "bc_3month" to numeric.
        df["bc_6month"] = pd.to_numeric(df["bc_6month"], errors = 'coerce')
        df["bc_1year"] = pd.to_numeric(df["bc_1year"], errors = 'coerce')
        df["bc_10year"] = pd.to_numeric(df["bc_10year"], errors = 'coerce')
        df["bc_30year"] = pd.to_numeric(df["bc_30year"], errors = 'coerce')

        return df
    
    def get_data(id):
        """Implementation of get_data for the U.S.Treasury repository. 
        Returns a pandas dataframe.
        """
        data = USTREASURY.request_data(id)
        df = USTREASURY.transform_data(id, data)

        return df
