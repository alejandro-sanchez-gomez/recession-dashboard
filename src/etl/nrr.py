#!/usr/bin/env python3

"""Provides the necessary methods need to calculate the risk of recession.
- NRR_VALUE: Class used to store all methods that will be used to calculate the risk of recession. 
"""
import os # access Window's system environmental variables in order to authentificate to Azure Blob Storage.
import io # transforms data into a stream of in-memory bytes.
import pandas # performs operations to datasets.
from azure.storage.blob import BlobServiceClient # uploads data in Azure Blob Storage.
from sklearn.linear_model import LogisticRegression # allows the usage of Logistic Regression for the model.

__author__ = "Alejandro Sánchez Gómez"
__copyright__ = "Copyright 2024, Recession Dashboard"
__license__ = "MIT license"
__version__ = "1.0.0"
__maintainer__ = "Alejandro Sánchez Gómez"
__email__ = "alejandro@recession-dashboard.com"
__status__ = "Production"

class NRR_VALUE:
    """Class used to store all methods that will be used to calculate the risk of recession."""
    def __init__(self, list_kpi):
        """Initializes the class and calculates the recession risk.
        list_kpi: List of all KPIs needed to calculate the recession risk. 
        """
        self.set_kpis(list_kpi) # stores the list of KPIs needed.
        self.calculate_nrr() # calculates the recession risk level.

    def set_kpis(self, list):
        """Stores the list of KPIs needed. If the list does not have the correct format, it will throw exceptions.
        list: List of all KPIs needed to calculate the recession risk. 
        """
        expected_length = 9 # number of dataframes that must be contained in the list.
        try:
            if len(list) != expected_length: # throws ValueError if the number of dataframes is not correct.
                raise ValueError(f"The list must contain exactly {expected_length} DataFrames. Found {len(list)} instead.")
            if 'USREC' not in list[0].columns: # throws KeyError if the dataframe USREC is not the first one stored.
                raise KeyError("The first DataFrame must contain a column named 'USREC'.")
            for item in list: # throws TypeError if the list contains an element that it is not a dataframe.
                if not isinstance(item, pandas.DataFrame):
                    raise TypeError(f"All items in the list must be pandas DataFrames. Found {type(item)} instead.")
            self.kpis = list # list is stored in the class.
        except TypeError as te:
            print(f"TypeError: {te}")
        except KeyError as ke:
            print(f"KeyError: {ke}")
        except ValueError as ve:
            print(f"ValueError: {ve}")

    def get_kpis(self):
        """Returns the list of KPIs."""
        return self.kpis
    
    def set_nrr_table(self, dataframe):
        """Stores the timeseries dataframe regarding the risk of recession."""
        self.nrr_table = dataframe
    
    def get_nrr_table(self):
        """Returns the timeseries dataframe regarding the risk of recession."""
        return self.nrr_table
    
    # OPERATIONS
    def list_normalize(self):
        """Normalized the data contained inside the list of KPIs."""
        list_df = self.get_kpis()

        n = 0
        for elem in list_df: 
            elem_copy = elem
            elem_date = elem_copy['date'] # separates the dataframe from columns that are not labeled as "date".
            elem_value = elem_copy.drop(columns=['date']) # separates the dataframe from the "date" column.
            elem_value = (elem_value - elem_value.min())/(elem_value.max()-elem_value.min()) # normalized values in a range between 0 and 1 using the min max method.
            elem = pandas.concat([elem_date, elem_value], axis=1) # unites the columns into a single dataframe.
            list_df[n] = elem # updates each element of the list of KPIs.
            n = n+1
        
        self.set_kpis(list_df) # updates the list of KPIs.

    def list_unify(self):
        
        list_df = self.get_kpis()

        n = 0
        for elem in list_df: # iterates the list in order to get each dataframe and then performs left join in the previous stored dataframe of the iteration.
            if(n==0):
                table = elem
            else:
                table = pandas.merge(prev, elem, on="date", how="left")
            prev = table
            n = n + 1
        
        table = table.iloc[852:] # removes rows that contain dates before 1990.

        return table

    def calculate_nrr(self):
        """Calculates the risk of recesion for each date and stores the values into a dataframe."""
        self.list_normalize() # normalizes list of KPIs needed.
        table = self.list_unify() # unifies the list of KPIs into a single dataframe.

        table_no_date = table.drop(columns=['date']) # copies the unified KPIs table with the "date" column removed.
        table_date = table['date'] # copies the unified KPIs table with only "date" column.
        table_x = table_no_date.drop(columns=['USREC']) # copies the table without dates with the "USREC" column removed.
        table_y = table_no_date['USREC'] # copies the table without dates with only "USREC" column.

        table_x.interpolate(method ='linear', limit_direction ='backward', inplace=True) # fills empty cells with the linear interpolate method from the next filled cell backwards.
        table_x.interpolate(method ='linear', limit_direction ='forward', inplace=True) # fills empty cells with the linear interpolate method from the last filled cell forwards.

        z = 300 # creates the trainning dataset by selecting the rows that contain 3 major recessions
        x = table_x.iloc[:z] 
        y = table_y.iloc[:z]
        x_test = table_x.iloc[z:] # creates the prediction dataset by selecting the rest of rows

        try: # trains the model used to calculate the recession risk levels. If it fails, throws an Exception. 
            model = LogisticRegression(solver='liblinear', C=1.0, random_state=0) # creates the model using Logistic Regression.
            model.fit(x, y) # trains the model using Logistic Regression.
            x_pred = model.predict_proba(x_test) # calculates the risk of entering a recession using the previous trained model and the prediction dataset

            list_nrr = [] # initializing the list used to store the recession risk levels.
            list_dates = [] # initializing the list used to store the dates regarding the recession risk levels.

            for elem in x_pred: # classificates the risk of recession with the following levels: Extreme, Safe, Low, Moderate and High. 
                value = 1
                if elem[1] < 0.075: 
                    value = 5
                elif elem[1] < 0.15: 
                    value = 4
                elif elem[1] < 0.225: 
                    value = 3
                elif elem[1] < 0.3: 
                    value = 2

                list_nrr.append(value)  # stores the recession risk levels.
                list_dates.append(table_date.iloc[z]) # stores the dates of the recession risk levels.
                z = z+1

            dataframe = pandas.DataFrame(list(zip(list_dates, list_nrr)),columns=['date','nrr']) # combines both lists into a single timeserie dataframe.
            
        except Exception as e:
             print(f"An unexpected error occurred: {e}")
             print(f"The model fitting failed. Please check the input data.")
             dataframe = None
        
        self.set_nrr_table(dataframe) # stores the recession risk level timeserie dataframe.


    def upload_nrr_azure(self):
        """Loads the Recession Risk Level timeserie dataframe into our Data Warehouse located at Azure Blob Storage."""
        connect_str = os.getenv('KEY_NRR_DATA') # gets the key needed to authentificate into our Data Warehouse.
        blob_service_client = BlobServiceClient.from_connection_string(connect_str) # authentificates into the Data Warehouse hosted at Azure Blob Storage.
        blob_client = blob_service_client.get_blob_client(
            container = "container-nrr", blob = "nrr_table") # create the "nrr_table" blob in the "container-nrr" container which will store the RRL timeserie.

        writer = io.BytesIO() # prepares the bytes streamer.
        self.get_nrr_table().to_csv(writer) # stores the RRL timeserie as CSV through the conversion from pandas dataframe to in-memory bytes.

        blob_client.upload_blob(writer.getvalue(), overwrite = True) # uploads it into the Data Warehouse.     
