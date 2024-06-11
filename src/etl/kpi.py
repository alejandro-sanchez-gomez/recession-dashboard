#!/usr/bin/env python3

"""Provides the necessary methods need to store data regarding the KPIs used to calculate the risk of recession.
- KPI: Class used to store all methods need to store the KPIs data. 
"""
import os # access Window's system environmental variables in order to authentificate to Azure Blob Storage.
import io # transforms data into a stream of in-memory bytes.
from azure.storage.blob import BlobServiceClient # uploads data in Azure Blob Storage.
from etl import api # imports the API module in order to extract and transform the data we need.

__author__ = "Alejandro Sánchez Gómez"
__copyright__ = "Copyright 2024, Recession Dashboard"
__license__ = "MIT license"
__version__ = "1.0.0"
__maintainer__ = "Alejandro Sánchez Gómez"
__email__ = "alejandro@recession-dashboard.com"
__status__ = "Production"

class KPI:
    """Class used to store all methods need to store the KPIs data."""    
    
    def __init__(self, kpi_name):
        """Initializes the KPI and sets its name.
        kpi_name: Name of the KPI.
        """
        self.set_name(kpi_name)

    def set_name(self, kpi_name):
        """Sets the name of the KPI."""
        self.name = kpi_name
    
    def get_name(self):
        """Returns the name of the KPI."""
        return self.name
        
    def set_data(self, api_class, kpi_id):
        """Sets the KPI dataframe by using the API module."""
        self.data = api.API_DATA_DISPATCHER.get_data(api_class, kpi_id)

    def get_data(self):
        """Returns the dataframe from the KPI."""
        return self.data
    
    def upload_data_azure(self):
        """Loads the KPIs dataframes into our Data Warehouse located at Azure Blob Storage."""

        connect_str = os.getenv('KEY_NRR_DATA') # gets the key needed to authentificate into our Data Warehouse.
        blob_service_client = BlobServiceClient.from_connection_string(connect_str) # authentificates into the Data Warehouse hosted at Azure Blob Storage.
        blob_client = blob_service_client.get_blob_client(
            container = "container-kpi", blob = self.get_name()) # create a blob in the "container-kpi" container which will store the KPI dataframe.

        writer = io.BytesIO() # prepares the bytes streamer.
        self.data.to_csv(writer) # stores the RRL timeserie as CSV through the conversion from pandas dataframe to in-memory bytes.
        
        blob_client.upload_blob(writer.getvalue(), overwrite = True) # uploads it into the Data Warehouse.
