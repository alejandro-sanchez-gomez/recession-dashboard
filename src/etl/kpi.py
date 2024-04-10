

import os, io
from azure.storage.blob import BlobServiceClient
from etl import api

class KPI:
    def __init__(self, kpi_name):
        self.set_name(kpi_name)

    def set_name(self, kpi_name):
        self.name = kpi_name
    
    def get_name(self):
        return self.name
        
    def set_data(self, api_class, kpi_id):
        self.data = api.API_DATA_DISPATCHER.get_data(api_class, kpi_id)

    def get_data(self):
        return self.data
    
    def upload_data_azure(self):

        #authentificate
        connect_str = os.getenv('KEY_NRR_DATA')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        #client
        blob_client = blob_service_client.get_blob_client(container = "container-kpi", blob = self.get_name())

        #data to csv
        writer = io.BytesIO()
        self.data.to_csv(writer)
        
        #upload
        blob_client.upload_blob(writer.getvalue(), overwrite = True)
