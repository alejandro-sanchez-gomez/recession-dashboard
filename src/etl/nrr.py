
import os, io
import datetime
from azure.storage.blob import BlobServiceClient, BlobClient

class nrr_value:

    def __init__(self, PCE, CP, GDP, INDPRO, USREC, SLRTTO01USQ661S, UNRATE, VIXCLS, YIELD):
        self.set_PCE(PCE) 
        self.set_CP(CP)
        self.set_GDP(GDP)
        self.set_INDPRO(INDPRO)
        self.set_USREC(USREC)
        self.set_RETAIL(SLRTTO01USQ661S)
        self.set_UNRATE(UNRATE)
        self.set_VIXCLS(VIXCLS)
        self.set_YIELD(YIELD)

    def set_PCE(self, PCE):
        self.PCE = PCE
    def set_CP(self, CP):
        self.CP = CP
    def set_GDP(self, GDP):
        self.GDP = GDP
    def set_INDPRO(self, INDPRO):
        self.INDPRO = INDPRO
    def set_USREC(self, USREC):
        self.USREC = USREC
    def set_RETAIL(self, SLRTTO01USQ661S):
        self.RETAIL = SLRTTO01USQ661S
    def set_UNRATE(self, UNRATE):
        self.UNRATE = UNRATE
    def set_VIXCLS(self, VIXCLS):
        self.VIXCLS = VIXCLS
    def set_YIELD(self, daily_treasury_yield_curve):
        self.YIELD = daily_treasury_yield_curve

    def get_PCE(self):
        return self.PCE
    def get_CP(self):
        return self.CP 
    def get_GDP(self):
        return self.GDP 
    def get_INDPRO(self):
        return self.INDPRO
    def get_USREC(self):
        return self.USREC 
    def get_RETAIL(self):
        return self.RETAIL
    def get_UNRATE(self):
        return self.UNRATE
    def get_VIXCLS(self):
        return self.VIXCLS
    def get_YIELD(self):
        return self.YIELD
    def get_nrr(self):
        return self.nrr
    
    def calculate_nrr(self):
        nrr_value = 5
        self.nrr_value = nrr_value
    
class nrr_table:
    def __init__(self):
        print()

    def download_nrr_azure(self):

        #authentificate
        connect_str = os.getenv('KEY_NRR_DATA')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str) 

        #client
        blob = BlobClient(account_url="https://<account_name>.blob.core.windows.net"
                  container_name="<container_name>",
                  blob_name="<blob_name>",
                  credential="<account_key>")

        #data
        with open("example.csv", "wb") as f:
            data = blob.download_blob()
            data.readinto(f)
        
        #csv to dataframe
        self.nrr_table = data

    def update_nrr_table(self):
        #get month
        current_month = datetime.now().month
        current_month = str(current_month)

    def upload_nrr_azure(self):

        #authentificate
        connect_str = os.getenv('KEY_NRR_DATA')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        #client
        blob_client = blob_service_client.get_blob_client(container = "container-nrr", blob = "nrr_table")

        #data to csv
        writer = io.BytesIO()
        self.nrr_table.to_csv(writer)
        
        #upload
        blob_client.upload_blob(writer.getvalue(), overwrite = True)    
