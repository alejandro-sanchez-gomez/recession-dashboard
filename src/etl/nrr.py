
import os, io
import pandas
from azure.storage.blob import BlobServiceClient
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix

class nrr_value:

    def __init__(self, list_kpi):
        self.set_kpis(list_kpi) 

    # SETTERS/GETTERS
    def set_kpis(self, list):
        self.kpis = list
   
    def get_kpis(self):
        return self.kpis
   
    def get_nrr_table(self):
        return self.nrr_table
    
    # OPERATIONS
    def list_normalize(self):
        
        list_df = self.get_kpis()

        n = 0
        for elem in list_df:
            elem_copy = elem
            elem_date = elem_copy['date']
            elem_value = elem_copy.drop(columns=['date'])
            elem_value = (elem_value - elem_value.min())/(elem_value.max()-elem_value.min())
            elem = pandas.concat([elem_date, elem_value], axis=1)
            list_df[n] = elem
            n = n+1
        
        self.set_kpis(list_df)

    def list_unify(self):

        list_df = self.get_kpis()

        n = 0
        for elem in list_df:
            if(n==0):
                table = elem
            else:
                table = pandas.merge(prev, elem, on="date", how="left")
            prev = table
            n = n + 1
        
        # remove rows that don't match
        table = table.iloc[852:]

        return table

    def calculate_nrr(self):
        
        self.list_normalize()
        table = self.list_unify()

        table_no_date = table.drop(columns=['date'])
        table_date = table['date']
        table_x = table_no_date.drop(columns=['USREC'])
        table_y = table_no_date['USREC']

        table_x.interpolate(method ='linear', 
                            limit_direction ='backward', inplace=True)
        table_x.interpolate(method ='linear', 
                            limit_direction ='forward', inplace=True)

        x = table_x
        y = table_y

        z = 300
        x = table_x.iloc[:z]
        y = table_y.iloc[:z]
        x_test = table_x.iloc[z:]
        y_test = table_y.iloc[z:]

        model = LogisticRegression(solver='liblinear', C=1.0, random_state=0)
        model.fit(x, y)

        y_pred = model.predict(x_test)
        x_pred = model.predict_proba(x_test)

        list_nrr = []
        list_dates = []
        for elem in x_pred:
            value = 1
            if elem[1] < 0.075: 
                value = 5
            elif elem[1] < 0.15: 
                value = 4
            elif elem[1] < 0.225: 
                value = 3
            elif elem[1] < 0.3: 
                value = 2

            list_nrr.append(value)
            list_dates.append(table_date.iloc[z])
            z = z+1

        self.nrr_table = pandas.DataFrame(list(zip(list_dates, list_nrr)),
              columns=['date','nrr'])

    def upload_nrr_azure(self):

        #authentificate
        connect_str = os.getenv('KEY_NRR_DATA')
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        #client
        blob_client = blob_service_client.get_blob_client(container = "container-nrr", blob = "nrr_table")

        #data to csv
        writer = io.BytesIO()
        self.get_nrr_table().to_csv(writer)
        
        #upload
        blob_client.upload_blob(writer.getvalue(), overwrite = True)    
