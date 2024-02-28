
import api

class KPI:
    def __init__(self, kpi_name):
        self.name = kpi_name
        
    def set_data(self, api_class, kpi_id):
        self.data = api.API_DATA_DISPATCHER.get_data(api_class, kpi_id)

    def get_data(self):
        print(self.data)
        return self.data 
