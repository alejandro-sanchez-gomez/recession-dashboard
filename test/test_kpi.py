
import sys
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
sys.path.append(os.environ.get("src_path"))
import unittest
import kpi

class TestKpi(unittest.TestCase):

    def test_init_kpi(self):

        kpi_pce = kpi.KPI("Price Consumer Expenditure")
        self.assertIsNotNone(kpi_pce)

        kpi_pce.set_data(0, "PCE")
        self.assertIsNotNone(kpi_pce.get_data())
    
    def test_upload_data_azure(self):
        kpi_pce = kpi.KPI("Price Consumer Expenditure")
        kpi_pce.set_data(0, "PCE")
        kpi_pce.upload_data_azure()
        
    
if __name__ == '__main__':
    unittest.main()
