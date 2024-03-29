
import sys
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
sys.path.append(os.environ.get("src_path"))
import unittest
import api, kpi

class TestKpi(unittest.TestCase):

    def test_init_kpi(self):

        kpi_pce = kpi.KPI("Price Consumer Expenditure")
        self.assertIsNotNone(kpi_pce)

        kpi_pce.set_data(0, "PCE")
        self.assertIsNotNone(kpi_pce.get_data())
        
    
if __name__ == '__main__':
    unittest.main()
