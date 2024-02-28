
import sys
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
sys.path.append(os.environ.get("src_path"))
import unittest
import api

class TestApi(unittest.TestCase):

    def test_get_data_stlouis(self):

        self.assertNotEqual(api.STLOUIS.get_data("PCE"), 0)
        self.assertNotEqual(api.STLOUIS.get_data("GDP"), 0)
        self.assertNotEqual(api.STLOUIS.get_data("VIXCLS"), 0)
        self.assertNotEqual(api.STLOUIS.get_data("SLRTTO01USQ661S"), 0)
        self.assertNotEqual(api.STLOUIS.get_data("UNRATE"), 0)
        self.assertNotEqual(api.STLOUIS.get_data("INDPRO"), 0)
        self.assertNotEqual(api.STLOUIS.get_data("USREC"), 0)
        self.assertNotEqual(api.STLOUIS.get_data("CP"), 0)
    
    def test_get_data_ustreasury(self):
        
        self.assertNotEqual(api.USTREASURY.get_data("daily_treasury_yield_curve"), 0)

if __name__ == '__main__':
    unittest.main()
