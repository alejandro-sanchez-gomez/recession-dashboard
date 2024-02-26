
import sys
import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
sys.path.append(os.environ.get("src_path"))
import unittest
import api

class TestCal(unittest.TestCase):

    def test_get_data_stlouis(self):
        self.assertNotEqual(api.get_data_stlouis("PCE"), 0)
        self.assertNotEqual(api.get_data_stlouis("GDP"), 0)
        self.assertNotEqual(api.get_data_stlouis("VIXCLS"), 0)
        self.assertNotEqual(api.get_data_stlouis("SLRTTO01USQ661S"), 0)
        self.assertNotEqual(api.get_data_stlouis("UNRATE"), 0)
        self.assertNotEqual(api.get_data_stlouis("INDPRO"), 0)
        self.assertNotEqual(api.get_data_stlouis("USREC"), 0)
        self.assertNotEqual(api.get_data_stlouis("CP"), 0)
    
    def test_get_data_ustreasury(self):
        
        self.assertNotEqual(api.get_data_ustreasury("daily_treasury_yield_curve", 2024), 0)

if __name__ == '__main__':
    unittest.main()
