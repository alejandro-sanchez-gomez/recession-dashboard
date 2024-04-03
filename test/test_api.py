
import sys, os
import unittest
sys.path.append(os.environ.get("src_path"))
import api

class TestSTLOUIS(unittest.TestCase):

    def test_request_data_stlouis(self):
        self.assertNotEqual(api.STLOUIS.request_data("PCE"), 0)
        self.assertNotEqual(api.STLOUIS.request_data("GDP"), 0)
        self.assertNotEqual(api.STLOUIS.request_data("VIXCLS"), 0)
        self.assertNotEqual(api.STLOUIS.request_data("SLRTTO01USQ661S"), 0)
        self.assertNotEqual(api.STLOUIS.request_data("UNRATE"), 0)
        self.assertNotEqual(api.STLOUIS.request_data("INDPRO"), 0)
        self.assertNotEqual(api.STLOUIS.request_data("USREC"), 0)
        self.assertNotEqual(api.STLOUIS.request_data("CP"), 0)
            
    def test_transform_data_stlouis(self):
        
        data = api.STLOUIS.request_data("PCE")
        df = api.STLOUIS.transform_data(data)
        self.assertIsNotNone(df)

    def test_get_data_stlouis(self):

        self.assertIsNotNone(api.STLOUIS.get_data("PCE"), 0)
        self.assertIsNotNone(api.STLOUIS.get_data("GDP"), 0)
        self.assertIsNotNone(api.STLOUIS.get_data("VIXCLS"), 0)
        self.assertIsNotNone(api.STLOUIS.get_data("SLRTTO01USQ661S"), 0)
        self.assertIsNotNone(api.STLOUIS.get_data("UNRATE"), 0)
        self.assertIsNotNone(api.STLOUIS.get_data("INDPRO"), 0)
        self.assertIsNotNone(api.STLOUIS.get_data("USREC"), 0)
        self.assertIsNotNone(api.STLOUIS.get_data("CP"), 0)

class TestUSTREASURY(unittest.TestCase):    

    def test_request_data_ustreasury(self):
        
        self.assertIsNotNone(api.USTREASURY.request_data("daily_treasury_yield_curve"))

    def test_transform_data_ustreasury(self):
        
        data = api.USTREASURY.request_data("daily_treasury_yield_curve")
        df = api.USTREASURY.transform_data(data)
        self.assertIsNotNone(df)

    def test_get_data_ustreasury(self):
        
        self.assertIsNotNone(api.USTREASURY.get_data("daily_treasury_yield_curve"))

if __name__ == '__main__':
    unittest.main()
