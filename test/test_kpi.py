#!/usr/bin/env python3
import sys
import unittest

from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent)+"\src")
from etl import kpi

class TestKpiSTLOUIS(unittest.TestCase):

    def test_init_kpi(self):

        kpi_pce = kpi.KPI("Price Consumer Expenditure")
        self.assertIsNotNone(kpi_pce)

        kpi_pce.set_data(0, "PCE")
        self.assertIsNotNone(kpi_pce.get_data())
    
    def test_upload_data_azure(self):
        kpi_pce = kpi.KPI("Price Consumer Expenditure")
        kpi_pce.set_data(0, "PCE")
        kpi_pce.upload_data_azure()

class TestKpiUSTREASURY(unittest.TestCase):
    
    def test_init_kpi(self):

        kpi_yield = kpi.KPI("Yield Curve")
        self.assertIsNotNone(kpi_yield)

        kpi_yield.set_data(1, "daily_treasury_yield_curve")
        self.assertIsNotNone(kpi_yield.get_data())
    
    def test_upload_data_azure(self):
        kpi_yield = kpi.KPI("Yield Curve")
        kpi_yield.set_data(1, "daily_treasury_yield_curve")
        kpi_yield.upload_data_azure()
        
    
if __name__ == '__main__':
    unittest.main()
