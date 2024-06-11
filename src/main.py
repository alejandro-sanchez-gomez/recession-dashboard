#!/usr/bin/env python3

"""Provides coordination between modules and executes them in order to run the system.
"""
from etl import kpi, nrr # modules used by the program

__author__ = "Alejandro Sánchez Gómez"
__copyright__ = "Copyright 2024, Recession Dashboard"
__license__ = "MIT license"
__version__ = "1.0.0"
__maintainer__ = "Alejandro Sánchez Gómez"
__email__ = "alejandro@recession-dashboard.com"
__status__ = "Production"

def set_kpi_data(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield):
        """Defines data for all KPIs that will be used to calculate the NRR."""
        kpi_pce.set_data(0, "PCE")
        kpi_cp.set_data(0, "CP")
        kpi_gdp.set_data(0, "GDP")
        kpi_indpro.set_data(0, "INDPRO")
        kpi_usrec.set_data(0, "USREC")
        kpi_retail.set_data(0, "MRTSSM44000USS")
        kpi_unrate.set_data(0, "UNRATE")
        kpi_vixcls.set_data(0, "VIXCLS")
        kpi_yield.set_data(1, "daily_treasury_yield_curve")     


def make_list_kpi(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield):
        """Defines the list of KPIs which will be used to calculate the NRR. 
        Note: The first item of the list must be kpi_usrec and all parameters must be used.
        """
        list_kpi = [] # empty list
        list_kpi.append(kpi_usrec.get_data()) # append kpi_usrec first
        list_kpi.append(kpi_pce.get_data())
        list_kpi.append(kpi_cp.get_data())
        list_kpi.append(kpi_gdp.get_data())
        list_kpi.append(kpi_indpro.get_data())
        list_kpi.append(kpi_retail.get_data())
        list_kpi.append(kpi_unrate.get_data())
        list_kpi.append(kpi_vixcls.get_data())
        list_kpi.append(kpi_yield.get_data())

        return list_kpi

def upload_azure(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield):
        """Uploads all KPIs data into our Data Warehouse at Azure Blob Storage."""
        kpi_pce.upload_data_azure()
        kpi_cp.upload_data_azure()
        kpi_gdp.upload_data_azure()
        kpi_indpro.upload_data_azure()
        kpi_usrec.upload_data_azure()
        kpi_retail.upload_data_azure()
        kpi_unrate.upload_data_azure()
        kpi_vixcls.upload_data_azure()
        kpi_yield.upload_data_azure()    

def run():
        """Runs the code in order to make the system work."""
        # sets data for all KPIs
        kpi_pce = kpi.KPI("pce")
        kpi_cp = kpi.KPI("cp")
        kpi_gdp = kpi.KPI("gdp")
        kpi_indpro = kpi.KPI("indpro")
        kpi_usrec = kpi.KPI("usrec")
        kpi_retail = kpi.KPI("retail")
        kpi_unrate = kpi.KPI("unrate")
        kpi_vixcls = kpi.KPI("volatility")
        kpi_yield = kpi.KPI("yield")

        set_kpi_data(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield) # sets data into the kpi
        list_kpi = make_list_kpi(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield) # joins all KPIs into one list
        nrr_value = nrr.NRR_VALUE(list_kpi) # calculates the NRR

        upload_azure(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield) # uploads the KPIs into Azure Blob Storage
        nrr_value.upload_nrr_azure() # uploads the NRR into Azure Blob Storage

run() # runs the system