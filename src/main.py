
#!/usr/bin/env python3

from etl import kpi, nrr

def set_kpi_data(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield):
        
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
        
        list_kpi = []

        # append with usrec as the first value

        list_kpi.append(kpi_usrec.get_data())
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

        kpi_pce.upload_data_azure()
        kpi_cp.upload_data_azure()
        kpi_gdp.upload_data_azure()
        kpi_indpro.upload_data_azure()
        kpi_usrec.upload_data_azure()
        kpi_retail.upload_data_azure()
        kpi_unrate.upload_data_azure()
        kpi_vixcls.upload_data_azure()
        kpi_yield.upload_data_azure()    

def main():

        # initialize kpis
        kpi_pce = kpi.KPI("pce")
        kpi_cp = kpi.KPI("cp")
        kpi_gdp = kpi.KPI("gdp")
        kpi_indpro = kpi.KPI("indpro")
        kpi_usrec = kpi.KPI("usrec")
        kpi_retail = kpi.KPI("retail")
        kpi_unrate = kpi.KPI("unrate")
        kpi_vixcls = kpi.KPI("volatility")
        kpi_yield = kpi.KPI("yield")
        
        # set data into the kpi
        set_kpi_data(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield)

        # upload them into azure
        upload_azure(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield)

        # join all the kpis into one list
        list_kpi = make_list_kpi(kpi_pce, kpi_cp, kpi_gdp, kpi_indpro, kpi_usrec, kpi_retail, kpi_unrate, kpi_vixcls, kpi_yield)
        nrr_value = nrr.nrr_value(list_kpi)
        nrr_value.calculate_nrr()
        nrr_value.upload_nrr_azure()


main()