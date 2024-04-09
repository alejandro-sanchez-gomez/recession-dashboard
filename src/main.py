
#!/usr/bin/env python3

from etl import kpi

def main():
        
        kpi_pce = kpi.KPI("Price Consumer Expenditure")
        kpi_pce.set_data(0, "PCE")
        kpi_pce.upload_data_azure()

        kpi_pce = kpi.KPI("Corporate Profits After Tax")
        kpi_pce.set_data(0, "CP")
        kpi_pce.upload_data_azure()

        kpi_pce = kpi.KPI("Gross Domestic Product")
        kpi_pce.set_data(0, "GDP")
        kpi_pce.upload_data_azure()

        kpi_pce = kpi.KPI("Industrial Production")
        kpi_pce.set_data(0, "INDPRO")
        kpi_pce.upload_data_azure()

        kpi_pce = kpi.KPI("NBER Recession Index")
        kpi_pce.set_data(0, "USREC")
        kpi_pce.upload_data_azure()

        kpi_pce = kpi.KPI("Total Retail Trade")
        kpi_pce.set_data(0, "SLRTTO01USQ661S")
        kpi_pce.upload_data_azure()

        kpi_pce = kpi.KPI("Unemployment Rate")
        kpi_pce.set_data(0, "UNRATE")
        kpi_pce.upload_data_azure()
        
        kpi_pce = kpi.KPI("Volatily Index")
        kpi_pce.set_data(0, "VIXCLS")
        kpi_pce.upload_data_azure()

        kpi_pce = kpi.KPI("Yield Curve")
        kpi_pce.set_data(1, "daily_treasury_yield_curve")
        kpi_pce.upload_data_azure()

main()