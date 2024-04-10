
class NRR:

    def __init__(self, PCE, CP, GDP, INDPRO, USREC, SLRTTO01USQ661S, UNRATE, VIXCLS, daily_treasury_yield_curve):
        self.set_PCE(PCE) 
        self.set_CP(CP)
        self.set_GDP(GDP)
        self.set_INDPRO(INDPRO)
        self.set_USREC(USREC)
        self.set_RETAIL(SLRTTO01USQ661S)
        self.set_UNRATE(UNRATE)
        self.set_VIXCLS(VIXCLS)
        self.set_YIELD(daily_treasury_yield_curve)

    def set_PCE(self, PCE):
        self.PCE = PCE
    def set_CP(self, CP):
        self.CP = CP
    def set_GDP(self, GDP):
        self.GDP = GDP
    def set_INDPRO(self, INDPRO):
        self.INDPRO = INDPRO
    def set_USREC(self, USREC):
        self.USREC = USREC
    def set_RETAIL(self, SLRTTO01USQ661S):
        self.RETAIL = SLRTTO01USQ661S
    def set_UNRATE(self, UNRATE):
        self.UNRATE = UNRATE
    def set_VIXCLS(self, VIXCLS):
        self.VIXCLS = VIXCLS
    def set_YIELD(self, daily_treasury_yield_curve):
        self.YIELD = daily_treasury_yield_curve

    def get_PCE(self):
        return self.PCE
    def get_CP(self):
        return self.CP 
    def get_GDP(self):
        return self.GDP 
    def get_INDPRO(self):
        return self.INDPRO
    def get_USREC(self):
        return self.USREC 
    def get_RETAIL(self):
        return self.RETAIL
    def get_UNRATE(self):
        return self.UNRATE
    def get_VIXCLS(self):
        return self.VIXCLS
    def get_YIELD(self):
        return self.YIELD

    def calculate_nrr(self):
        nrr = 5
        self.nrr = nrr
    
    def get_nrr(self):
        return self.nrr
    
