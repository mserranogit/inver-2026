import mstarpy as ms
import pandas as pd


funds = ms.Funds("LU0293294277")
print(funds.name)
print(funds.isin)


#print(funds.esgData())

#print(funds.creditQuality())
#print(funds.holdings())

#print(funds.maturitySchedule())
#print(funds.maxDrawDown())
#print(funds.performanceTable())
#print(funds.riskVolatility())
#funds.riskReturnScatterplot()
#funds.regionalSector()

#print(funds.allocationWeighting())
print(funds.allocationMap())

print(funds.fixedIncomeStyle())