import mstarpy as ms
import pandas as pd


funds = ms.Funds("LU0293294277")
print(funds.name)
print(funds.isin)

