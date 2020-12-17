# yfinance - if you change its name to something else other than "download.py", it doesn;t work

#generates AAPL.csv in this folder

import yfinance

df = yfinance.download('AAPL', start='2020-01-01', end='2020-10-02')
df.to_csv('AAPL.csv')

print(df)


import sys
sys.getdefaultencoding()

