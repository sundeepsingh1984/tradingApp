
# https://towardsdatascience.com/how-to-download-all-historic-intraday-ohcl-data-from-iex-with-python-asynchronously-via-api-b5b04a31b187
# https://github.com/timkpaine/pyEX/blob/main/examples/all.ipynb


import config
import pyEX as p

#Use 'sandbox' when don't want to waste iex limit otherwise use 'stable'
c = p.Client(api_token=config.API_KEY_IEX, version='stable')

sym='AAPL'
timeframe='5d'
# df = c.chartDF(symbol=sym, timeframe=timeframe)
# close_prices = df[['close', 'volume']]

# FOR current INTRA-DAY PRICing
df = c.intradayDF(symbol=sym)

print(df)
# print(df['minute'],":",df['close'])


#FOR INTRADAY HISTORICAL - FIND OUT HOW TO GIVE START & END DATES
from datetime import datetime
from iexfinance.stocks import get_historical_intraday

date = datetime(2020, 12, 8)
# hist_df = get_historical_intraday(token=config.API_KEY_IEX,symbol = "AAPL", date = date)
