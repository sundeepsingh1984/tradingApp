#This script gets data for a stock 5 days in one go.
#Also not sure but all Open, low high and close prices are coming out to be the same - check if its issue with Alpaca data

import sqlite3, config, csv
import pandas as pd
import alpaca_trade_api as tradeapi
import datetime
from datetime import date, timedelta

NY = 'America/New_York'

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

symbols = []
stock_ids = {}

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

with open('qqq.csv') as f:
    reader = csv.reader(f)

    for line in reader:
        symbols.append(line[1])

cursor.execute("""
    SELECT * FROM stock
""")
stocks = cursor.fetchall()

for stock in stocks:
    symbol = stock['symbol']
    stock_ids[symbol] = stock['id']

#remove this
# symbols = ['AAPL']

i = 0
for symbol in symbols:
    #check that st_date is a Monday
    st_date = datetime.date(2020, 1, 6)
    start_date = pd.Timestamp(st_date, tz=NY).isoformat()
    end_date_range = pd.Timestamp(date.today(), tz=NY).isoformat()

    i +=1
    while start_date < end_date_range:
        start_date = pd.Timestamp(st_date, tz=NY).isoformat()
        end_date = pd.Timestamp(st_date + timedelta(days=4), tz=NY).isoformat()

        print(f"== Fetching minute bars for {i} {symbol} {start_date} - {end_date} ==")

        minutes = api.get_barset(symbol, 'minute', start=start_date, end=end_date).df
        # forward fill pandas dataframe so no data is missing - since alpaca is missing lots of data check if its a valid assumption to make
        minutes = minutes.resample('1min', ).ffill()

        # Alpaca gives all data atleast so no loop required but this one is for polygon whenever we use it (got Alpaca intraday from 1 Jan to uptil 10 Dec)

        for index, row in minutes[symbol].iterrows():

            # ***IMPORTANT: Change code to use 'open','close','high' keys etc instead of 0,1,2,3,4 index in "row" below

            cursor.execute("""
                    INSERT INTO stock_price_minute (stock_id, datetime, open, high, low, close, volume)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,(stock_ids[symbol], index.tz_localize(None).isoformat(),
                     row.to_list()[0],
                     row.to_list()[1],
                     row.to_list()[2],
                     row.to_list()[3],
                     row.to_list()[4]),)

        st_date = st_date + timedelta(days=7)

connection.commit()