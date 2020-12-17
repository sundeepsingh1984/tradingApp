import sqlite3, config
from sqlite3 import Error
import pandas as pd
import simfin as sf
import os
from simfin.names import *
import numpy as np
from datetime import date

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()
cursor.execute("""SELECT id, symbol, name FROM stock""")
rows = cursor.fetchall()

symbols = []
stock_dict = {}
for row in rows:
    symbol = row['symbol']
    symbols.append(symbol)
    stock_dict[symbol] = row['id']

#SIMFIN credentials and root drive
sf.set_data_dir(config.SIMFIN_DIR)
sf.set_api_key(api_key= config.API_KEY_SIMFIN)

#Get list of simFin Tickers & match with Alpaca Tickers
simfin_tickers = sf.load_companies(market='us').index
simfin_companies_name = sf.load_companies(market='us')['Company Name']
simfin_tickers = [simfin_tickers[i] for i in range(len(simfin_tickers))]

get_errors = []
ignore_list =[]

for ticks in simfin_tickers:
    try:
        stock_dict[ticks]
    except Exception as e:
        get_errors.append(e.args)

# Ignore list has simfin tickers which couldn't be matched/found with Alpaca
ignore_list = [list(get_errors[i])[0] for i in range(len(get_errors))]
final_ticker_list = [x for x in simfin_tickers if x not in ignore_list]

#Need to get latest data - this is still getting few days old data
df = sf.load_shareprices(variant='daily', market='us')

# INDUSTRY & Sector
industry_df = sf.load_industries()
company_df = sf.load_companies(market='us')

#Choosing one Apple stock to calculate date_index - assuming index stays same across all stocks
date_index = df.loc['AAPL'].index

for ticks in final_ticker_list:

    try:
        sector_tick = industry_df.loc[company_df.loc[ticks]['IndustryId']][0]
        industry_tick = industry_df.loc[company_df.loc[ticks]['IndustryId']][1]

    # Some tickers don't have either industry or sector code so need to let this pass
    except Exception as e:
        print(e)
        pass

    try:

        for dt in date_index:
            s_id = df.loc['A',dt]['SimFinId']
            s_open = df.loc['A',dt]['Open']
            s_low = df.loc['A', dt]['Low']
            s_high = df.loc['A', dt]['High']
            s_close = df.loc['A', dt]['Close']
            s_adj_close = df.loc['A', dt]['Adj. Close']
            s_div = df.loc['A', dt]['Dividend']
            s_vol = df.loc['A', dt]['Volume']
            s_share_out = df.loc['A', dt]['Shares Outstanding']

            cursor.execute("""
                    INSERT INTO simfin_stock_price (stock_id, simfin_id, sector, industry, date, open, low, high, close, adj_close, dividends, volume, shares_outstanding)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (stock_dict[ticks], s_id, sector_tick, industry_tick, dt.date(), s_open, s_low, s_high, s_close, s_adj_close, s_div, s_vol, s_share_out))
        print(f"Inserted price for {ticks}")
    except Exception as e:
        print(e)
        pass
connection.commit()