
#recheck this strategy and also apply email notifs and cron job
# (after selecting right candidates for bollinger bands- also the print statements are a bit confusing

import sqlite3, pytz, datetime
import tulipy
import config
import alpaca_trade_api as tradeapi
import pandas as pd
from datetime import date, timedelta
from timezone import is_dst
from helpers import  calculate_quantity

#Following uses Newyork time for current date
NY = 'America/New_York'
current_date = datetime.datetime.utcnow().date().isoformat()
start_date = pd.Timestamp(current_date, tz=NY).isoformat()
end_date = pd.Timestamp(datetime.datetime.utcnow().date() + timedelta(days=1), tz=NY).isoformat()

print(current_date)

#Incase above dates don't work during weekdays etc, hardcode like below
# current_date ='2020-12-09'
# start_date = pd.Timestamp('2020-12-09', tz=NY).isoformat()
# end_date = pd.Timestamp('2020-12-10', tz=NY).isoformat()

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("""
SELECT id FROM strategy where name='bollinger bands'
""")

strategy_id = cursor.fetchone()['id']

#Collect "symbols" of all stocks on which we apply "opening_range_breakout" strategy
cursor.execute("""
SELECT symbol, name FROM stock
JOIN stock_strategy ON stock_strategy.stock_id = stock.id
WHERE stock_strategy.strategy_id = ?
""",(strategy_id,))
stocks = cursor.fetchall()
symbols =[stock['symbol'] for stock in stocks]

start_minute_bar = f"{current_date} 09:30:00-05:00"
end_minute_bar = f"{current_date} 15:59:00-05:00"

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)
orders = api.list_orders(status='all', after= current_date, limit= 500)
existing_order_symbols =[order.symbol for order in orders if order.status != 'canceled']

messages = []

for symbol in symbols:
    print(symbol)
    minute_bars = api.get_barset(symbol, 'minute', start=start_date, end=end_date).df
    market_open_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    market_open_bars = minute_bars.loc[market_open_mask]

    if len(market_open_bars) >= 20:
        closes = market_open_bars[symbol].close.values
        lower, middle, upper = tulipy.bbands(closes, period=20, stddev=2)

        current_candle = market_open_bars.iloc[-1]
        previous_candle = market_open_bars.iloc[-2]
        candle_range = current_candle[symbol].high - current_candle[symbol].low

#candle range condition still is an issue - sort it out - some stocks throw exception for??
        if current_candle[symbol].close > lower[-1] and previous_candle[symbol].close < lower[-2] and candle_range > 0.01:
            print(f"{symbol} closed above lower bollinger band")

            if symbol not in existing_order_symbols:

                limit_price = current_candle[symbol].close
                messages.append(f"placing order for {symbol} at {limit_price}")

                print(messages)

                try:
                    api.submit_order(
                        symbol=symbol,
                        side='buy',
                        type='limit',
                        qty=calculate_quantity(limit_price),
                        time_in_force='day',
                        order_class='bracket',
                        limit_price=limit_price,
                        take_profit=dict(
                            limit_price=(limit_price + (candle_range*3)),
                        ),
                        stop_loss=dict(
                            stop_price=(previous_candle[symbol].low),
                        )
                    )

                except Exception as e:
                    print(f"could not submit order {e}")

            else:
                print(f"Already an order for {symbol}, skipping")


# print(messages)