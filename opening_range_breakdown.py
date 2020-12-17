
#need to run cronjob every minute to place orders 15 minutes after open when conditions meet

import sqlite3, pytz, datetime
import config
import alpaca_trade_api as tradeapi
import pandas as pd
from datetime import date, timedelta
from timezone import is_dst
from helpers import calculate_quantity

#For email notifs
import smtplib, ssl
context = ssl.create_default_context()

#Following uses Newyork time for current date
NY = 'America/New_York'
current_date = datetime.datetime.utcnow().date().isoformat()
start_date = pd.Timestamp(current_date, tz=NY).isoformat()
end_date = pd.Timestamp(datetime.datetime.utcnow().date() + timedelta(days=1), tz=NY).isoformat()

#Incase above dates don't work during weekdays etc, hardcode like below
# current_date ='2020-12-09'
# start_date = pd.Timestamp('2020-12-09', tz=NY).isoformat()
# end_date = pd.Timestamp('2020-12-10', tz=NY).isoformat()

connection = sqlite3.connect(config.DB_FILE)
connection.row_factory = sqlite3.Row
cursor = connection.cursor()

cursor.execute("""
SELECT id FROM strategy where name='opening_range_breakdown'
""")
strategy_id = cursor.fetchone()['id']


#Collect "symbols" of all stocks on which we apply "opening_range_breakdown" strategy
cursor.execute("""
SELECT symbol, name FROM stock
JOIN stock_strategy ON stock_strategy.stock_id = stock.id
WHERE stock_strategy.strategy_id = ?
""",(strategy_id,))
stocks = cursor.fetchall()
symbols =[stock['symbol'] for stock in stocks]

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

#List of all the orders already placed (includes all after the date below) . Limit is max stocks in response

#Check if its T09:30:00Z or T13:30:00Z - Check the condition of current date...
orders = api.list_orders(status='all', after= current_date, limit= 500)
existing_order_symbols =[order.symbol for order in orders if order.status != 'canceled']

#Get minute data for the first 15 minutes - workaround since alpaca data doesn't have data for all continuous minutes

if is_dst():
    start_minute_bar = f"{current_date} 09:30:00-05:00"
    end_minute_bar = f"{current_date} 09:45:00-05:00"
else:
    start_minute_bar = f"{current_date} 09:30:00-04:00"
    end_minute_bar = f"{current_date} 09:45:00-04:00"


messages = []
#Don't have Polygon API. Using Alpaca intraday although some ticks missing. Can use IEX but that uses up monthly limit
for symbol in symbols:

    # minute_bars = api.polygon.historic_agg_v2(symbol, 1, 'minute', _from='2020-12-04', to='2020-12-04').df
    minute_bars = api.get_barset(symbol, 'minute', start=start_date, end=end_date).df

    # "opening_range_bars" has first 15 minutes of data - missing for some symbols since using alpaca instead of polygon
    opening_range_mask = (minute_bars.index >= start_minute_bar) & (minute_bars.index < end_minute_bar)
    opening_range_bars = minute_bars.loc[opening_range_mask]

    # find the price range (high-low) in the first 15 minutes
    opening_range_low = opening_range_bars[symbol]['low'].min()
    opening_range_high = opening_range_bars[symbol]['high'].max()
    opening_range = opening_range_high - opening_range_low

    # "after_opening_range_mask" has data after the 15 first minutes
    after_opening_range_mask = minute_bars.index >= end_minute_bar
    after_opening_range_bars = minute_bars.loc[after_opening_range_mask]

    # "after_opening_range_breakdown" filters "after_opening_range_mask" for data which is higher than the high in the price range of first 15 minutes
    after_opening_range_breakdown = after_opening_range_bars[after_opening_range_bars[symbol]['close'] < opening_range_low]

    # print(symbol)
    # print('opening_range_bars',opening_range_bars)
    # print("opening_range_bars",opening_range_bars)
    # print("after_opening_range_breakdown",after_opening_range_breakdown)

    #Limit_price is the first cell in the "after_opening_range_breakdown"
    if not after_opening_range_breakdown.empty and opening_range > 0.01:
        if symbol not in existing_order_symbols:

            limit_price = after_opening_range_breakdown.iloc[0][symbol]['close']
            message = f"selling short {symbol} at {limit_price}, closed below {opening_range_low} \n\n {after_opening_range_breakdown.iloc[0]}\n\n"
            messages.append(message)

            print(message)

            try:

                api.submit_order(
                        symbol=symbol,
                        side='sell',
                        type='limit',
                        qty=calculate_quantity(limit_price),
                        time_in_force='day',
                        order_class='bracket',
                        limit_price= limit_price,
                        take_profit=dict(
                            limit_price= limit_price - opening_range,
                        ),
                        stop_loss=dict(
                            stop_price=limit_price + opening_range,
                        )
                )

                # Email Notification (works) and SMS (not working)
                with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
                    server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)

                    email_message = f"Subject: Trade Notifications for {current_date}\n\n"
                    email_message += "\n\n".join(messages)
                    server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_ADDRESS, email_message)

                    # doesn't work for phone notifications - Require Google fi Account
                    # server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_SMS, email_message)



            except Exception as e:
                print(f"could not submit order {e}")

        else:
            print(f"Already an order for {symbol}, skipping")


print(messages)

