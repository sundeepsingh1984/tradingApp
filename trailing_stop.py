import alpaca_trade_api as tradeapi
import config, tulipy
from helpers import calculate_quantity

api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

#Sample stocks

# symbols = ['SPY','IWM','DIA']
#
# for symbol in symbols:
#     quote = api.get_last_quote(symbol)
#
#     api.submit_order(
#         symbol =symbol,
#         side='buy',
#         type='market',
#         qty= calculate_quantity(quote.bidprice),
#         time_in_force = 'day'
#     )
# #Once you have verified that your positions are filled, then you should do trailing stop loss
# orders = api.list_orders()
# positions = api.list_positions()
#
# #You can have mutiple orders with varying sell quantity to let your profits run, also can calculate number of shares dynamically from alpaca api.
# api.submit_order(
#     symbol='IWM',
#     side='sell',
#     qty=57,
#     time_in_force='day',
#     type = 'trailing_stop',
#     trail_price='0.20'
# )
#
# api.submit_order(
#     symbol='DIA',
#     side='sell',
#     qty=5,
#     time_in_force='day',
#     type = 'trailing_stop',
#     trail_percent= '0.70'
# )

symbol1='NIO'
#can use ATR(Average True Range) to calculate stop loss
# Note that we are using day below
daily_bars = api.get_barset(symbol1, 'day', start='2020-11-11', end='2020-12-11').df
# print(daily_bars)
atr = tulipy.atr(daily_bars[symbol1].high.values,daily_bars[symbol1].low.values, daily_bars[symbol1].close.values,14)

print(atr)
