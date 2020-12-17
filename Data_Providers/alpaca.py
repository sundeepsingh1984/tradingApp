

#put a crontab to run this half hour before market close  ???

import alpaca_trade_api as tradeapi
import config


api = tradeapi.REST(config.API_KEY, config.SECRET_KEY, base_url=config.API_URL)

response = api.close_all_positions()
print(response)