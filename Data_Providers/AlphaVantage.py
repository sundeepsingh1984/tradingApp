import config

#Intraday from AlphaVantage
import config
from alpha_vantage.timeseries import TimeSeries
ts = TimeSeries(key=config.API_KEY_ALPHA ,output_format='pandas', indexing_type='date')
# Get json object with the intraday data and another with  the call's metadata
data, meta_data = ts.get_intraday(symbol='MSFT',interval='1min', outputsize='full')
print(data)




