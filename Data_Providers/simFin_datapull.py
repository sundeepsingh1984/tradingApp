

# Import the main functionality from the SimFin Python API.
import simfin as sf

# Import names used for easy access to SimFin's data-columns.
from simfin.names import *

sf.set_data_dir('//')
sf.set_api_key(api_key="vHOwuZOEK3CR3tLhv4wHjVohTK9Brv2h")

df1 = sf.load(dataset='income', variant='annual-full', market='us')

df_prices_latest = sf.load_shareprices(variant='latest', market='us')

print(df_prices_latest.tail())

#See how the web API retrives info and if that's easier or more direct