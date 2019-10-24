import psycopg2 as ps
import requests
import pandas as pd

# Define credentials.

credentials = {'POSTGRES_ADDRESS' : '#',
               'POSTGRES_PORT' : '#',
               'POSTGRES_USERNAME' : '#',
               'POSTGRES_PASSWORD' : '#',
               'POSTGRES_DBNAME' : '#',
               'API_KEY' : '#'}

# Define currency pairs within each exchange and create the names of the tables for each exchange.

hitbtc_pairs = ['bch_btc', 'bch_usdt', 'btc_usdc', 'btc_usdt', 'dash_btc',
                'dash_usdt', 'eos_btc', 'eos_usdt', 'etc_usdt', 'eth_btc', 
                'eth_usdc', 'eth_usdt', 'ltc_btc', 'ltc_usdt', 'xrp_btc', 
                'xrp_usdt', 'zec_usdt', 'zrx_usdt']
bitfinex_pairs = ['bch_btc', 'bch_usd', 'bch_usdt', 'btc_usd', 'btc_usdt', 
                  'dash_btc', 'dash_usd', 'eos_btc', 'eos_usd', 'eos_usdt', 
                  'etc_usd', 'eth_btc', 'eth_usd', 'eth_usdt', 'ltc_btc', 'ltc_usd', 
                  'ltc_usdt', 'xrp_btc', 'xrp_usd', 'zec_usd', 'zrx_usd']
coinbase_pro_pairs = ['bch_btc', 'bch_usd', 'btc_usd', 'btc_usdc', 'dash_btc',
                      'dash_usd', 'eos_btc', 'eos_usd', 'etc_usd', 'eth_btc',
                      'eth_usd', 'eth_usdc', 'ltc_btc', 'ltc_usd', 'xrp_btc',
                      'xrp_usd', 'zec_usdc', 'zrx_usd']

hitbtc_table_list = ['hitbtc_' + pair for pair in hitbtc_pairs]

bitfinex_table_list = ['bitfinex_' + pair for pair in bitfinex_pairs]

coinbase_pro_table_list = ['coinbase_pro_' + pair for pair in coinbase_pro_pairs]

# Define create_tables function.

def create_tables(credentials):
  '''Connects to a PostgreSQL database and adds tables to each respective schema.'''

  # Create connection.
  conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                  port=credentials['POSTGRES_PORT'],
                  user=credentials['POSTGRES_USERNAME'],
                  password=credentials['POSTGRES_PASSWORD'],
                  database=credentials['POSTGRES_DBNAME'])


  # Create a cursor.
  cur = conn.cursor()
  
  
  # Define schemas and table_list.
  schemas = ['fiveminute', 'onehour']
  
  table_list = hitbtc_table_list + bitfinex_table_list + coinbase_pro_table_list
  

  # Loop through schemas and table_list.
  for schema in schemas:
    for table_name in table_list:
      cur.execute('''
      CREATE TABLE {schema}.{table_name}
      (
      closing_time integer,
      open float,
      high float,
      low float,
      close float,
      base_volume float
      );'''.format(schema=schema, table_name=table_name))

  # Commit and close. Verify that tables were created successfully.
  conn.commit()

  print("Tables created successfully!")

  conn.close()