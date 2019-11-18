import psycopg2 as ps
import requests
import json
import time
import boto3
import os
from base64 import b64decode

# The packages and code included in this file are written exactly as they appear
# in AWS Lambda.

# Supported trading pairs
coinbase_pro_pairs = ['bch_btc', 'bch_usd', 'btc_usd', 'btc_usdc', 'dash_btc',
                      'dash_usd', 'eos_btc', 'eos_usd', 'etc_usd', 'eth_btc',
                      'eth_usd', 'eth_usdc', 'ltc_btc', 'ltc_usd', 'xrp_btc',
                      'xrp_usd', 'zec_usdc', 'zrx_usd']

bitfinex_pairs = ['bch_btc', 'bch_usd', 'bch_usdt', 'btc_usd', 'btc_usdt', 
                  'dash_btc', 'dash_usd', 'eos_btc', 'eos_usd', 'eos_usdt', 
                  'etc_usd', 'eth_btc', 'eth_usd', 'eth_usdt', 'ltc_btc',
                  'ltc_usd', 'ltc_usdt', 'xrp_btc', 'xrp_usd', 'zec_usd',
                  'zrx_usd']

hitbtc_pairs = ['bch_btc', 'bch_usdt', 'btc_usdc', 'btc_usdt', 'dash_btc',
                'dash_usdt', 'eos_btc', 'eos_usdt', 'etc_usdt', 'eth_btc', 
                'eth_usdc', 'eth_usdt', 'ltc_btc', 'ltc_usdt', 'xrp_btc', 
                'xrp_usdt', 'zec_usdt', 'zrx_usdt']

gemini_pairs = ['bch_btc', 'bch_usd', 'btc_usd', 'eth_btc', 'eth_usd',
                'ltc_btc', 'ltc_usd', 'zec_usd']

kraken_pairs = ['bch_btc', 'bch_usd', 'btc_usd', 'dash_btc', 'dash_usd',
                'eos_btc', 'eos_usd', 'etc_usd', 'eth_btc', 'eth_usd', 
                'ltc_btc', 'ltc_usd', 'xrp_btc', 'xrp_usd', 'zec_usd']

# Supported exchanges
exchanges = {'bitfinex': bitfinex_pairs,
             'coinbase-pro': coinbase_pro_pairs,
             'hitbtc': hitbtc_pairs,
             'gemini': gemini_pairs,
             'kraken': kraken_pairs}

# Hide credentials
POSTGRES_ADDRESS = os.environ['POSTGRES_ADDRESS']
POSTGRES_PORT = os.environ['POSTGRES_PORT']
POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_DBNAME = os.environ['POSTGRES_DBNAME']
API_KEY = os.environ['API_KEY']

# Define credentials
credentials = {'POSTGRES_ADDRESS' : POSTGRES_ADDRESS,
               'POSTGRES_PORT' : POSTGRES_PORT,
               'POSTGRES_USERNAME' : POSTGRES_USERNAME,
               'POSTGRES_PASSWORD' : POSTGRES_PASSWORD,
               'POSTGRES_DBNAME' : POSTGRES_DBNAME,
               'API_KEY' : API_KEY}

# Define five minute insert data function.
def insert_fivemin_data(credentials, exchanges, period='300'):
    """This function connects to a database and inserts live data from the
    Cryptowatch API into tables for each exchange/trading pair combination.
    Option to select a period ('60', '300', '3600'; set default='300')
    """
    # Connect to database.
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])
    
    # Create cursor.
    cur = conn.cursor()
    
    # Cryptowatch API url
    base_url = ('https://api.cryptowat.ch/markets/{exchange}/{trading_pair}/'
                'ohlc?apikey={api_key}&periods={period}')
    
    # Cryptowatch API key
    api_key = credentials['API_KEY']
    
    # Define schema in database.
    schema = 'fiveminute'

    # Iterate through all exchange/trading pair combinations.
    for exchange in exchanges:
    
        for trading_pair in exchanges[exchange]:
                
            # Table name as it appears in database.
            table_name = ('_'.join(exchange.split('-')) + '_' +
                              trading_pair)
                
            # Cryptowatch wants the trading pair without the underscore.
            cleaned_trading_pair = trading_pair.replace('_', '')

            # Generate url.
            url = base_url.format(exchange=exchange,
                                      trading_pair=cleaned_trading_pair,
                                      api_key=api_key,
                                      period=period)              
                
            try:
                # Get response from Cryptowatch.
                response = requests.get(url).json()
                    
                # Define candles for data returned from the API and only include the actual candle.
                candles = response['result'][period]
                    
                # Retrieve candles already in database. Collect roughly 12 hours worth of candles.
                cur.execute('''SELECT closing_time FROM {schema}.
                                {table_name} ORDER BY closing_time DESC LIMIT 150
                                '''.format(schema=schema,
                                           table_name=table_name))
                
                # Save those candles to results.  
                results = cur.fetchall()
                    
                # Select only the timestamps from the returned candles and save to timestamps.
                timestamps = [result[0] for result in results]
                    
                # Take most recent 12 hours worth of candles (144 candles) of the candles returned by the API. 
                # Ignore the final candle because it is still open.
                completed_candles = candles[-144:-1]
                    
                # Check if candle is in database.
                # Only add candles if timestamp is not in database.
                missing_candles = []
                for candle in completed_candles:
                        
                    if candle[0] not in timestamps:
                            
                          # We don't need 7th value returned by API...
                          new_candle = candle[:6]
                          missing_candles.append(new_candle)

                insert_query = ('INSERT INTO '
                                            '{schema}.{table_name} '
                                            '(closing_time, open, high, low, '
                                            'close, base_volume) VALUES (%s, '
                                            '%s, %s, %s, %s, %s)'
                                           ).format(schema=schema,
                                                    table_name=table_name)
                            
                cur.executemany(insert_query, missing_candles)
                                
            except:
                
                pass

            # Commit after each table.
            conn.commit()
            
    # Close.
    
    cur.close()
    conn.close()
   
def lambda_handler(event, context):
    period = '300'
    insert_fivemin_data(credentials, exchanges, period)
    return 'Success!'