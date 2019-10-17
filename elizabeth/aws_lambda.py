import psycopg2 as ps
import requests
import json
import time
import boto3
import os
from base64 import b64decode

#  supported trading pairs
bitfinex_pairs = ['bchbtc', 'bchusd', 'btcusd',
                  'btcusdt', 'dashbtc', 'dashusd', 'eosbtc', 'eosusd',
                  'eosusdt', 'etcbtc', 'etcusd', 'ethbtc', 'ethusd',
                  'ltcbtc', 'ltcusd',
                  'xlmusd', 'xrpbtc', 'xrpusd', 'zecbtc',
                  'zecusd']

coinbase_pro_pairs = ['bchbtc', 'bchusd', 'btcusd',
                      'btcusdc', 'dashbtc', 'dashusd', 'eosbtc', 'eosusd',
                      'etcbtc', 'etcusd', 'ethbtc', 'ethusd', 'ethusdc',
                      'ltcbtc', 'ltcusd',
                      'xlmusd', 'xrpbtc', 'xrpusd',
                      'zecbtc', 'zecusdc']

poloniex_pairs = ['bchbtc', 'bchusdc',
                  'btcusdc', 'btcusdt', 'dashbtc', 'dashusdc', 'dashusdt',
                  'eosbtc', 'eosusdc', 'eosusdt', 'etcbtc',
                  'etcusdt', 'ethbtc', 'ethusdc', 'ethusdt',
                  'ltcbtc', 'ltcusdc',
                  'xlmusdc', 'xlmusdt', 'xrpbtc',
                  'xrpusdc', 'xrpusdt', 'zecbtc', 'zecusdc', 'zecusdt']

# supported exchanges
exchanges = {'bitfinex': bitfinex_pairs,
             'coinbase-pro': coinbase_pro_pairs,
             'poloniex': poloniex_pairs}

# decrypt credentials
ENCRYPTED_POSTGRES_ADDRESS = os.environ['POSTGRES_ADDRESS']
DECRYPTED_POSTGRES_ADDRESS = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_POSTGRES_ADDRESS))[
    'Plaintext'].decode()

ENCRYPTED_POSTGRES_PORT = os.environ['POSTGRES_PORT']
DECRYPTED_POSTGRES_PORT = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_POSTGRES_PORT))[
    'Plaintext'].decode()

ENCRYPTED_POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
DECRYPTED_POSTGRES_USERNAME = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_POSTGRES_USERNAME))[
    'Plaintext'].decode()

ENCRYPTED_POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
DECRYPTED_POSTGRES_PASSWORD = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_POSTGRES_PASSWORD))[
    'Plaintext'].decode()

ENCRYPTED_POSTGRES_DBNAME = os.environ['POSTGRES_DBNAME']
DECRYPTED_POSTGRES_DBNAME = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_POSTGRES_DBNAME))[
    'Plaintext'].decode()

ENCRYPTED_POSTGRES_PORT = os.environ['POSTGRES_PORT']
DECRYPTED_POSTGRES_PORT = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_POSTGRES_PORT))[
    'Plaintext'].decode()

ENCRYPTED_API_KEY = os.environ['API_KEY']
DECRYPTED_API_KEY = boto3.client('kms').decrypt(CiphertextBlob=b64decode(ENCRYPTED_API_KEY))['Plaintext'].decode()

credentials = {'POSTGRES_ADDRESS': DECRYPTED_POSTGRES_ADDRESS,
               'POSTGRES_PORT': DECRYPTED_POSTGRES_PORT,
               'POSTGRES_USERNAME': DECRYPTED_POSTGRES_USERNAME,
               'POSTGRES_PASSWORD': DECRYPTED_POSTGRES_PASSWORD,
               'POSTGRES_DBNAME': DECRYPTED_POSTGRES_DBNAME,
               'API_KEY': DECRYPTED_API_KEY
               }

def insert_data(credentials, exchanges, period='300'):
    """ This function connects to a database and inserts live data
        from cryptowatch API into tables for each exchange/trading
        pair combination. Option to select a period (60, 300, 3600; default=300)"""

    # connect to database
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'], database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'], password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])
    # create cursor
    cur = conn.cursor()

    # cryptowat.ch API URL
    base_url = 'https://api.cryptowat.ch/markets/{exchange}/{trading_pair}/ohlc?apikey={api_key}&period={period}&after={cutoff_time}'

    # api key
    api_key = credentials['API_KEY']

    # iterate through all exchange/trading pair combinations
    for exchange in exchanges:
        for trading_pair in exchanges[exchange]:

            # table name in database
            table_name = '_'.join(exchange.split('-')) + '_' + trading_pair

            # get current time and end time to limit number of results returned
            now = round(time.time())
            cutoff_time = str(now - 10000)

            # generate url
            url = base_url.format(exchange=exchange, trading_pair=trading_pair
                                  , api_key=api_key, period=period, cutoff_time=cutoff_time)

            try:
                # get request ohlcv data
                response = requests.get(url).json()
                now = round(time.time()) - 60

                if len(response['result'][period]) > 0:

                    if response['result'][period][-1][0] < now:
                        new_data = response['result'][period][-1][:6]

                    # get second to last data row if the last data row doesn't work
                    elif len(response['result'][period]) > 1:
                        new_data = response['result'][period][-2][:6]

                        # insert into table
                    insert_query = """INSERT INTO {table_name} (time, open, high, 
                                    low, close, volume) VALUES (%s, %s, %s, %s,
                                    %s, %s)""".format(table_name=table_name)
                    cur.execute(insert_query, new_data)

            except:
                pass

    # commit and close
    conn.commit()
    cur.close()


def lambda_handler(event, context):

    period = '300'
    insert_data(credentials, exchanges, period)
    return 'success!'
