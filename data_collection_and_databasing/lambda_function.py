import psycopg2 as ps
import requests
import time

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

# Supported exchanges
exchanges = {'bitfinex': bitfinex_pairs,
             'coinbase-pro': coinbase_pro_pairs,
             'hitbtc': hitbtc_pairs}

# Decrypt credentials
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

# Define credentials
credentials = {'POSTGRES_ADDRESS': DECRYPTED_POSTGRES_ADDRESS,
               'POSTGRES_PORT': DECRYPTED_POSTGRES_PORT,
               'POSTGRES_USERNAME': DECRYPTED_POSTGRES_USERNAME,
               'POSTGRES_PASSWORD': DECRYPTED_POSTGRES_PASSWORD,
               'POSTGRES_DBNAME': DECRYPTED_POSTGRES_DBNAME,
               'API_KEY': DECRYPTED_API_KEY}

def insert_data(credentials, exchanges, periods=['300','3600']):
    """This function connects to a database and inserts live data from
    cryptowatch API into tables for each exchange/trading pair combination.
    Option to select a period ('60', '300', '3600'; default=['300', '3600'])
    """

    # connect to database
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])
    # create cursor
    cur = conn.cursor()

    # cryptowatch API URL
    base_url = ('https://api.cryptowat.ch/markets/{exchange}/{trading_pair}/'
                'ohlc?apikey={api_key}&periods={period}')

    # cryptowatch api key
    api_key = credentials['API_KEY']

    # iterate through all exchange/trading pair combinations
    for exchange in exchanges:
        for trading_pair in exchanges[exchange]:
            for period in periods:

                # define schemas in database
                if period =='300':
                    schema = 'fiveminute'
                if period =='3600':
                    schema = 'onehour'

                # table name in database
                table_name = ('_'.join(exchange.split('-')) + '_' +
                              trading_pair)
                
                # cryptowatch wants the trading pair without the underscore
                cleaned_trading_pair = trading_pair.replace('_', '')

                # generate url
                url = base_url.format(exchange=exchange,
                                      trading_pair=cleaned_trading_pair,
                                      api_key=api_key,
                                      period=period)
                
                try:
                    # get response
                    response = requests.get(url).json()
                    candles = response['result'][period]
                    
                    # get timestamps for last 12 candles in database
                    cur.execute('''SELECT closing_time FROM {schema}.
                                {table_name} order by closing_time desc
                                '''.format(schema=schema,
                                           table_name=table_name))
                    results = cur.fetchall()
                    timestamps = [result[0] for result in results]

                    # ignoring final candle, since it is still open
                    completed_candles = candles[:-1]
                    # only add candles if timestamp not in database
                    for candle in completed_candles:
                        if candle[0] not in timestamps:
                            # we don't need 7th value returned by api...
                            new_data = candle[:6]
                            insert_query = ('INSERT INTO '
                                            '{schema}.{table_name} '
                                            '(closing_time, open, high, low, '
                                            'close, base_volume) VALUES (%s, '
                                            '%s, %s, %s, %s, %s)'
                                           ).format(schema=schema,
                                                    table_name=table_name)
                            cur.execute(insert_query, new_data)
                                
                except:
                    pass

    # commit and close
    conn.commit()
    cur.close()