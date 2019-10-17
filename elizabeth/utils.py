# credentials
credentials = {'POSTGRES_ADDRESS': '',
               'POSTGRES_PORT': '',
               'POSTGRES_USERNAME': '',
               'POSTGRES_PASSWORD': '',
               'POSTGRES_DBNAME': '',
               'API_KEY': ''
               }

# supported trading pairs
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

def create_tables(credentials, exchanges):
    """ Connects to a database and creates unique tables for each
        cryptocurrency trading pair/exchange combination"""

    # create connection
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'], database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'], password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    # create cursor
    cur = conn.cursor()

    # create a list of table names
    table_list = []
    for exchange in exchanges:
        for trading_pair in exchanges[exchange]:
            for i in ['300', '3600']:
                table = exchange + '_' + trading_pair + '_' + i
                table_list.append(table)

    # create each table in table_list
    for table in table_list:
         cur.execute('''
                    CREATE TABLE {table}
                    (
                    time integer,
                    open float,
                    high float,
                    low float,
                    close float,
                    volume float
                    );
                    '''.format(table_name=table))

    # commit and close
    conn.commit()
    print("Table created Successfully!")
    conn.close()

# run function
# create_tables(credentials, exchanges)