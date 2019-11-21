import psycopg2 as ps

# Credentials
credentials = {'POSTGRES_ADDRESS' : '#',
               'POSTGRES_PORT' : '#',
               'POSTGRES_USERNAME' : '#',
               'POSTGRES_PASSWORD' : '#',
               'POSTGRES_DBNAME' : '#',
               'API_KEY' : '#'}

# Define postgres_db_conn function.
def postgres_db_conn(credentials):

    # Create database connection.
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    cur = conn.cursor()
    return conn, cur

# Before running the functions below, add two schemas to
# your PostgreSQL database, one named 'fiveminute', the other named 'onehour'.

# Define currency pairs within each exchange and create the names of the 
# tables for each exchange.

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

hitbtc_table_list = ['hitbtc_' + pair for pair in hitbtc_pairs]

bitfinex_table_list = ['bitfinex_' + pair for pair in bitfinex_pairs]

coinbase_pro_table_list = ['coinbase_pro_' + pair for pair in 
                           coinbase_pro_pairs]

gemini_table_list = ['gemini_' + pair for pair in gemini_pairs]

kraken_table_list = ['kraken_' + pair for pair in kraken_pairs]

# Define create_tables function.
def create_tables(credentials):
    '''Connects to a PostgreSQL database and adds tables to each respective 
    schema.'''

    # Create connection and cursor to database.
    conn, cur = create_conn(credentials)
    
    # Define schemas and table_list.
    schemas = ['fiveminute', 'onehour']
    
    table_list = (hitbtc_table_list + bitfinex_table_list + 
                  coinbase_pro_table_list + gemini_table_list + kraken_table_list)
    
    # Loop through schemas and table_list.
    for schema in schemas:
        for table_name in table_list:
            cur.execute('''
            CREATE TABLE {schema}.{table_name}
            (closing_time integer,
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
 
# Define insert_csv_to_db function.
# Within Jupyter Lab, a folder entitled "data" was created. A folder for each exchange was
# then created within the "data" folder. Each exchange folder then included csv files for all 
# of the trading pairs supported on that respective exchange and for all intervals (300 and 3600).
def insert_csv_to_db(credentials):
  '''Connects to a PostgreSQL database and imports csv files into a specified schema
  and table name.'''
    
    # Create connection and cursor to database.
    conn, cur = create_conn(credentials)

    print("Connect to database.")

    for directory in os.listdir('data'):
        if directory != '.DS_Store':
            for filename in os.listdir('data/' + directory):
                if filename.endswith('300.csv'):
                    schema = 'fiveminute'
                    table_name = filename.replace('_300.csv', '')
                elif filename.endswith('3600.csv'):
                    schema = 'onehour'
                    table_name = filename.replace('_3600.csv', '')
                with open('data/' + directory + '/' + filename, 'r') as f:
                    # Skip the header row.
                    next(f) 
                    cur.copy_from(f, '{schema}.{table_name}'.format(schema=schema, table_name=table_name), sep=',')
                    conn.commit()

    conn.close()
    print('Done!')

# Define drop_column function.
def drop_column(credentials):
  '''Connects to a PostgreSQL database and drops a table column, in this case ID,
  from all tables within each schema.'''
    
    # Create connection and cursor to database.
    conn, cur = create_conn(credentials)
    
    print("Connect to database.")
    
    schemas = ['fiveminute', 'onehour']
    table_list = (hitbtc_table_list + bitfinex_table_list + coinbase_pro_table_list + gemini_table_list + kraken_table_list)
    
    for schema in schemas:
        for table_name in table_list:
            cur.execute('''ALTER TABLE {schema}.{table_name} DROP COLUMN ID;'''.format(schema=schema, table_name=table_name))
    
    # Commit and close. Verify that tables were created successfully.
    conn.commit()
    
    print("Column removed.")
    conn.close()
