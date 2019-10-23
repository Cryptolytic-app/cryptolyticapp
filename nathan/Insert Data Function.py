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
                'ohlc?apikey={api_key}&periods={period}&after={cutoff_time}')

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
                
                # use current time to calculate window for past 12 candles
                now = round(time.time())
                cutoff_time = str(now - (12 * int(period)))
                
                # cryptowatch wants the trading pair without the underscore
                cleaned_trading_pair = trading_pair.replace('_', '')

                # generate url
                url = base_url.format(exchange=exchange,
                                      trading_pair=cleaned_trading_pair,
                                      api_key=api_key,
                                      period=period,
                                      cutoff_time=cutoff_time)

                try:
                    # get response
                    response = requests.get(url).json()
                    candles = response['result'][period]

                    # get timestamps for last 12 candles in database
                    cur.execute('''SELECT closing_time FROM {schema}.
                                {table_name} order by closing_time desc limit
                                12'''.format(schema=schema,
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