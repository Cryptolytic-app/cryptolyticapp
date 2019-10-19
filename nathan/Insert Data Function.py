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
    
    # poloniex API URL
    poloniex_base = ("https://poloniex.com/public?command=returnChartData"
                     "&currencyPair={trading_pair}&start={cutoff_time}"
                     "&period={period}")

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
                
                # this conditional is for getting data from cryptowatch api;
                # all data comes from cryptowatch except poloniex 5mn candles
                if exchange != 'poloniex' or period == '3600':
                    # use current time to calculate window for past 12 candles
                    now = round(time.time())
                    cutoff_time = str(now - (12 * int(period)))

                    # generate url
                    url = base_url.format(exchange=exchange,
                                          trading_pair=trading_pair,
                                          api_key=api_key,
                                          period=period,
                                          cutoff_time=cutoff_time)

                    try:
                        # get response
                        response = requests.get(url).json()
                        candles = response['result'][period]

                        # get timestamps for last 12 candles in database
                        cur.execute('''SELECT time FROM {schema}.{table_name}
                                    order by time desc limit 12'''.format(
                                    schema=schema, table_name=table_name))
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
                                                '(time, open, high, low, '
                                                'close, volume) VALUES (%s, '
                                                '%s, %s, %s, %s, %s)'
                                               ).format(schema=schema,
                                                        table_name=table_name)
                                cur.execute(insert_query, new_data)
                                
                    except:
                        pass
                    
                # now for the five minute candles from the poloniex api...
                else:
                    # first we have to reformat trading pair names;
                    # this requires separating currencies with an underscore,
                    # capitlizing the currency names, and
                    # using poloniex's preferred currency abbreviations:
                    # 'BCHABC' for bitcoin cash and 'STR' for stellar
                    
                    # dash is only 4-letter abbreviation at start of pair, so:
                    if trading_pair.startswith('dash'):
                        # DON'T overwrite trading_pair; needed for 1hr candles
                        cleaned_pair = '_'.join([trading_pair[4:],
                                                 trading_pair[:4]]).upper(
                        ).replace('BCH', 'BCHABC').replace('XLM', 'STR')
                    # ...the rest can be split after the third letter:
                    else:
                        cleaned_pair = '_'.join([trading_pair[3:],
                                                 trading_pair[:3]]).upper(
                        ).replace('BCH', 'BCHABC').replace('XLM', 'STR')
                    
                    # use current time to calculate window for past 12 candles
                    now = round(time.time())
                    # poloniex will not return evenly-spaced 5min candles
                    # unless the start time is a multiple of 300, so:
                    cutoff_time = str(((now - (12*int(period))) // 300) * 300)

                    # generate url (using cleaned_pair, not trading_pair)
                    url = poloniex_base.format(trading_pair=cleaned_pair,
                                               period=period,
                                               cutoff_time=cutoff_time)

                    try:
                        # response from poloniex is a list of candles
                        candles = requests.get(url).json()

                        # get timestamps for last 12 candles in database
                        cur.execute('''SELECT time FROM {schema}.{table_name}
                                    order by time desc limit 12'''.format(
                                    schema=schema, table_name=table_name))
                        results = cur.fetchall()
                        timestamps = [result[0] for result in results]

                        # ignoring final candle, since it is still open
                        completed_candles = candles[:-1]
                        # only add candles if timestamp not in database
                        for candle in completed_candles:
                            # candles structured as dicts;
                            # 'date' is opening time, but we want closing:
                            candle['date'] += int(period)
                            if candle['date'] not in timestamps:
                                # new list to contain data in dict:
                                new_data = []
                                # keys in dict with values we want:
                                features = ['date', 'open', 'high', 'low', 
                                            'close', 'volume']
                                # populating the list with values from dict:
                                for feature in features:
                                    new_data.append(candle[feature])
                                                            
                                insert_query = ('INSERT INTO '
                                                '{schema}.{table_name} '
                                                '(time, open, high, low, '
                                                'close, volume) VALUES (%s, '
                                                '%s, %s, %s, %s, %s)'
                                               ).format(schema=schema,
                                                        table_name=table_name)
                                cur.execute(insert_query, new_data)

                    except:
                        pass

    # commit and close
    conn.commit()
    cur.close()