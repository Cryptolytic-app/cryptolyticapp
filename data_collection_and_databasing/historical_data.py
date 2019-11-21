# Before running this py file, you should have a folder named 'coinbase_pro',
# a folder named 'bitfinex', a folder named 'hitbtc', a folder named 'gemini',
# and a folder named 'kraken' in your repository.
# This py file will add 36 csvs to the 'coinbase_pro' folder totalling 
# 159.6 MB, 42 csvs to the 'bitfinex' folder totalling 286 MB, 36 csvs to
# the 'hitbtc' folder totalling 256.2 MB, 16 csvs to the 'gemini' folder
# totalling 4.2 MB, and 30 csvs to the 'kraken' folder totalling 10.4 MB
# for a grand total of 160 csvs across five folders totalling 716.4 MB. The
# csvs are created with their indices; if you would like to ignore the index 
# when importing a csv with pandas.read_csv, add index_col=0 as a parameter.

import requests
import pandas as pd
import datetime as DT
import time

# list of all trading pairs for which we want to grab historical candles
trading_pairs = ['bch_btc', 'bch_usd', 'bch_usdc', 'bch_usdt', 'btc_usd',
                 'btc_usdc', 'btc_usdt', 'dash_btc', 'dash_usd', 'dash_usdc',
                 'dash_usdt', 'eos_btc', 'eos_usd', 'eos_usdc', 'eos_usdt',
                 'etc_usd', 'etc_usdc', 'etc_usdt', 'eth_btc', 'eth_usd',
                 'eth_usdc', 'eth_usdt', 'ltc_btc', 'ltc_usd', 'ltc_usdc',
                 'ltc_usdt', 'xrp_btc', 'xrp_usd', 'xrp_usdc', 'xrp_usdt',
                 'zec_usd', 'zec_usdc', 'zec_usdt', 'zrx_usd', 'zrx_usdc',
                 'zrx_usdt']

# length in seconds of candle intervals we want to grab; 1 hour and 5 minutes
periods = [(60*60), (5*60)]

# beginning of october 2019 in Unix time; to use as end of historical window
start_of_oct_2019 = 1569888000

# beginning of 2015 in Unix time; to use as start of historical window
start_of_2015 = 1420070400

# function for grabbing historical candles from coinbase pro
def get_coinbase_pro_candles(trading_pairs=trading_pairs,
                             periods=periods,
                             starting=start_of_2015,
                             ending=start_of_oct_2019):
    """This function gets historical candle data from the Coinbase Pro API.
    The trading_pairs parameter takes a list of trading pairs, where the 
    currencies in the pair are separated by an underscore and each currency is
    represented by its lowercase symbol on cryptowatch. The periods parameter
    takes a list of candle intervals in seconds. The starting parameter takes
    a Unix timestamp for the start of the historical window. The ending 
    parameter takes a Unix timestamp for the end of the historical window. 
    The function creates a csv for each trading pair in the trading_pairs list
    for each candle interval in the periods list. The csvs contain all of the
    available candles from the time specified by the starting parameter to the
    time specified by the ending parameter. The csvs are added to the 
    coinbase_pro folder in the repository. The filenames take the format 
    exchange_pair_interval.csv; coinbase_pro_btc_usd_3600.csv contains 
    one hour (3600 seconds) candles for the BTC/USD trading pair from Coinbase
    Pro. The candles consist of six values: closing_time, open, high, low,
    close, base_volume. The closing time is given as a Unix timestamp."""
    
    # printing the name of the exchange to track progress in case this
    # function is run alongside similar functions for other exchanges
    print("EXCHANGE: Coinbase Pro")
    
    # requesting available currency pairs from the Coinbase Pro API
    response = requests.get('https://api.pro.coinbase.com/products/').json()
    
    # turning the response into a list of available currency pairs
    coinbase_pro_pairs = [symbol['id'] for symbol in response]
    
    # checking that list of desired trading_pairs against the list of currency
    # pairs available on Coinbase Pro; underscores need to be replaced with
    # hyphens and symbols need to be capitalized to make that comparison.
    coinbase_pro_pairs = [pair for pair in trading_pairs if pair.replace(
        '_', '-').upper() in coinbase_pro_pairs]
    
    # limit to the number of candles returned by the Coinbase Pro API with 
    # each API request; 300 is the maximum.
    limit = 300
    
    # looping through candle intervals
    for period in periods:
        
        # printing candle interval to track progress
        print("CANDLE SIZE:", period)
        
        # formatting the interval as a string for use later in function
        interval = str(period)
        
        # looping through trading pairs
        for trading_pair in coinbase_pro_pairs:
            
            # printing trading pair to track progress
            print("TRADING PAIR:", trading_pair)
            
            # reformatting the trading pair for use with Coinbase Pro API
            clean_trading_pair = trading_pair.replace('_', '-').upper()
            
            # name and order of features returned by Coinbase Pro API
            old_columns = ['timestamp', 'low', 'high', 'open', 'close',
                           'volume']
            
            # creating an empty dataframe to populate with candles
            df = pd.DataFrame(columns=old_columns)
            
            # number of iterations it will take to grab all candles of the
            # specififed interval in the specified historical window, given
            # the limit to the number of candles returned by Coinbase Pro API
            # with each request; this is equivalent to ceiling dividing the
            # number of seconds in the historical window by the number of
            # seconds covered by each API request
            total_iterations = ((ending-starting)+(limit*period)
                               )//(limit*period)
            
            # completed_iterations will be updated with each iteration
            completed_iterations = 0
            
            # cutoff_time will be updated with each iteration as its value 
            # is always that of the most recent timestamp covered by our 
            # current API request; our function begins by grabbing the most
            # recent data in our specified historical window and moves 
            # backwards in time, so cutoff_time is initially set to the end of
            # our specified historical window and its value decreses as we
            # make requests for earlier data
            cutoff_time = ending
            
            # i.e., while we're still requesting data within our specified
            # historical window...
            while cutoff_time > (starting):
                
                try:
                    
                    # timestamp for earliest candle covered by our current
                    # API request; if we were requesting two one hour candles,
                    # the timestamp for the earlier of the two candles would
                    # be one hour before the timestamp for the later of the 
                    # two candles, and similarly if we are requesting 300 one
                    # hour candles, the start time will be 299 hours before
                    # the cutoff time
                    start = cutoff_time - ((limit-1)*period)
                    
                    # converting Unix time to ISO 8601 for Coinbase Pro API
                    start_iso_8601 = DT.datetime.utcfromtimestamp(start)
                    cutoff_time_iso_8601 = DT.datetime.utcfromtimestamp(
                        cutoff_time)
                    
                    # constructing url for our API request
                    url = ('https://api.pro.coinbase.com/products/'
                           '{trading_pair}/candles/?granularity={interval}'
                           '&start={start}&end={cutoff_time}'.format(
                               trading_pair=clean_trading_pair,
                               interval=interval, 
                               cutoff_time=cutoff_time_iso_8601,
                               start=start_iso_8601))
                    
                    # making API request
                    response = requests.get(url).json()
                    
                    # creating a pandas dataframe from the response
                    to_append = pd.DataFrame(response, columns=old_columns)
                    
                    # appending the dataframe just created from the most 
                    # recent response to the dataframe that will contain all 
                    # of the candles within our specified historical window
                    df = df.append(to_append).reset_index(drop=True)
                    
                    # updating the cutoff time for the next API request; if we
                    # are requesting 300 one hour candles, we want the cutoff
                    # time to be 300 hours earlier next time around
                    cutoff_time -= (limit*period)
                    
                    # updating the number of completed iterations
                    completed_iterations += 1
                    
                    # tracking progress by printing the number of API requests
                    # that still need to be made to generate the next csv
                    print("ITERATIONS REMAINING:", 
                          (total_iterations-completed_iterations))
                    
                    # sleeping so as not to hit a too many requests limit
                    time.sleep(.75)

                except:
                    pass
                
            # if we grabbed the same candle twice, we want to drop that
            df = df.drop_duplicates(subset='timestamp')
            
            # sorting the candles from latest to earliest
            df = df.sort_values(by='timestamp', ascending=False
                               ).reset_index(drop=True)
            
            # the Coinbase Pro API uses the time a candle opens for its
            # timestamps; to maintain consistency with Cryptowatch, we want 
            # the timestamp to be the closing time; converting accordingly
            df['timestamp'] = ((pd.to_datetime(df['timestamp']).values.astype(
                int)) + period).astype(int)
            
            # our csv will have the following columns in the following order
            new_columns = ['closing_time', 'open', 'high', 'low', 'close',
                           'base_volume']
            
            # reformatting csv to have desired columns in desired order
            df[new_columns] = df[['timestamp', 'open', 'high', 'low', 'close', 
                                 'volume']]
            
            # and making sure it doesn't have any extraneous columns
            df = df[new_columns]

            # exporting the dataframe to a csv
            df.to_csv('coinbase_pro/coinbase_pro_' + trading_pair + '_' +
                      str(period) + '.csv')
            
            # printing the dataframe to track progress
            print(df)

# function for grabbing historical candles from bitfinex
def get_bitfinex_candles(trading_pairs=trading_pairs,
                         periods=periods,
                         starting=start_of_2015,
                         ending=start_of_oct_2019):
    """This function gets historical candle data from the Bitfinex API.
    The trading_pairs parameter takes a list of trading pairs, where the 
    currencies in the pair are separated by an underscore and each currency is
    represented by its lowercase symbol on cryptowatch. The periods parameter
    takes a list of candle intervals in seconds. The starting parameter takes
    a Unix timestamp for the start of the historical window. The ending 
    parameter takes a Unix timestamp for the end of the historical window. 
    The function creates a csv for each trading pair in the trading_pairs list
    for each candle interval in the periods list. The csvs contain all of the
    available candles from the time specified by the starting parameter to the
    time specified by the ending parameter. The csvs are added to the 
    bitfinex folder in the repository. The filenames take the format 
    exchange_pair_interval.csv; bitfinex_btc_usd_3600.csv contains one hour
    (3600 seconds) candles for the BTC/USD trading pair from Bitfinex. The
    candles consist of six values: closing_time, open, high, low, close,
    base_volume. The closing time is given as a Unix timestamp."""
    
    # printing the name of the exchange to track progress in case this
    # function is run alongside similar functions for other exchanges
    print("EXCHANGE: Bitfinex")
    
    # requesting available currency pairs from the Bitfinex API
    response = requests.get('https://api-pub.bitfinex.com/v2/tickers'
                            '?symbols=ALL').json()
    
    # turning the response into a list of available currency pairs
    bitfinex_pairs = [symbol[0] for symbol in response]
    
    # checking that list of desired trading_pairs against the list of currency
    # pairs available on Bitfinex; underscores need to be replaced with
    # hyphens and symbols need to be capitalized to make that comparison;
    # further, Bitfinex abbreviates USD Coin as 'udc' rather than 'usdc',
    # USD Tether as 'ust' rather than 'usdt', Dash as 'dsh' rather than
    # 'dash', and Bitcoin Cash as 'bab' rather than 'bch', and this needs to
    # be accounted for when making the comparison between the lists
    bitfinex_pairs = [pair for pair in trading_pairs if ('t' + pair.replace(
        '_', '').replace('usdc', 'udc').replace('usdt', 'ust').replace(
        'dash', 'dsh').replace('bch', 'bab').upper()) in bitfinex_pairs]
    
    # limit to the number of candles returned by the Bitfinex API with each 
    # API request; 5000 is the maximum.
    limit = 5000

    # looping through candle intervals
    for period in periods:
        
        # printing candle interval to track progress
        print("CANDLE SIZE:", period)
        
        # since Bitfinex API takes string representations of candle intervals
        if period == (60*60):
            interval = '1h'
        elif period == (5*60):
            interval = '5m'
            
        # looping through trading pairs
        for trading_pair in bitfinex_pairs:
            
            # printing trading pair to track progress
            print("TRADING PAIR:", trading_pair)
            
            # reformatting the trading pair for use with Bitfinex API; again,
            # Bitfinex abbreviates USD Coin as 'udc' rather than 'usdc',
            # USD Tether as 'ust' rather than 'usdt', Dash as 'dsh' rather 
            # than 'dash', and Bitcoin Cash as 'bab' rather than 'bch'
            clean_trading_pair = trading_pair.replace('_', '').replace(
                'usdc', 'udc').replace('usdt', 'ust').replace(
                'dash', 'dsh').replace('bch', 'bab').upper()
            
            # name and order of features returned by Bitfinex API
            old_columns = ['timestamp', 'open', 'close', 'high', 'low',
                           'volume']
            
            # creating an empty dataframe to populate with candles
            df = pd.DataFrame(columns=old_columns)
            
            # number of iterations it will take to grab all candles of the
            # specififed interval in the specified historical window, given
            # the limit to the number of candles returned by the Bitfinex API
            # with each request; this is equivalent to ceiling dividing the
            # number of seconds in the historical window by the number of
            # seconds covered by each API request
            total_iterations = ((ending-starting)+(limit*period)
                               )//(limit*period)
            
            # completed_iterations will be updated with each iteration
            completed_iterations = 0
            
            # cutoff_time will be updated with each iteration as its value 
            # is always that of the most recent timestamp covered by our 
            # current API request; our function begins by grabbing the most
            # recent data in our specified historical window and moves 
            # backwards in time, so cutoff_time is initially set to the end of
            # our specified historical window and its value decreses as we
            # make requests for earlier data. Bitfinex API uses Unix time in
            # milliseconds rather than seconds, so any time given in seconds
            # needs to be multiplied by 1000
            cutoff_time = ending*1000
            
            # i.e., while we're still requesting data within our specified
            # historical window...
            while cutoff_time > (starting*1000):
                
                try:
                    
                    # constructing url for our API request
                    url = ('https://api.bitfinex.com/v2/candles/'
                           'trade:{interval}:t{trading_pair}/hist?end='
                           '{cutoff_time}&limit={limit}'.format(
                               trading_pair=clean_trading_pair,
                               interval=interval, 
                               cutoff_time=cutoff_time,
                               limit=limit))
                    
                    # making API request
                    response = requests.get(url).json()
                    
                    # creating a pandas dataframe from the response
                    to_append = pd.DataFrame(response, columns=old_columns)
                    
                    # appending the dataframe just created from the most 
                    # recent response to the dataframe that will contain all 
                    # of the candles within our specified historical window
                    df = df.append(to_append).reset_index(drop=True)
                    
                    # updating the cutoff time for the next API request; if we
                    # are requesting 5000 one hour candles, we want the cutoff
                    # time to be 5000 hours earlier next time around; again,
                    # Bitfinex API uses Unix time in milliseconds rather than 
                    # seconds, so any time given in seconds needs to be 
                    # multiplied by 1000
                    cutoff_time -= (limit*period*1000)
                    
                    # updating the number of completed iterations
                    completed_iterations += 1
                    
                    # tracking progress by printing the number of API requests
                    # that still need to be made to generate the next csv
                    print("ITERATIONS REMAINING:", 
                          (total_iterations-completed_iterations))
                    
                    # sleeping so as not to hit a too many requests limit
                    time.sleep(1.25)

                except:
                    pass

            # if we grabbed the same candle twice, we want to drop that
            df = df.drop_duplicates(subset='timestamp')
            
            # sorting the candles from latest to earliest
            df = df.sort_values(by='timestamp', ascending=False
                               ).reset_index(drop=True)

            # dividing timestamps by 1000 to get Unix time in seconds rather
            # than milliseconds; further, the Bitfinex API uses the time a 
            # candle opens for its timestamps so to maintain consistency with
            # Cryptowatch, we want the timestamp to be the closing time
            df['timestamp'] = ((pd.to_datetime(df['timestamp']).values.astype(
                int)/1000) + period).astype(int)

            # our csv will have the following columns in the following order
            new_columns = ['closing_time', 'open', 'high', 'low', 'close',
                           'base_volume']
            
            # reformatting csv to have desired columns in desired order
            df[new_columns] = df[['timestamp', 'open', 'high', 'low', 'close', 
                                 'volume']]
            
            # and making sure it doesn't have any extraneous columns
            df = df[new_columns]

            # exporting the dataframe to a csv
            df.to_csv('bitfinex/bitfinex_' + trading_pair + '_' + 
                      str(period) + '.csv')

            # printing the dataframe to track progress
            print(df)

# function for grabbing historical candles from hitbtc
def get_hitbtc_candles(trading_pairs=trading_pairs,
                       periods=periods,
                       starting=start_of_2015,
                       ending=start_of_oct_2019):
    """This function gets historical candle data from the HitBTC API.
    The trading_pairs parameter takes a list of trading pairs, where the 
    currencies in the pair are separated by an underscore and each currency is
    represented by its lowercase symbol on cryptowatch. The periods parameter
    takes a list of candle intervals in seconds. The starting parameter takes
    a Unix timestamp for the start of the historical window. The ending 
    parameter takes a Unix timestamp for the end of the historical window. 
    The function creates a csv for each trading pair in the trading_pairs list
    for each candle interval in the periods list. The csvs contain all of the
    available candles from the time specified by the starting parameter to the
    time specified by the ending parameter. The csvs are added to the 
    hitbtc folder in the repository. The filenames take the format 
    exchange_pair_interval.csv; hitbtc_btc_usd_3600.csv contains one hour
    (3600 seconds) candles for the BTC/USD trading pair from HitBTC. The
    candles consist of six values: closing_time, open, high, low, close,
    base_volume. The closing time is given as a Unix timestamp."""
    
    # printing the name of the exchange to track progress in case this
    # function is run alongside similar functions for other exchanges
    print("EXCHANGE: HitBTC")
    
    # requesting available currency pairs from the HitBTC API
    response = requests.get('https://api.hitbtc.com/api/2/public/symbol'
                           ).json()
    
    # turning the response into a list of available currency pairs
    hitbtc_pairs = [symbol['id'] for symbol in response]
    
    # checking that list of desired trading_pairs against the list of currency
    # pairs available on HitBTC; underscores need to be replaced with hyphens 
    # and symbols need to be capitalized to make that comparison.
    hitbtc_pairs = [pair for pair in trading_pairs if pair.replace(
        '_', '').upper() in hitbtc_pairs]
    
    # limit to the number of candles returned by the HitBTC API with each API
    # request; 1000 is the maximum.
    limit = 1000

    # looping through candle intervals
    for period in periods:
        
        # printing candle interval to track progress
        print("CANDLE SIZE:", period)
        
        # since HitBTC API takes string representations of candle intervals
        if period == (60*60):
            interval = 'H1'
        elif period == (5*60):
            interval = 'M5'
            
        # looping through trading pairs
        for trading_pair in hitbtc_pairs:
            
            # printing trading pair to track progress
            print("TRADING PAIR:", trading_pair)
            
            # reformatting the trading pair for use with HitBTC API
            clean_trading_pair = trading_pair.replace('_', '').upper()
            
            # creating an empty dataframe to populate with candles
            df = pd.DataFrame()
            
            # number of iterations it will take to grab all candles of the
            # specififed interval in the specified historical window, given
            # the limit to the number of candles returned by the HitBTC API
            # with each request; this is equivalent to ceiling dividing the
            # number of seconds in the historical window by the number of
            # seconds covered by each API request
            total_iterations = ((ending-starting)+(limit*period)
                               )//(limit*period)
            
            # completed_iterations will be updated with each iteration
            completed_iterations = 0
            
            # cutoff_time will be updated with each iteration as its value 
            # is always that of the most recent timestamp covered by our 
            # current API request; our function begins by grabbing the most
            # recent data in our specified historical window and moves 
            # backwards in time, so cutoff_time is initially set to the end of
            # our specified historical window and its value decreses as we
            # make requests for earlier data
            cutoff_time = ending - ((limit-1)*period)
            
            # i.e., while we're still requesting data within our specified
            # historical window...
            while cutoff_time > (starting - (limit*period)):
                
                try:
                    
                    # constructing url for our API request
                    url = ('https://api.hitbtc.com/api/2/public/candles/'
                           '{trading_pair}?period={interval}&from='
                           '{cutoff_time}&limit={limit}'.format(
                               trading_pair=clean_trading_pair,
                               interval=interval, 
                               cutoff_time=cutoff_time,
                               limit=limit))
                    
                    # making API request
                    response = requests.get(url).json()
                    
                    # HitBTC returns earliest candles first, we want reverse
                    # since we are moving backwards in time
                    response.reverse()
                    
                    # appending the response to the dataframe that will 
                    # contain all of the candles within our specified 
                    # historical window
                    df = df.append(response).reset_index(drop=True)
                    
                    # updating the cutoff time for the next API request; if we
                    # are requesting 1000 one hour candles, we want the cutoff
                    # time to be 1000 hours earlier next time around
                    cutoff_time -= (limit*period)
                    
                    # updating the number of completed iterations
                    completed_iterations += 1
                    
                    # tracking progress by printing the number of API requests
                    # that still need to be made to generate the next csv
                    print("ITERATIONS REMAINING:", 
                          (total_iterations-completed_iterations))

                except:
                    pass

            # if we grabbed the same candle twice, we want to drop that
            df = df.drop_duplicates(subset='timestamp')
            
            # sorting the candles from latest to earliest
            df = df.sort_values(by='timestamp', ascending=False
                               ).reset_index(drop=True)

            # dividing timestamps by one billion to get Unix time in seconds 
            # rather than nanoseconds; further, the HitBTC API uses the time a 
            # candle opens for its timestamps so to maintain consistency with
            # Cryptowatch, we want the timestamp to be the closing time
            df['timestamp'] = ((pd.to_datetime(df['timestamp']).values.astype(
                int)/1000000000) + period).astype(int)

            # names and order of columns in dataframe inherited from HitBTC
            old_columns = ['timestamp', 'open', 'max', 'min', 'close',
                           'volume']
            
            # our csv will have the following columns in the following order
            new_columns = ['closing_time', 'open', 'high', 'low', 'close',
                           'base_volume']
            
            # reformatting csv to have desired columns in desired order
            df[new_columns] = df[old_columns]
            
            # and making sure it doesn't have any extraneous columns
            df = df[new_columns]

            # the HitBTC API sometimes uses 'usd' as a symbol for USD Tether,
            # and sometimes uses usdt as a symbol for USD Tether; 'usd' is 
            # never being used to symbolize actual USD; we want to clean this
            # up before we name our csvs...
            if trading_pair.endswith('usd'):
                trading_pair = trading_pair.replace('usd', 'usdt')

            # exporting the dataframe to a csv; using default index=True with
            # pandas.DataFrame.to_csv function, but if index not wanted when 
            # using pandas.read_csv to import the csv, add index_col=0 to 
            # ignore it.
            df.to_csv('hitbtc/hitbtc_' + trading_pair + '_' + str(period) + 
                      '.csv')

            # printing the dataframe to track progress
            print(df)

# start of June 2019 in Unix time; earlier Cryptowatch data not available
start_of_jun_2019 = 1559347200

# the present time; we will gather Cryptowatch data until the present
now = round(time.time())

# insert Cryptowatch API key here
api_key = 'INSERT_API_KEY_HERE'

# function for grabbing Kraken and Gemini historical candles from Cryptowatch
def get_cryptowatch_candles(exchanges=['kraken', 'gemini'],
                            trading_pairs=trading_pairs,
                            periods=periods,
                            starting=start_of_jun_2019,
                            ending=now):
    """This function gets historical candle data from the Cryptowatch API.
    The trading_pairs parameter takes a list of trading pairs, where the 
    currencies in the pair are separated by an underscore and each currency is
    represented by its lowercase symbol on cryptowatch. The periods parameter
    takes a list of candle intervals in seconds. The starting parameter takes
    a Unix timestamp for the start of the historical window. The ending 
    parameter takes a Unix timestamp for the end of the historical window. 
    The function creates a csv for each trading pair in the trading_pairs list
    for each candle interval in the periods list. The csvs contain all of the
    available candles from the time specified by the starting parameter to the
    time specified by the ending parameter. The csvs are added to the 
    kraken and gemini folders in the repository. The filenames take the format 
    exchange_pair_interval.csv; kraken_btc_usd_3600.csv contains one hour
    (3600 seconds) candles for the BTC/USD trading pair from Kraken. The
    candles consist of six values: closing_time, open, high, low, close,
    base_volume. The closing time is given as a Unix timestamp."""
    
    # looping through kraken and gemini...
    for exchange in exchanges:

        # printing exchange to track progress...
        print("EXCHANGE", exchange)
    
        # requesting available currency pairs from the Cryptowatch API
        response = requests.get('https://api.cryptowat.ch/markets/{exchange}'
                                '?apikey={api_key}'
                                .format(exchange=exchange, api_key=api_key)
                               ).json()

        # turning the response into a list of available currency pairs
        exchange_pairs = [result['pair'] for result in response['result']]

        # checking that list of desired trading_pairs against the list of
        # pcurrency airs available on Cryptowatch; underscores need to be 
        # dropped to make that comparison.
        exchange_pairs = [pair for pair in trading_pairs if pair.replace(
            '_', '') in exchange_pairs]

        # limit to the number of candles returned by the Cryptowatch API with
        # each API request; 500 is the maximum.
        limit = 500

        # looping through candle intervals
        for period in periods:

            # printing candle interval to track progress
            print("CANDLE SIZE:", period)

            # since Cryptowatch API takes candle intervals as strings
            if period == (60*60):
                interval = '3600'
            elif period == (5*60):
                interval = '300'

            # looping through trading pairs
            for trading_pair in exchange_pairs:

                # printing trading pair to track progress
                print("TRADING PAIR:", trading_pair)

                # reformatting the trading pair for use with Cryptowatch API
                cleaned_trading_pair = trading_pair.replace('_', '')

                # our csv will have the following columns in the following order
                columns = ['closing_time', 'open', 'high', 'low', 'close', 
                           'base_volume']

                # creating an empty dataframe to populate with candles
                df = pd.DataFrame(columns=columns)

                # number of iterations it will take to grab all candles of the
                # specififed interval in the specified historical window, given
                # the limit to the number of candles returned by the Cryptowatch 
                # API with each request; this is equivalent to ceiling dividing
                # the number of seconds in the historical window by the number
                # of seconds covered by each API request
                total_iterations = ((ending-starting)+(limit*period)
                                   )//(limit*period)

                # completed_iterations will be updated with each iteration
                completed_iterations = 0

                # cutoff_time will be updated with each iteration as its value 
                # is always that of the most recent timestamp covered by our 
                # current API request; our function begins by grabbing the most
                # recent data in our specified historical window and moves 
                # backwards in time, so cutoff_time is initially set to the end of
                # our specified historical window and its value decreses as we
                # make requests for earlier data
                cutoff_time = ending - ((limit-1)*period)
                
                # i.e., while we're still requesting data within our specified
                # historical window...
                while cutoff_time > (starting - (limit*period)):

                    try:

                        # constructing url for our API request
                        url = ('https://api.cryptowat.ch/markets/{exchange}/'
                               '{trading_pair}/ohlc?apikey={api_key}&'
                               'periods={period}&after={after}'.format(
                                   exchange=exchange, 
                                   trading_pair=cleaned_trading_pair,
                                   api_key=api_key,
                                   period=period,
                                   after=cutoff_time))
                        
                        # making API request
                        response = requests.get(url).json()
                        
                        # turning the response into a list of candles
                        candles = response['result'][interval]

                        # isolating the candle data we need for each candle
                        candles = [candle[:6] for candle in candles]

                        # reversing order so earlier candles are first
                        candles.reverse()
                        
                        # creating a pandas dataframe from the candles
                        to_append = pd.DataFrame(candles, columns=columns)

                        # appending the dataframe just created from the most 
                        # recent response to the dataframe that will contain all 
                        # of the candles within our specified historical window
                        df = df.append(to_append).reset_index(drop=True)

                        # updating the cutoff time for the next API request; if we
                        # are requesting 500 one hour candles, we want the cutoff
                        # time to be 500 hours earlier next time around
                        cutoff_time -= (limit*period)

                        # updating the number of completed iterations
                        completed_iterations += 1

                        # tracking progress by printing the number of API requests
                        # that still need to be made to generate the next csv
                        print("ITERATIONS REMAINING:", 
                              (total_iterations-completed_iterations))

                    except:
                        pass

                # if we grabbed the same candle twice, we want to drop that
                df = df.drop_duplicates(subset='closing_time')

                # sorting the candles from latest to earliest
                df = df.sort_values(by='closing_time').reset_index(drop=True)

                # exporting the dataframe to a csv
                df.to_csv(exchange + '/' + exchange + '_' + trading_pair + 
                          '_' + str(period) + '.csv')

                # printing the dataframe to track progress
                print(df)
            
# calling all four functions to get the historical candles we want; since 
# the Bitfinex API returns 5000 candles with each request, the 
# get_bitfinex_candles function completes its task more quickly than the
# get_hitbtc_candles function (the HitBTC API returns 1000 candles with
# each request), or the get_coinbase_pro_candles function (the Coinbase Pro 
# API only returns 300 candles with each request)...
get_bitfinex_candles()
get_hitbtc_candles()
get_coinbase_pro_candles()
get_cryptowatch_candles()