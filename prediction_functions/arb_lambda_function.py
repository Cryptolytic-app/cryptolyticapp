###########################################################
########### ARBITRAGE FORCASTER LAMBDA FUNCTION ############
###########################################################

# This lambda function is created for AWS Lambda and runs in a cloud9 environment
# It generates predictions with all arbitrage models from an S3 bucket and inserts
# them into the arp table under the prediction schema in the database.
# The function is set to run every 3 mins on a trigger.

# imports
import pandas as pd
import pickle
from ta import add_all_ta_features
import datetime as dt
import numpy as np
import psycopg2 as ps
import pandas as pd
import boto3
import warnings
warnings.filterwarnings("ignore")


# have to input models manually based off the best models selected from
# arbitrage_models.ipynb
model_list = ['bitfinex_coinbase_pro_bch_btc', 'coinbase_pro_hitbtc_bch_btc',
       'bitfinex_coinbase_pro_ltc_usd', 'bitfinex_coinbase_pro_etc_usd',
       'bitfinex_hitbtc_eth_btc', 'bitfinex_coinbase_pro_eth_btc',
       'gemini_hitbtc_bch_btc', 'coinbase_pro_gemini_bch_btc',
       'coinbase_pro_hitbtc_eth_usdc', 'bitfinex_gemini_bch_btc',
       'bitfinex_hitbtc_bch_usdt', 'bitfinex_hitbtc_ltc_btc',
       'coinbase_pro_gemini_ltc_btc', 'bitfinex_hitbtc_eos_usdt',
       'gemini_hitbtc_ltc_btc', 'bitfinex_hitbtc_ltc_usdt',
       'kraken_gemini_ltc_btc', 'bitfinex_coinbase_pro_bch_usd',
       'bitfinex_gemini_ltc_btc', 'bitfinex_coinbase_pro_ltc_btc',
       'kraken_gemini_bch_btc']

# # decrypt credentials
# POSTGRES_ADDRESS = os.environ['POSTGRES_ADDRESS']
# POSTGRES_PORT = os.environ['POSTGRES_PORT']
# POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
# POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
# POSTGRES_DBNAME = os.environ['POSTGRES_DBNAME']

# fill in credentials
credentials = {'POSTGRES_ADDRESS': POSTGRES_ADDRESS,
               'POSTGRES_PORT': POSTGRES_PORT,
               'POSTGRES_USERNAME': POSTGRES_USERNAME,
               'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
               'POSTGRES_DBNAME': POSTGRES_DBNAME
               }


def create_conn(credentials):
    """Creates database connection and returns connection and cursor"""

    # create connection to postgres DB
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    # create cursor
    cursor = conn.cursor()

    return conn, cursor

def retrieve_data(cursor, table_name, schema='fiveminute'):
    """Retrieves data from a database where a connection is already established.
        Returns the last 100 rows from the table selected."""

    # Get the last 100 rows from the DB
    cursor.execute("""SELECT * FROM {schema}.{table_name} 
                      ORDER BY closing_time DESC
                      LIMIT 100""".format(table_name=table_name, schema=schema))

    # Reverse the order of the list so time is ascending
    result = cursor.fetchall()[::-1]

    # Create dataframe with data and rename columns
    result = pd.DataFrame(result)
    result = result.rename(columns={0: 'closing_time', 1: 'open', 2: 'high',
                                    3: 'low', 4: 'close', 5: 'base_volume'})

    return result

def get_exchange_trading_pair(table_name):
    """Function to get exchange and trading_pair bc coinbase_pro has an extra '_' """

    # for coinbase_pro
    if len(table_name.split('_')) == 4:
        exchange = table_name.split('_')[0] + '_' + table_name.split('_')[1]
        trading_pair = table_name.split('_')[2] + '_' + table_name.split('_')[3]

    # for all other exchanges
    else:
        exchange = table_name.split('_')[0]
        trading_pair = table_name.split('_')[1] + '_' + table_name.split('_')[2]

    return exchange, trading_pair

# this function is not used in this lambda function but is a way of
# generating combinations for arbitrage given the table_names and exchanges
def get_table_pairs(table_names, exchanges):
    """Takes in a list of table names and exchanges and creates all possible
        matches of trading pairs between the exchanges and returns those tables
        as a list of lists"""

    # list of table pairs
    table_pairs = []

    # iterates through table names and creates a list of matches
    for table_name_1 in table_names:
        remaining_filenames = table_names[table_names.index(table_name_1) + 1:]
        for table_name_2 in remaining_filenames:
            for exchange in exchanges:
                if table_name_1.replace(exchange, '') in table_name_2:
                    table_pairs.append([table_name_1, table_name_2])

    # new list to remove any mismatched trading pairs with usd/usdt
    table_pairs2 = []
    for table_pair in table_pairs:
        exchange_1, trading_pair_1 = get_exchange_trading_pair(table_pair[0])
        exchange_2, trading_pair_2 = get_exchange_trading_pair(table_pair[1])
        if trading_pair_1 == trading_pair_2:
            if exchange_1 != exchange_2:
                table_pairs2.append(table_pair)

    # return table_pairs, table_pairs2, len(table_pairs), len(table_pairs2)
    return table_pairs2

# print(get_table_pairs(table_names, exchanges))

#############################################################################
################## functions to engineer features pre-merge #################
#############################################################################
def np64_to_utc(np64):
    """ converts a numpy64 time to datetime """

    dt64 = np.datetime64(np64)
    unix_epoch = np.datetime64(0, 's')
    one_second = np.timedelta64(1, 's')
    seconds_since_epoch = (dt64 - unix_epoch) / one_second

    return dt.datetime.utcfromtimestamp(seconds_since_epoch)

def resample_ohlcv(df, period='5T'):
    """this function resamples ohlcv csvs for a specified candle interval; while
        this can be used to change the candle interval for the data, it can also be
        used to fill in gaps in the ohlcv data without changing the candle interval"""

    # set the date as the index; this is needed for the function to run
    df = df.set_index(['date'])

    # dictionary specifying which columns to use for resampling
    ohlc_dict = {'open':'first',
                 'high':'max',
                 'low':'min',
                 'close': 'last',
                 'base_volume': 'sum'}

    # apply resampling
    df = df.resample(period, how=ohlc_dict, closed='left', label='left')

    return df

# function to handle nans in the data introduced by resampling
def fill_nan(df):
    """Iterates through a dataframe and fills NaNs with appropriate open,
        high, low, close values."""

    # forward filling the closing price where there were gaps in ohlcv csv
    df['close'] = df['close'].ffill()

    # backfilling the rest of the nans
    df = df.bfill(axis=1)

    return df

def engineer_features(df, period='5T'):
    """Takes a df, engineers ta features, and returns a df
       default period=['5T']"""

    # convert unix closing_time to datetime
    df['date'] = pd.to_datetime(df['closing_time'], unit='s')

    # time resampling to fill gaps in data
    df = resample_ohlcv(df, period)

    # move date off the index
    df = df.reset_index()

    # create closing_time
    closing_time = df.date.values
    df.drop(columns='date', inplace=True)

    # create feature to indicate where rows were gaps in data
    df['nan_ohlcv'] = df['close'].apply(lambda x: 1 if pd.isnull(x) else 0)

    # fill gaps in data
    df = fill_nan(df)

    # adding all the technical analysis features...
    df = add_all_ta_features(df, 'open', 'high', 'low', 'close', 'base_volume', fillna=True)

    # add closing time column
    df['closing_time'] = closing_time

    return df


#############################################################################
#### the following functions are used in engineering features post-merge ####
#############################################################################

def get_higher_closing_price(df):
    """returns the exchange with the higher closing price"""

    # exchange 1 has higher closing price
    if (df['close_exchange_1'] - df['close_exchange_2']) > 0:
        return 1

    # exchange 2 has higher closing price
    elif (df['close_exchange_1'] - df['close_exchange_2']) < 0:
        return 2

    # closing prices are equivalent
    else:
        return 0


def get_pct_higher(df):
    """returns the percentage of the difference between ex1/ex2
        closing prices"""

    # if exchange 1 has a higher closing price than exchange 2
    if df['higher_closing_price'] == 1:

        # % difference
        return ((df['close_exchange_1'] /
                 df['close_exchange_2']) - 1) * 100

    # if exchange 2 has a higher closing price than exchange 1
    elif df['higher_closing_price'] == 2:

        # % difference
        return ((df['close_exchange_2'] /
                 df['close_exchange_1']) - 1) * 100

    # closing prices are equivalent
    else:
        return 0


def get_arbitrage_opportunity(df):
    """function to create column showing available arbitrage opportunities"""

    # assuming the total fees are 0.55%, if the higher closing price is less
    # than 0.55% higher than the lower closing price...
    if df['pct_higher'] < .55:
        return 0  # no arbitrage

    # if exchange 1 closing price is more than 0.55% higher
    # than the exchange 2 closing price
    elif df['higher_closing_price'] == 1:
        return -1  # arbitrage from exchange 2 to exchange 1

    # if exchange 2 closing price is more than 0.55% higher
    # than the exchange 1 closing price
    elif df['higher_closing_price'] == 2:
        return 1  # arbitrage from exchange 1 to exchange 2


def get_window_length(df):
    """function to create column showing how long arbitrage opportunity has lasted"""

    # convert arbitrage_opportunity column to a list
    target_list = df['arbitrage_opportunity'].to_list()

    # set initial window length
    window_length = 5  # time in minutes

    # list for window_lengths
    window_lengths = []

    # iterate through arbitrage_opportunity column
    for i in range(len(target_list)):

        # check if a value in the arbitrage_opportunity column is equal to the
        # previous value in the arbitrage_opportunity column and increase
        # window length
        if target_list[i] == target_list[i - 1]:
            window_length += 5
            window_lengths.append(window_length)

        # if a value in the arbitrage_opportunity column is
        # not equal to the previous value in the arbitrage_opportunity column
        # reset the window length to five minutes
        else:
            window_length = 5
            window_lengths.append(window_length)

    # create window length column showing how long an arbitrage opportunity has lasted
    df['window_length'] = window_lengths

    return df

def merge_dfs(df1, df2):
    """function to merge dataframes and create final features for arbitrage data.
        Returns a dataframe and the prediction time"""

    # merging two modified ohlcv dfs on closing time to create arbitrage df
    df = pd.merge(df1, df2, on='closing_time',
                  suffixes=('_exchange_1', '_exchange_2'))

    # check if the df merged successfully - shape should not be 0
    print('after merge:', df.shape)

    # convert closing_time to datetime
    df['closing_time'] = pd.to_datetime(df['closing_time'])
    print('closing time added')

    # Create additional date features.
    df['year'] = df['closing_time'].dt.year
    df['month'] = df['closing_time'].dt.month
    df['day'] = df['closing_time'].dt.day
    print('date feat added')

    # get time of prediction
    prediction_time = df.closing_time.values[-1] # -1 for most recent row
    prediction_time = np64_to_utc(str(prediction_time))
    print(prediction_time)

    # getting higher_closing_price feature to create pct_higher feature
    df['higher_closing_price'] = df.apply(get_higher_closing_price, axis=1)
    print('higher_closing_price added')

    # getting pct_higher feature to create arbitrage_opportunity feature
    df['pct_higher'] = df.apply(get_pct_higher, axis=1)
    print('pct_higher added')

    # getting arbitrage_opportunity feature
    df['arbitrage_opportunity'] = df.apply(get_arbitrage_opportunity, axis=1)
    print('arbitrage_opportunity added')

    # getting window_length feature
    df = get_window_length(df)
    print('window length added')

    # keep only the last column
    df = df[-1:]

    # feature selection
    features = ['close_exchange_1','base_volume_exchange_1',
                    'nan_ohlcv_exchange_1','volume_adi_exchange_1', 'volume_obv_exchange_1',
                    'volume_cmf_exchange_1', 'volume_fi_exchange_1','volume_em_exchange_1',
                    'volume_vpt_exchange_1','volume_nvi_exchange_1', 'volatility_atr_exchange_1',
                    'volatility_bbhi_exchange_1','volatility_bbli_exchange_1',
                    'volatility_kchi_exchange_1', 'volatility_kcli_exchange_1',
                    'volatility_dchi_exchange_1','volatility_dcli_exchange_1',
                    'trend_macd_signal_exchange_1', 'trend_macd_diff_exchange_1', 'trend_adx_exchange_1',
                    'trend_adx_pos_exchange_1', 'trend_adx_neg_exchange_1',
                    'trend_vortex_ind_pos_exchange_1', 'trend_vortex_ind_neg_exchange_1',
                    'trend_vortex_diff_exchange_1', 'trend_trix_exchange_1',
                    'trend_mass_index_exchange_1', 'trend_cci_exchange_1',
                    'trend_dpo_exchange_1', 'trend_kst_sig_exchange_1',
                    'trend_kst_diff_exchange_1', 'trend_aroon_up_exchange_1',
                    'trend_aroon_down_exchange_1',
                    'trend_aroon_ind_exchange_1',
                    'momentum_rsi_exchange_1', 'momentum_mfi_exchange_1',
                    'momentum_tsi_exchange_1', 'momentum_uo_exchange_1',
                    'momentum_stoch_signal_exchange_1',
                    'momentum_wr_exchange_1', 'momentum_ao_exchange_1',
                    'others_dr_exchange_1', 'close_exchange_2',
                    'base_volume_exchange_2', 'nan_ohlcv_exchange_2',
                    'volume_adi_exchange_2', 'volume_obv_exchange_2',
                    'volume_cmf_exchange_2', 'volume_fi_exchange_2',
                    'volume_em_exchange_2', 'volume_vpt_exchange_2',
                    'volume_nvi_exchange_2', 'volatility_atr_exchange_2',
                    'volatility_bbhi_exchange_2',
                    'volatility_bbli_exchange_2',
                    'volatility_kchi_exchange_2',
                    'volatility_kcli_exchange_2',
                    'volatility_dchi_exchange_2',
                    'volatility_dcli_exchange_2',
                    'trend_macd_signal_exchange_2',
                    'trend_macd_diff_exchange_2', 'trend_adx_exchange_2',
                    'trend_adx_pos_exchange_2', 'trend_adx_neg_exchange_2',
                    'trend_vortex_ind_pos_exchange_2',
                    'trend_vortex_ind_neg_exchange_2',
                    'trend_vortex_diff_exchange_2', 'trend_trix_exchange_2',
                    'trend_mass_index_exchange_2', 'trend_cci_exchange_2',
                    'trend_dpo_exchange_2', 'trend_kst_sig_exchange_2',
                    'trend_kst_diff_exchange_2', 'trend_aroon_up_exchange_2',
                    'trend_aroon_down_exchange_2',
                    'trend_aroon_ind_exchange_2',
                    'momentum_rsi_exchange_2', 'momentum_mfi_exchange_2',
                    'momentum_tsi_exchange_2', 'momentum_uo_exchange_2',
                    'momentum_stoch_signal_exchange_2',
                    'momentum_wr_exchange_2', 'momentum_ao_exchange_2',
                    'others_dr_exchange_2', 'year', 'month', 'day',
                    'higher_closing_price', 'pct_higher',
                    'arbitrage_opportunity', 'window_length']
    df = df[features]
    print('features selected')

    return df, prediction_time

#############################################################################
##########################        END         ###############################
#############################################################################

def generate_predictions_arb(model_list):
    """ Takes in a list of models,  makes predictions using the correct
        arbitrage model, and inserts the prediction into the database"""

    # create connection to database
    conn, cursor = create_conn(credentials)
    print('connected to db :)')

    # # cnnect to S3 bucket
    # s3 = boto3.resource('s3')
    print('connected to S3')

    # # connect to S3 bucket locally to test
    # # make sure the bucket has public access
    # # all tr models should be in the bucket
    # Get access to our S3 service
    s3 = boto3.resource('s3',
                        aws_access_key_id='', # fill in access key
                        aws_secret_access_key='' # fill in secret)
    print('connected to S3')

    # fill in bucket name
    bucket = ''

    # list for table pairs
    table_pair_list = []

    # iterate through models
    for model in model_list:

        ############# define model name, exchanges, and trading pairs ##############
        # replace coinbase_pro bc string format is inconsistent for splitting patterns
        model = model.replace('coinbase_pro', 'cbpro')
        ex1, ex2 = model.split('_')[0], model.split('_')[1]
        trading_pair = model.split('_')[2] + '_' + model.split('_')[3]

        # put it back to coinbase_pro
        if ex1 == 'cbpro':
            ex1 = 'coinbase_pro'
        if ex2 == 'cbpro':
            ex2 = 'coinbase_pro'

        table_name_1 = ex1 + '_' + trading_pair
        table_name_2 = ex2 + '_' + trading_pair
        table_pair_list.append([table_name_1, table_name_2])
        print(ex1, ex2, trading_pair)
    print(table_pair_list)

    # iterate through all combinations of exchanges with a trading pair match
    for table_names in table_pair_list:
        print('---------------STARTING {table_names}-----------------'.format(table_names=table_names))

        # get exchanges and trading pairs
        exchange_1, trading_pair_1 = get_exchange_trading_pair(table_names[0])
        exchange_2, trading_pair_2 = get_exchange_trading_pair(table_names[1])
        print(trading_pair_1,trading_pair_2)
        assert trading_pair_1 == trading_pair_2
        print('exchange 1:', exchange_1)
        print('exchange 2:', exchange_2)


        # Fetch time with correct exchange/trading_pair
        # THIS IS ONLY GOING TO WORK IF WE DO IT IN THE SAME
        # ORDER ALL THE TIME WHICH WE SHOULD BE
        select_query = ("""SELECT p_time FROM prediction.arp
                            WHERE exchange_1 = '{exchange_1}'
                            AND exchange_2 = '{exchange_2}'
                            AND trading_pair = '{trading_pair_1}'
                            ORDER BY p_time desc LIMIT 100;""".format(exchange_1=exchange_1,
                                                                      exchange_2=exchange_2,
                                                                      trading_pair_1=trading_pair_1))
        cursor.execute(select_query)
        timestamps = cursor.fetchall()
        timestamps = [timestamp[0] for timestamp in timestamps]
        print('timestamps', timestamps)

        # retrieve data from databases
        schema = 'fiveminute'
        df1 = retrieve_data(cursor, table_names[0], schema)
        print('retrieved_data 1')
        df2 = retrieve_data(cursor, table_names[1], schema)
        print('retrieved_data 2')

        # get time of prediction
        time_list = [df1['closing_time'][-1:].values[0], df2['closing_time'][-1:].values[0]]
        time_min = np.min(time_list)
        time_min = pd.to_datetime(time_min, unit='s')
        time_min = np64_to_utc(str(time_min))
        print('time min', time_min)

        # Conditional to catch duplicates before inserting
        # NOTE - this won't get every duplicate
        if str(time_min) not in timestamps:

            try: # used to catch errors in case database is not fully up to date
                # feature engineering
                df1 = engineer_features(df1)
                df2 = engineer_features(df2)
                print('features engineered')
                # print(df1, df2)
                print(df1.shape, df2.shape)

                # merge dataframes
                df, prediction_time = merge_dfs(df1, df2)
                print('dataframes merged', df.shape)
                print('prediction time', prediction_time)

            except:
                prediction_time = 0
                print('error: df not merged')

            try: # used to catch errors if prediction time doesn't exist from prev error
                # filter duplicates again in case prediction_time is different from time_min
                if str(prediction_time) not in timestamps and prediction_time != 0:
                    # define model path
                    model_path = exchange_1 + '_' + table_names[1] + '.pkl'
                    print(model_path)

                    # Load Model from S3 Bucket
                    loaded_model = pickle.loads(s3.Bucket(bucket).Object(model_path).get()['Body'].read())
                    print('loaded model')

                    # make prediction
                    pred = loaded_model.predict(df)
                    print('prediction made:', pred)

                    # formatting for output
                    if pred[0] == -1:
                        pred = 'arb from exchange_2 to exchange_1'
                    elif pred[0] == 1:
                        pred = 'arb from exchange_1 to exchange_2'
                    else:
                        pred = 'no_arbitrage'

                    # create output
                    output = [str(prediction_time), str(dt.datetime.utcnow()), exchange_1, exchange_2, trading_pair_1, str(pred)]
                    print(output)

                    # insert output into database
                    insert_query = """INSERT INTO prediction.arp
                                    (p_time, c_time, exchange_1, exchange_2, trading_pair, prediction)
                                    VALUES (%s, %s, %s, %s, %s, %s)"""
                    cursor.execute(insert_query, output)
                    print('inserted into db!')

                else:
                    print('error: duplicates not inserted or df not merged')

            except:
                print('error with model prediction or inserting into db')

        else:
            print('duplicates not inserted')

    # commit and close connection
    # conn.commit()
    conn.close()

    return 'function complete!'

# for testing locally
print(generate_predictions_arb(model_list))

# def lambda_handler(event, context):
#     generate_predictions_arb(models)
#     return 'succesfully inserted data'