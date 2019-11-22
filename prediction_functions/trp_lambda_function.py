###########################################################
########### TRADE RECCOMMENDER LAMBDA FUNCTION ############
###########################################################

# This lambda function is created for AWS Lambda and runs in a cloud9 environment
# It generates predictions with all trade recommender models from an S3 bucket
# and inserts them into the trp table under the prediction schema in the database.
# The function is set to run every 10 mins on a trigger.

# imports
import pandas as pd
import pickle
from ta import add_all_ta_features
import datetime as dt
import numpy as np
import psycopg2 as ps
import boto3
import os

# exchanges and trading pairs
coinbase_pro_pairs = ['btc_usd', 'eth_usd', 'ltc_usd']
bitfinex_pairs = ['btc_usd', 'eth_usd', 'ltc_usd']
hitbtc_pairs = ['btc_usdt', 'eth_usdt', 'ltc_usdt']

bitfinex_table_list = ['bitfinex_' + pair for pair in bitfinex_pairs]
coinbase_pro_table_list = ['coinbase_pro_' + pair for pair in coinbase_pro_pairs]
hitbtc_table_list = ['hitbtc_' + pair for pair in hitbtc_pairs]

table_names = bitfinex_table_list + coinbase_pro_table_list + hitbtc_table_list

# have to manually get periods from tr modeling notebook since each model
# had a different period for best performance
models = ['hitbtc_eth_usdt', 'bitfinex_ltc_usd', 'bitfinex_btc_usd',
          'hitbtc_ltc_usdt', 'coinbase_pro_btc_usd', 'coinbase_pro_ltc_usd',
          'coinbase_pro_eth_usd', 'bitfinex_eth_usd', 'hitbtc_btc_usdt']

periods = ['1440T', '720T', '960T', '1440T', '1440T', '960T', '720T', '720T',
           '1440T']

model_periods = {model : period for model, period in zip(models,periods)}

# decrypt credentials - Add in credentials in AWS environment variable
# POSTGRES_ADDRESS = os.environ['POSTGRES_ADDRESS']
# POSTGRES_PORT = os.environ['POSTGRES_PORT']
# POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']
# POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
# POSTGRES_DBNAME = os.environ['POSTGRES_DBNAME']

# # fill in credentials
# credentials = {'POSTGRES_ADDRESS': POSTGRES_ADDRESS,
#                 'POSTGRES_PORT': POSTGRES_PORT,
#                 'POSTGRES_USERNAME': POSTGRES_USERNAME,
#                 'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
#                 'POSTGRES_DBNAME': POSTGRES_DBNAME
#                 }

def create_conn(credentials):
    """Creates database connection and returns connection and cursor"""

    # connect to database
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    cursor = conn.cursor()
    return conn, cursor


def retrieve_data(cursor, table_name, schema):
    """Retrieves data from a database where a connection is already established.
        Returns a df of the last 1000 rows from the table selected."""

    # Change limit number to whatever amount of rows you want to retrieve
    cursor.execute("""SELECT * FROM {schema}.{table_name} 
                      ORDER BY closing_time DESC 
                      LIMIT 1000""".format(table_name=table_name, schema=schema))

    # reverse the order of the list so time is ascending
    result = cursor.fetchall()[::-1]

    # create a df of the results and rename columns
    result = pd.DataFrame(result)
    result = result.rename(
        columns={0: 'closing_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'base_volume'})

    return result


def fill_nan(df):
    """Iterates through a dataframe and fills NaNs with appropriate open,
        high, low, close values."""

    # Forward fill close column.
    df['close'] = df['close'].ffill()

    # Backward fill the open, high, low rows with the close value.
    df = df.bfill(axis=1)

    return df

def change_ohlcv_time(df, period):
    """ Changes the time period on cryptocurrency ohlcv data.
        Period is a string denoted by 'time_in_minutesT'(ex: '1T', '5T', '60T')."""

    # Set date as the index. This is needed for the function to run
    df = df.set_index(['date'])

    # Aggregation function
    ohlc_dict = {'open':'first',
                 'high':'max',
                 'low':'min',
                 'close': 'last',
                 'base_volume': 'sum'}

    # Apply resampling
    df = df.resample(period, how=ohlc_dict, closed='left', label='left')

    return df

def np64_to_utc(np64):
    """ converts a numpy64 time to datetime """
    dt64 = np.datetime64(np64)
    unix_epoch = np.datetime64(0, 's')
    one_second = np.timedelta64(1, 's')
    seconds_since_epoch = (dt64 - unix_epoch) / one_second

    return dt.datetime.utcfromtimestamp(seconds_since_epoch)

def engineer_features(df, period):
    """Takes in a dataframe of 1 hour cryptocurrency trading data
        and returns a new dataframe with selected period, new technical analysis features,
        selects features for modeling, and keeps the last row (which is the most current time).
         The function returns a dataframe of inputs for the model and the prediction time
    """
    # Add date column to dataframe
    df['date'] = pd.to_datetime(df['closing_time'], unit='s')

    # Convert dataframe to correct period determine by model
    df = change_ohlcv_time(df, period)

    # Add feature to indicate gaps in data (could be caused by exchange being down)
    df['nan_ohlc'] = df['close'].apply(lambda x: 1 if pd.isnull(x) else 0)

    # Fill in missing values that were cause by gaps in data
    df = fill_nan(df)

    # Reset index
    df = df.reset_index()

    # Create additional date features
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    print('date feat engineered')

    # Add technical analysis features
    df = add_all_ta_features(df, "open", "high", "low", "close", "base_volume", fillna=True)
    print('ta feat engineered')

    # Set index and replace infinite values with NaNs
    df = df.replace([np.inf, -np.inf], np.nan)

    # Drop any features whose mean of missing values is greater than 20%
    df = df[df.columns[df.isnull().mean() < .2]]

    # Replace remaining NaN values with the mean of each respective column and reset index
    df = df.apply(lambda x: x.fillna(x.mean()),axis=0)

    # Create a feature for close price difference (current close price minus previous close price)/(current close price)
    df['close_diff'] = (df['close'] - df['close'].shift(1))/df['close'].shift(1)

    # keep only the last column
    df = df[-1:]

    # get time of prediction
    prediction_time = df.date.values[0]
    prediction_time = np64_to_utc(str(prediction_time))

    # feature selection
    feature_columns = ['open', 'high', 'low', 'close', 'base_volume', 'nan_ohlc', 'year', 'month', 'day', 'volume_adi',
                        'volume_obv', 'volume_cmf', 'volume_fi', 'volume_em', 'volume_vpt', 'volume_nvi', 'volatility_atr',
                        'volatility_bbh', 'volatility_bbl', 'volatility_bbm', 'volatility_bbhi', 'volatility_bbli', 'volatility_kcc',
                        'volatility_kch', 'volatility_kcl', 'volatility_kchi', 'volatility_kcli', 'volatility_dch', 'volatility_dcl',
                        'volatility_dchi', 'volatility_dcli', 'trend_macd', 'trend_macd_signal', 'trend_macd_diff', 'trend_ema_fast',
                        'trend_ema_slow', 'trend_adx_pos', 'trend_adx_neg', 'trend_vortex_ind_pos', 'trend_vortex_ind_neg', 'trend_vortex_diff',
                        'trend_trix', 'trend_mass_index', 'trend_cci', 'trend_dpo', 'trend_kst', 'trend_kst_sig', 'trend_kst_diff', 'trend_ichimoku_a',
                        'trend_ichimoku_b', 'trend_visual_ichimoku_a', 'trend_visual_ichimoku_b', 'trend_aroon_up', 'trend_aroon_down', 'trend_aroon_ind',
                        'momentum_rsi', 'momentum_mfi', 'momentum_tsi', 'momentum_uo', 'momentum_stoch', 'momentum_stoch_signal', 'momentum_wr', 'momentum_ao',
                        'others_dr', 'others_dlr', 'others_cr', 'close_diff']
    df = df[feature_columns]

    return df, prediction_time


# lambda function
def prediction_to_database(table_names):
    """ This function connects to a database, retrieves data, does FE, loads
        models from S3 bucket, makes predictions, and inserts those predictions
        into trp table under prediction schema in database. There is a known bug
        where duplicates are not caught and there are duplicates being inserted
        in the table - has to be fixed """

    # create connection to database
    conn, cursor = create_conn(credentials)
    'connected to db'

    # # connect to S3 bucket
    # s3 = boto3.resource('s3')
    print('connected to S3')

    # # connect to S3 bucket locally to test
    # # make sure the bucket has public access
    # # all tr models should be in the bucket
    s3 = boto3.resource('s3',
                        aws_access_key_id='', # fill in access key
                        aws_secret_access_key='' # fill in secret)

    # fill in bucket name
    bucket = ''

    # iterate through each table in database
    for table_name in table_names:
        print('---------------STARTING {table_name}-----------------'.format(table_name=table_name))
        # define exchange and trading_pair
        if len(table_name.split('_')) == 4:
            exchange = table_name.split('_')[0] + '_' + table_name.split('_')[1]
            trading_pair = table_name.split('_')[2] + '_' + table_name.split('_')[3]
        else:
            exchange = table_name.split('_')[0]
            trading_pair = table_name.split('_')[1] + '_' + table_name.split('_')[2]

        # conditional to catch duplicates
        # fetch timestamps for correct exchange/trading_pair
        select_query = ("""SELECT p_time FROM prediction.trp
                          WHERE exchange = '{exchange}'
                          AND trading_pair = '{trading_pair}'
                          ORDER BY p_time desc LIMIT 100;""".format(exchange=exchange, trading_pair=trading_pair))
        cursor.execute(select_query)

        # get timestamps and adjust formatting
        timestamps = cursor.fetchall()
        timestamps = [timestamp[0] for timestamp in timestamps]
        print('timestamps', timestamps)

        # define schema
        schema = 'onehour'

        # get last column of prediction data
        data_df = retrieve_data(cursor, table_name, schema)
        print('retrieved_data')

        # get time of prediction
        date = pd.to_datetime(data_df['closing_time'][-1:], unit='s').values[0]
        prediction_time = np64_to_utc(str(date))
        print(prediction_time)

        # define period
        period = model_periods[table_name]

        # Conditional to catch duplicates before inserting
        if str(prediction_time) not in timestamps:

            # define period
            period = model_periods[table_name]

            # iterate through the data and engineer features
            df, prediction_time = engineer_features(data_df, period)
            print('features engineered')

            # define model
            model_path = table_name + '.pkl'

            # load model from S3 bucket
            model = pickle.loads(s3.Bucket(bucket).Object(model_path).get()['Body'].read())
            print('loaded model')

            # make prediction
            pred = model.predict(df)

            # prediction output, price going up or down the next period
            if pred[0] == True:
                pred = 'Up'
            else:
                pred = 'Down'


            # TODO: return period in the result below
            # This needs to be done because each model has a different period for prediction
            # That was a change made after this function was written
            # Steps:
            # 1- database table has to be changed so that it has a period column
            # 2- add period in the result list below
            # 3- modify the flask app so its not returning period from there

            # creating the output result of our prediction
            result = [str(prediction_time), str(dt.datetime.utcnow()), exchange, trading_pair, pred]
            print(result)

            # insert into db
            insert_query = """INSERT INTO prediction.trp
                              (p_time, c_time, exchange, trading_pair, prediction)
                              VALUES (%s, %s, %s, %s, %s)"""
            cursor.execute(insert_query, result)
            print('inserted into db!')

        else:
            print('duplicate not inserted')

    # commit and close connection
    conn.commit()
    conn.close()
    return 'completed function'

# run locally to test
# print(prediction_to_database(table_names))


def lambda_handler(event, context):
    prediction_to_database(table_names)
    return 'succesfully inserted data'