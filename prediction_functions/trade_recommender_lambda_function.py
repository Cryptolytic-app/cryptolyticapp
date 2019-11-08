import pandas as pd
import pickle
from ta import add_all_ta_features
import datetime as dt
import numpy as np
import psycopg2 as ps
import boto3
import os

# trp

coinbase_pro_pairs = ['btc_usd', 'eth_usd', 'ltc_usd']
bitfinex_pairs = ['btc_usd', 'eth_usd', 'ltc_usd']
hitbtc_pairs = ['btc_usdt', 'eth_usdt', 'ltc_usdt']

bitfinex_table_list = ['bitfinex_' + pair for pair in bitfinex_pairs]

coinbase_pro_table_list = ['coinbase_pro_' + pair for pair in
                           coinbase_pro_pairs]

hitbtc_table_list = ['hitbtc_' + pair for pair in hitbtc_pairs]

table_names = bitfinex_table_list + coinbase_pro_table_list + hitbtc_table_list

# decrypt credentials - Add in credentials in AWS environment variable
POSTGRES_ADDRESS = os.environ['POSTGRES_ADDRESS']

POSTGRES_PORT = os.environ['POSTGRES_PORT']

POSTGRES_USERNAME = os.environ['POSTGRES_USERNAME']

POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']

POSTGRES_DBNAME = os.environ['POSTGRES_DBNAME']


credentials = {'POSTGRES_ADDRESS': POSTGRES_ADDRESS,
                'POSTGRES_PORT': POSTGRES_PORT,
                'POSTGRES_USERNAME': POSTGRES_USERNAME,
                'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
                'POSTGRES_DBNAME': POSTGRES_DBNAME
                }


# Database connection
def create_conn(credentials):

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
        Returns the last 60 rows from the table selected."""
    # Change limit number to whatever amount of rows you want to retrieve
    cursor.execute("""SELECT * FROM {schema}.{table_name} 
                      ORDER BY closing_time DESC 
                      LIMIT 1000""".format(table_name=table_name, schema=schema))

    result = cursor.fetchall()[::-1]

    result = pd.DataFrame(result)

    result = result.rename(
        columns={0: 'closing_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'base_volume'})

    return result


def fill_nan(df):
    """Iterates through a dataframe and fills NaNs with appropriate open, high, low, close values."""

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
                 'base_volume': 'sum'
                }

    # Apply resampling.
    df = df.resample(period, how=ohlc_dict, closed='left', label='left')

    return df

def np64_to_utc(np64):
    """ converts a numpy64 time to datetime """
    dt64 = np.datetime64(np64)
    unix_epoch = np.datetime64(0, 's')
    one_second = np.timedelta64(1, 's')
    seconds_since_epoch = (dt64 - unix_epoch) / one_second

    return dt.datetime.utcfromtimestamp(seconds_since_epoch)

def engineer_features(df):

    # Add date column to dataframe.
    df['date'] = pd.to_datetime(df['closing_time'], unit='s')

    # Convert dataframe to one hour candles.
    period = '60T'
    df = change_ohlcv_time(df, period)

    # Add feature to indicate user inactivity.
    df['nan_ohlc'] = df['close'].apply(lambda x: 1 if pd.isnull(x) else 0)

    # Fill in missing values using fill function.
    df = fill_nan(df)

    # Reset index.
    df = df.reset_index()

    # Create additional date features.
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day'] = df['date'].dt.day
    print('date feat engineered')

    # Add technical analysis features.
    df = add_all_ta_features(df, "open", "high", "low", "close", "base_volume", fillna=True)
    print('ta feat engineered')

    # Set index and replace infinite values with NaNs.
    df = df.replace([np.inf, -np.inf], np.nan)

    # Drop any features whose mean of missing values is greater than 20%.
    df = df[df.columns[df.isnull().mean() < .2]]

    # Replace remaining NaN values with the mean of each respective column and reset index.
    df = df.apply(lambda x: x.fillna(x.mean()),axis=0)

    # Create a feature for close price difference (current close price minus previous close price)/(current close price).
    df['close_diff'] = (df['close'] - df['close'].shift(1))/df['close'].shift(1)

    # keep only the last column
    df = df[-1:]

    # get time of prediction
    prediction_time = df.date.values[0]
    prediction_time = np64_to_utc(str(prediction_time))

    # keep only model's features
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
    df = df.rename(columns={'base_volume':'volume'})

    return df, prediction_time


# lambda function
def prediction_to_database(table_names):

    # create connection to database
    conn, cursor = create_conn(credentials)

    # Connect to S3 Bucket
    s3 = boto3.resource('s3')

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

        # Conditional to catch duplicates
        # Fetch all columns with correct exchange/trading_pair
        select_query = ("""SELECT p_time FROM prediction.trp
                          WHERE exchange = '{exchange}'
                          AND trading_pair = '{trading_pair}'
                          ORDER BY p_time desc LIMIT 100;""".format(exchange=exchange, trading_pair=trading_pair))

        cursor.execute(select_query)
        timestamps = cursor.fetchall()
        timestamps = [timestamp[0] for timestamp in timestamps]
        print('timestamps', timestamps)

        # define schema
        schema = 'onehour'

        # get last column of prediction data
        data_df = retrieve_data(cursor, table_name, schema)
        print('retrieved_data', data_df)

        # get time of prediction
        date = pd.to_datetime(data_df['closing_time'][-1:], unit='s').values[0]
        prediction_time = np64_to_utc(str(date))

        # Conditional to catch duplicates before inserting
        if str(prediction_time) not in timestamps:
            # iterate through the data and engineer features
            df, prediction_time = engineer_features(data_df)
            print('features engineered')

            # Insert your bucket name here
            bucket = 'bucket-name'

            # define model
            model_path = table_name + '.pkl'
            # Loaded Model from S3 Bucket
            loaded_model = pickle.loads(s3.Bucket(bucket).Object(model_path).get()['Body'].read())
            print('loaded model')

            # make prediction
            pred = loaded_model.predict(df)

            # Prediction Output, Price going up or down the next hour
            if pred[0] == True:
                pred = 'Up'
            else:
                pred = 'Down'

            # Creating the Output Result of our Prediction
            result = [str(prediction_time), str(dt.datetime.utcnow()), exchange, trading_pair, pred]
            print(result)
            print(type(result[0]))
            print(type(result[1]))

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
    
def lambda_handler(event, context):
    prediction_to_database(table_names)
    return 'succesfully inserted data'