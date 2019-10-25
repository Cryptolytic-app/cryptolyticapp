import glob
import pandas as pd
import pickle
from ta import add_all_ta_features
import datetime as dt
import numpy as np
import psycopg2 as ps
import pandas as pd
import boto3 
import ta
from config import aws_access_key_id, aws_secret_access_key, BUCKET_NAME POSTGRES_DBNAME, POSTGRES_PASSWORD, POSTGRES_USERNAME, POSTGRES_PORT, POSTGRES_ADDRESS

""" Functions for Flask App"""

credentials = {'POSTGRES_ADDRESS': POSTGRES_ADDRESS,
               'POSTGRES_PORT': POSTGRES_PORT,
              'POSTGRES_USERNAME': POSTGRES_USERNAME,
              'POSTGRES_PASSWORD': POSTGRES_PASSWORD,
              'POSTGRES_DBNAME' : POSTGRES_DBNAME
              }


# Database connection
def create_conn(credentials):

    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    cur = conn.cursor()
    return conn, cur

#print(create_conn(credentials))

def retrieve_data(table_name, schema):

    # create connection and cursor
    conn, cur = create_conn(credentials)

    # Change limit number to whatever amount of rows you want to retrieve
    cur.execute("SELECT * FROM {schema}.{table_name} order by closing_time desc limit 60".format(table_name=table_name, schema=schema))

    result = cur.fetchall()[::-1]

    result = pd.DataFrame(result)

    result = result.rename(
        columns={0: 'closing_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'base_volume'})

    conn.close()

    return result


def change_ohlcv_time(df, period):
    """ Changes the time period on cryptocurrency ohlcv data.
        Period is a string denoted by 'time_in_minutesT'(ex: '1T', '5T', '60T')."""

    # Set date as the index. This is needed for the function to run
    df = df.set_index(['date'])

    # Aggregation function
    ohlc_dict = {                                                                                                             
    'open':'first',                                                                                                    
    'high':'max',                                                                                                       
    'low':'min',                                                                                                        
    'close': 'last',                                                                                                    
    'base_volume': 'sum'
    }

    # Apply resampling.
    df = df.resample(period, how=ohlc_dict, closed='left', label='left')

    return df


def fill_nan(df):
  
    '''Iterates through a dataframe and fills NaNs with appropriate open, high, low, close values.'''

    # Forward fill close column.
    df['close'] = df['close'].ffill()

    # Backward fill the open, high, low rows with the close value.
    df = df.bfill(axis=1)

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

    # Add technical analysis features.
    df = add_all_ta_features(df, "open", "high", "low", "close", "base_volume", fillna=True)

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


# START HERE
def generate_predictions(exchange, trading_pair, schema):
    """ Takes in a list of datasets and a list of models and generates predictions for
        each dataset with the correct model. Returns a list of outputs for each model"""

    # Define table name and model path
    table_name = exchange + '_' + trading_pair
    model_path = table_name + '.pkl'
    
    # returns a dictionary of dataframes that have the last 60 entries in the database
    data_df = retrieve_data(table_name, schema)

    Get access to our S3 service
    s3 = boto3.resource('s3',
                         aws_access_key_id=aws_access_key_id,
                         aws_secret_access_key=aws_secret_access_key)

    print('connected to s3')

    """ iterate through the data and engineer features """
    df, prediction_time = engineer_features(data_df)

    # Loaded Model from S3 Bucket
    loaded_model = pickle.loads(s3.Bucket(BUCKET_NAME).Object(model_path).get()['Body'].read())

    # make prediction
    pred = loaded_model.predict(df)

    # Prediction Output, Price going up or down the next hour
    if pred[0] == True:
        pred = 'Up'
    else:
        pred = 'Down'


    # Creating the Output Result of our Prediction
    output = [exchange, trading_pair, str(prediction_time).split()[0], str(prediction_time).split()[1], pred]

    return output
