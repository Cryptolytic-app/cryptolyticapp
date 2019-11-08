import pandas as pd
import pickle
from ta import add_all_ta_features
import datetime as dt
import numpy as np
import psycopg2 as ps
import pandas as pd
import boto3
import os

# arb

coinbase_pro_pairs = ['btc_usd', 'eth_btc', 'etc_usd',
                      'eth_usd', 'ltc_btc', 'ltc_usd', 'ltc_btc']
bitfinex_pairs = ['btc_usd', 'eth_btc', 'etc_usd', 'eth_usd',
                  'ltc_btc', 'ltc_usd', 'dash_btc', 'eos_btc', 'xrp_btc']
hitbtc_pairs = ['btc_usdt', 'eth_btc', 'eth_usdt',
                'ltc_usdt', 'ltc_btc', 'dash_btc', 'eos_btc', 'xrp_btc']

hitbtc_table_list = ['hitbtc_' + pair for pair in hitbtc_pairs]
bitfinex_table_list = ['bitfinex_' + pair for pair in bitfinex_pairs]
coinbase_pro_table_list = ['coinbase_pro_' +
                           pair for pair in coinbase_pro_pairs]

table_names = bitfinex_table_list + coinbase_pro_table_list + hitbtc_table_list
exchanges = ['coinbase_pro', 'bitfinex', 'hitbtc']


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


def create_conn(credentials):

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
        Returns the last 60 rows from the table selected."""

    # Get the last 60 rows from the DB
    cursor.execute("""SELECT * FROM {schema}.{table_name} 
                      ORDER BY closing_time DESC
                      LIMIT 60""".format(table_name=table_name, schema=schema))

    # Reverse order of the data so the most current time is at the bottom
    result = cursor.fetchall()[::-1]

    # Create dataframe with data
    result = pd.DataFrame(result)
    result = result.rename(columns={0: 'closing_time', 1: 'open', 2: 'high',
                                    3: 'low', 4: 'close', 5: 'base_volume'})

    return result


def np64_to_utc(np64):
    """ converts a numpy64 time to datetime """
    dt64 = np.datetime64(np64)
    unix_epoch = np.datetime64(0, 's')
    one_second = np.timedelta64(1, 's')
    seconds_since_epoch = (dt64 - unix_epoch) / one_second

    return dt.datetime.utcfromtimestamp(seconds_since_epoch)

# function to get exchange and trading_pair bc coinbase_pro has an extra '_'


def get_exchange_trading_pair(table_name):
    if len(table_name.split('_')) == 4:
        exchange = table_name.split('_')[0] + '_' + table_name.split('_')[1]
        trading_pair = table_name.split(
            '_')[2] + '_' + table_name.split('_')[3]
    else:
        exchange = table_name.split('_')[0]
        trading_pair = table_name.split(
            '_')[1] + '_' + table_name.split('_')[2]
    return exchange, trading_pair


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


print(get_table_pairs(table_names, exchanges))

#############################################################################
########################## NATHAN'S FUNCTIONS ###############################
#############################################################################


def engineer_features(df):

    # create technical analysis features
    df = add_all_ta_features(df, 'open', 'high', 'low', 'close',
                             'base_volume', fillna=True)

    # get explanation from nathan
    df['time_since_last'] = df['closing_time'].diff(-1)

    return df


def merge_dfs(df1, df2):
    """ Takes two dataframes with the same features and merges them on closing_time,
        renames the features, and returns the last row """
    df = pd.merge(df1, df2, on='closing_time',
                  suffixes=('_exchange_1', '_exchange_2'))

    # Keep only the last column
    df = df[-1:]

    # Add date column to dataframe
    df['date'] = pd.to_datetime(df['closing_time'], unit='s')

    # get time of prediction
    prediction_time = df.date.values[0]
    prediction_time = np64_to_utc(str(prediction_time))

    ############### FILL NANS - DELETE THIS LATER #################
    df = df.fillna(1)

    # select features
    features = ['closing_time', 'close_exchange_1', 'base_volume_exchange_1', 'volume_adi_exchange_1',
                'volume_obv_exchange_1', 'volume_cmf_exchange_1', 'volume_fi_exchange_1',
                'volume_em_exchange_1', 'volume_vpt_exchange_1', 'volume_nvi_exchange_1',
                'volatility_atr_exchange_1', 'volatility_bbhi_exchange_1', 'volatility_bbli_exchange_1',
                'volatility_kchi_exchange_1', 'volatility_kcli_exchange_1', 'volatility_dchi_exchange_1',
                'volatility_dcli_exchange_1', 'trend_macd_signal_exchange_1', 'trend_macd_diff_exchange_1',
                'trend_adx_exchange_1', 'trend_adx_pos_exchange_1', 'trend_adx_neg_exchange_1',
                'trend_vortex_ind_pos_exchange_1', 'trend_vortex_ind_neg_exchange_1',
                'trend_vortex_diff_exchange_1', 'trend_trix_exchange_1', 'trend_mass_index_exchange_1',
                'trend_cci_exchange_1', 'trend_dpo_exchange_1', 'trend_kst_sig_exchange_1',
                'trend_kst_diff_exchange_1', 'trend_aroon_up_exchange_1', 'trend_aroon_down_exchange_1',
                'trend_aroon_ind_exchange_1', 'momentum_rsi_exchange_1', 'momentum_mfi_exchange_1',
                'momentum_tsi_exchange_1', 'momentum_uo_exchange_1', 'momentum_stoch_signal_exchange_1',
                'momentum_wr_exchange_1', 'momentum_ao_exchange_1', 'others_dr_exchange_1',
                'time_since_last_exchange_1', 'close_exchange_2', 'base_volume_exchange_2',
                'volume_adi_exchange_2', 'volume_obv_exchange_2', 'volume_cmf_exchange_2',
                'volume_fi_exchange_2', 'volume_em_exchange_2', 'volume_vpt_exchange_2',
                'volume_nvi_exchange_2', 'volatility_atr_exchange_2', 'volatility_bbhi_exchange_2',
                'volatility_bbli_exchange_2', 'volatility_kchi_exchange_2', 'volatility_kcli_exchange_2',
                'volatility_dchi_exchange_2', 'volatility_dcli_exchange_2', 'trend_macd_signal_exchange_2',
                'trend_macd_diff_exchange_2', 'trend_adx_exchange_2', 'trend_adx_pos_exchange_2',
                'trend_adx_neg_exchange_2', 'trend_vortex_ind_pos_exchange_2', 'trend_vortex_ind_neg_exchange_2',
                'trend_vortex_diff_exchange_2', 'trend_trix_exchange_2', 'trend_mass_index_exchange_2',
                'trend_cci_exchange_2', 'trend_dpo_exchange_2', 'trend_kst_sig_exchange_2',
                'trend_kst_diff_exchange_2', 'trend_aroon_up_exchange_2', 'trend_aroon_down_exchange_2',
                'trend_aroon_ind_exchange_2', 'momentum_rsi_exchange_2', 'momentum_mfi_exchange_2',
                'momentum_tsi_exchange_2', 'momentum_uo_exchange_2', 'momentum_stoch_signal_exchange_2',
                'momentum_wr_exchange_2', 'momentum_ao_exchange_2', 'others_dr_exchange_2',
                'time_since_last_exchange_2']
    df = df[features]
    print('number of features:', len(features))

    return df, prediction_time

#############################################################################
##########################        END         ###############################
#############################################################################


def generate_predictions_arb(table_names, exchanges):
    """ Takes in a list of table names and exchanges and makes predictions using
        the correct arbitrage model and inserts the prediction into the database"""

    # create connection to database
    conn, cursor = create_conn(credentials)

    # # Get access to our S3 service
    s3 = boto3.resource('s3')

    # Insert your bucket name here
    bucket = 'bucket-name'

    # delete this list later
    model_path_list = []

    # iterate through all combinations of exchanges with a trading pair match
    for table_names in get_table_pairs(table_names, exchanges):
        print(
            '---------------STARTING {table_names}-----------------'.format(table_names=table_names))

        # get exchanges and trading pairs
        exchange_1, trading_pair_1 = get_exchange_trading_pair(table_names[0])
        exchange_2, trading_pair_2 = get_exchange_trading_pair(table_names[1])
        print(trading_pair_1, trading_pair_2)
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
        time_list = [df1['closing_time'][-1:].values[0],
                     df2['closing_time'][-1:].values[0]]
        time_min = np.min(time_list)
        time_min = pd.to_datetime(time_min, unit='s')
        time_min = np64_to_utc(str(time_min))
        print('time min', time_min)

        # Conditional to catch duplicates before inserting
        # NOTE - this won't get every duplicate
        if str(time_min) not in timestamps:
            # feature engineering
            df1 = engineer_features(df1)
            df2 = engineer_features(df2)
            print('features engineered')

            # merge dataframes
            df, prediction_time = merge_dfs(df1, df2)
            print('dataframes merged')
            print(df)
            print('prediction time', prediction_time)

            # filter duplicates again in case prediction_time is different from time_min
            if str(prediction_time) not in timestamps:
                # define model path
                model_path = exchange_1 + '_' + table_names[1] + '.pkl'
                print(model_path)
                model_path_list.append(model_path)

                # Load Model from S3 Bucket
                loaded_model = pickle.loads(
                    s3.Bucket(bucket).Object(model_path).get()['Body'].read())
                print('loaded model')

                # make prediction
                pred = loaded_model.predict(df)
                print('prediction made:', pred)

                # Creating the Output Result of our Prediction
                output = [str(prediction_time), str(
                    dt.datetime.utcnow()), exchange_1, exchange_2, trading_pair_1, pred[0]]
                print(output)

                insert_query = """INSERT INTO prediction.arp
                                  (p_time, c_time, exchange_1, exchange_2, trading_pair, prediction)
                                  VALUES (%s, %s, %s, %s, %s, %s)"""
                cursor.execute(insert_query, output)

                print('inserted into db!')

            else:
                print('duplicates not inserted')
        else:
            print('duplicates not inserted')

    # commit and close connection
    conn.commit()
    conn.close()

    print('model paths:', model_path_list)

    return 'function complete!'


def lambda_handler(event, context):
    generate_predictions_arb(table_names, exchanges)
    return 'succesfully inserted data'
