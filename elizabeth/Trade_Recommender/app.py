import numpy as np
import pandas as pd
from flask import Flask, jsonify, request
import pickle
from ta import add_all_ta_features
import datetime
import glob

# all trading pairs and exchanges
trading_pair_list = ['BTC/USD', 'ETH/USD', 'LTC/USD']
exchange_list = ['Bitfinex', 'Coinbase Pro', 'Poloniex']

# get model paths
model_paths = sorted(glob.glob("models/*.pkl"))
# print(model_paths)

def load_models(model_paths):
    """ Takes in a list of model paths and returns a list of the model objects

            right now its in the order:
                model_bitfinex_btc_usd, model_bitfinex_eth_usd, model_bitfinex_ltc_usd,
                model_cbpro_btc_usd, model_cbpro_eth_usd, model_cbpro_ltc_usd,
                model_poloniex_btc_usd, model_poloniex_eth_usd, model_poloniex_ltc_usd
    """

    loaded_models = []

    for path in model_paths:
        loaded_model = pickle.load(open(path, 'rb'))
        loaded_models.append(loaded_model)

    # list of models - will need this if you want to make a dict
    # model_list = [model_bitfinex_btc_usd, model_bitfinex_eth_usd, model_bitfinex_ltc_usd,
    #           model_cbpro_btc_usd, model_cbpro_eth_usd, model_cbpro_ltc_usd,
    #           model_poloniex_btc_usd, model_poloniex_eth_usd, model_poloniex_ltc_usd]
    # TODO make a dict of the model name as the key and the actual model as the value so it can be accessed like that

    return loaded_models

# check if the models loaded properly
# print(load_models(model_paths))

# list of csv paths
# TODO  this will later have to be replaced with the database connection
csv_paths = sorted(glob.glob("data/*.csv"))
# print(csv_paths)

def feature_engineer(path):
    # import csv and drop the Unnamed:0 column
    df = pd.read_csv(path, index_col=0)[::-1][-60:]

    # add close_diff feature
    df['close_diff'] = df['close'] - df['close'].shift(1)

    # engineer all ta features from ta library
    df = add_all_ta_features(df, "open", "high", "low", "close", "volume", fillna=True)[-1:]

    # get time of prediction
    prediction_time = df.time.values
    prediction_time = datetime.datetime.fromtimestamp(prediction_time).strftime('%Y-%m-%d %H:%M:%S')

    # drop null columns and time
    drop_columns = ['volume_obv', 'trend_adx', 'trend_adx_pos', 'trend_adx_neg', 'trend_trix', 'time']
    df.drop(columns=drop_columns, inplace=True)

    return [df, prediction_time]


def generate_predictions(csv_paths, model_paths):

    """ Takes in a list of datasets and a list of models and generates predictions for
        each dataset with the correct model. Returns a list of outputs for each model"""

    def load_models(model_paths):
        """ Takes in a list of model paths and returns a list of the model objects

                right now its in the order:
                    model_bitfinex_btc_usd, model_bitfinex_eth_usd, model_bitfinex_ltc_usd,
                    model_cbpro_btc_usd, model_cbpro_eth_usd, model_cbpro_ltc_usd,
                    model_poloniex_btc_usd, model_poloniex_eth_usd, model_poloniex_ltc_usd
        """

        loaded_models = []

        for path in model_paths:
            loaded_model = pickle.load(open(path, 'rb'))
            loaded_models.append(loaded_model)

        # list of models - will need this if you want to make a dict
        # model_list = [model_bitfinex_btc_usd, model_bitfinex_eth_usd, model_bitfinex_ltc_usd,
        #           model_cbpro_btc_usd, model_cbpro_eth_usd, model_cbpro_ltc_usd,
        #           model_poloniex_btc_usd, model_poloniex_eth_usd, model_poloniex_ltc_usd]
        # TODO make a dict of the model name as the key and the actual model as the value so it can be accessed like that

        return loaded_models

    # engineer features
    df_list = []
    datetime_list = []
    for path in csv_paths:
        """ iterate through the data and engineer features """
        df, prediction_time = feature_engineer(path)
        df_list.append(df)
        datetime_list.append(prediction_time)

    # load models
    loaded_models = load_models(model_paths)

    # make all predictions
    i = 0
    prediction_list = []
    for model in loaded_models:
        pred = model.predict(df_list[i])
        prediction_list.append(pred[0])
        i += 1

    # write outputs
    trading_pair_list = ['BTC/USD', 'ETH/USD', 'LTC/USD']
    exchange_list = ['Bitfinex', 'Coinbase Pro', 'Poloniex']
    output_list = []
    print(len(datetime_list), len(prediction_list))

    i = 0
    for name in csv_paths:

        # create names
        exchange_and_pair = name.split('/')[1]
        exchange_name = exchange_and_pair.split('_')[0].capitalize()
        trading_pair = exchange_and_pair.split('_')[1] + '/' + exchange_and_pair.split('_')[2][:-4]
        trading_pair = trading_pair.upper()

        new_output = [exchange_name, trading_pair, datetime_list[i].split()[0],
                      datetime_list[i].split()[1], str(prediction_list[i])]
        output_list.append(new_output)
        i += 1

    return output_list

# print(generate_predictions(csv_paths, model_paths))

app = Flask(__name__)

@app.route('/cryptotraderecommender', methods=['GET'])

def crypto_trade_predictions():
    """ Takes in data from crypto exchanges and returns an output for whether
        the model predicts the price of a coin will go up or down in the next
        5 minute period.

        Supported Exchanges: Bitfinex, Coinbase Pro, Poloniex
        Supported Trading Pairs: BTC/USD, ETH/USD, LTC/USD
        """

    # TODO modify the code to take in the data from databases

    predictions = generate_predictions(csv_paths, model_paths)

    return jsonify(results=predictions)

if __name__ == '__main__':
    app.run(port = 9000, debug=True)