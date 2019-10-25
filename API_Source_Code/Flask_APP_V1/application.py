import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, render_template
import pickle
from ta import add_all_ta_features
import datetime as dt
import glob
import boto3
from utils import create_conn, retrieve_data, credentials, generate_predictions


application = Flask(__name__)


@application.route('/')
def index():
    """Homepage"""
    return render_template('index.html')


@application.route('/crypto', methods=['POST'])
def crypto_trade_predictions():
    """ Takes in data from crypto exchanges and returns an output for whether
        the model predicts the price of a coin will go up or down in the next
        1 hour period.

        Supported Exchange/Trading Pairs:
            - bitfinex
                - 'btc_usd'
                - 'eth_usd'
                - 'ltc_usd'
            - coinbase_pro
                - 'btc_usd'
                - 'eth_usd'
                - 'ltc_usd'
            - hitbtc
                - 'btc_usdt'
                - 'eth_usdt'
                - 'ltc_usdt'

        Sample request:
        post = { "exchange" : "bitfinex",
                 "trading_pair" : "btc_usd" }
        """

    # request data
    exchange = request.form.get('exchange')
    trading_pair = request.form.get('trading_pair')

    # HitBTC needs a t at the end for usd pairings
    if exchange == 'hitbtc':
        trading_pair = trading_pair + 't'

    schema = 'onehour'

    predictions = generate_predictions(exchange, trading_pair, schema)

    return render_template("result.html", results=predictions)


@application.route('/testing', methods=['POST'])
def crypto_trade_predictions_testing():

    # request data
    exchange_trading_pair = request.get_json(force=True)

    # Define Parameters
    schema = 'onehour'
    exchange = exchange_trading_pair['exchange']
    trading_pair = exchange_trading_pair['trading_pair']
    
    predictions = generate_predictions(exchange, trading_pair, schema)

    return jsonify(results=predictions)


if __name__ == '__main__':
    application.run(port=3000, debug=True)
