from flask import Flask, jsonify, render_template
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from utils import retrieve_arb_pred, retrieve_tr_pred
from display import display_tr_pred, display_arb_pred


application = Flask(__name__)

# default limit for each route
limiter = Limiter(
    application,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)


@application.route('/')
def index():
    """ Homepage """
    return render_template('public/index.html')


@application.route('/api')
def api():
    """ API Documentation """
    return render_template('public/api_doc.html')


@application.route('/trade_rec', methods=['GET'])
def tr_predictions():
    """
    Returns a dataframe of cryptocurrency trade predictions whether it will go up or down in a certain time period
    """

    try:
        df = display_tr_pred()
        return render_template('public/display_tr.html', tables=[df.to_html(classes='data')], titles=df.columns.values)
    except:
        return render_template('public/error.html')


@application.route('/arb', methods=['GET'])
def arbitrage_predictions():
    """ Returns a dataframe of cryptocurrency arbitrage predictions """

    try:
        df = display_arb_pred()
        return render_template('public/display_arb.html', tables=[df.to_html(classes='data')], titles=df.columns.values)
    except:
        return render_template('public/error.html')


@application.route('/trade', methods=['GET'])
@limiter.limit("10 per minute")
def tr_predictions_api():
    """ Retrieve all available trade predictions and return in json format """

    try:
        predictions = retrieve_tr_pred()
        return jsonify(results=str(predictions))
    except:
        return render_template('public/error.html')


@application.route('/arbitrage', methods=['GET'])
@limiter.limit("30 per minute")
def arbitrage_predictions_api():
    """ Retrieve all available arbitrage predictions and return in json format """

    try:
        predictions = retrieve_arb_pred()
        return jsonify(results=str(predictions))
    except:
        return render_template('public/error.html')


if __name__ == '__main__':
    application.run(port=5000, debug=True)
