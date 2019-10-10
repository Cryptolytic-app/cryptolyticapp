import numpy as np
from flask import Flask, jsonify, request
import pickle

# model
my_model = pickle.load(open('model.pkl','rb'))
# load all 9 models

app = Flask(__name__)

@app.route('/cryptotraderecommender', methods=['POST'])

def predict():
    # get data
    # pull in data from 9 database sources

    # transform/parse
    # calculate technical analysis features

    # preds
     preds = my_model.predict(predict_request)
    # make 9 predictions

    # convert model outputs into text

    # send back to browser
    output = {#TODO write output}
    return jsonify(results=output)

if __name__ == '__main__':
    app.run(port = 5000, debug=True)