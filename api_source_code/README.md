# Flask App Documentation
Flask app allows user to request crypto prediction data. Flask App deployed on AWS Elastic Beanstalk.

The zip file includes:

application.py

utils.py

requirement.txt

/templates
  - index.html
  - result.html
  - resultarb.html
  - error.html
  
/.ebextension
  - app.config


## Flask App Endpoints

### Homepage - [/](http://www.cryptolyticapp.com/)
Returns a render template where a user can request prediction data

### Trade Prediction - /crypto
Returns this endpoint if user requested trade predictions

Method: ["POST"]

### Arbitrage Prediction - /arb
Returns this endpoint if user requested arbitrage predictions

Method: ["POST"]

### Trade API - [/trade](http://www.cryptolyticapp.com/trade)
Returns a json result of all available trade predictions

Method: ["GET"]

 Returns: ``` {"results":
"{('exchange', 'trading_pair'): [{
'p_time': 'time',
‘period’: ‘minutes’,
'prediction': 'result'}], }"} ```
  
### Arbitrage API - [/arbitrage](http://www.cryptolyticapp.com/arbitrage)
Returns a json result of all available arbitrage predictions

Method: ["GET"]

Returns: ``` {"results":"{
('exchange_1', 'exchange_2', 'trading_pair'): [
{'p_time': 'time',
'prediction': 'result'}
]} ```
