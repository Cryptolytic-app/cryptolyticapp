# Cryptolytic

## Project Overview
Cryptolytic is a platform for beginners tinkering with cryptocurrency to the seasoned trader. It provides you with recommendations on when to buy and sell based on technical indicators and assesses the markets to predict arbitrage opportunities before they even happen.

[Application Website!](http://www.cryptolyticapp.com/)

[Watch our demo video here!](https://youtu.be/ikKwhEgnNgw)

<img src="https://github.com/Cryptolytic-app/cryptolyticapp/blob/master/assets/cryptolytic_thumbnail.png?raw=true" width = "1000" />

## Contributers
### Team Lead
 |                                       [Stanley Kusmier](https://github.com/standroidbeta)                                        |
| :-----------------------------------------------------------------------------------------------------------: |
|                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Stan.png?raw=true" width = "200" />](https://github.com/standroidbeta)                       |
|                 [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/standroidbeta)                 |
| [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/stanley-kusmier-0563a153/) |
### Data Science


|                                       [Alfredo Quintana](https://github.com/alqu7095)                                        |                                       [Elizabeth Ter Sahakyan](https://www.github.com/elizabethts)                                        |                                       [Marvin A Davila](https://github.com/malexmad)                                        |                                       [Nathan Van Wyck](https://github.com/nrvanwyck)                                        |                                       [Taylor Bickell](https://github.com/tcbic)                                        |
| :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: |
|                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Alfredo.jpeg?raw=true" width = "200" />](https://github.com/alqu7095)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Elizabeth.jpeg?raw=true" width = "200" />](https://www.github.com/elizabethts)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Marvin.jpeg?raw=true" width = "200" />](https://github.com/malexmad)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Nathan.png?raw=true" width = "200" />](https://github.com/nrvanwyck)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Taylor.jpeg?raw=true" width = "200" />](https://github.com/tcbic)                       |
|                 [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/alqu7095)                 |            [<img src="https://github.com/favicon.ico" width="15"> ](https://www.github.com/elizabethts)             |           [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/malexmad)            |          [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/nrvanwyck)           |            [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/tcbic)             |
| [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/alfredo-quintana-98248a76/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/elizabethts) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/marvin-davila/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/nathan-van-wyck-48586718a/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/taylorbickell/) |


### Application Architechture
<img src="https://github.com/Cryptolytic-app/cryptolyticapp/blob/master/assets/cryptolytic-architecture.png?raw=true" width = "1000" />

### How it Works
We gather the historical data from each exchange’s API into a database for all of our supported exchanges and trading pairs and implement Lambda functions in Cloud9 to collect live data from the cryptowat.ch API in the respective tables. This allows us to have the most up to date data in our databases for predictions.

Random forest classifier models were then trained on that dataset, stored in S3 buckets, and more Lambda functions were used to load those models and make predictions on the live data every 3 minutes. Those predictions are inserted into the database in a new table that stores all of the predictions.

The Flask app retrieves the most recent predictions from the database and can return them to a user or be available for a backend team to use via API. The API was deployed on Elastic Beanstalk.

Overall we created a data pipeline and a backend that makes use of 30 different models to generate and store predictions on a recurring basis, which can then be accessed via API endpoints.

### Product Canvas
[Notion Link](https://www.notion.so/e563b27ab8e94ce2a3f7b536fc365715?v=3781e3eb9e72447f9262ebacd1e21fa9)

### Tech Stack
Python, SQL, Flask, AWS (Elastic Beanstalk, RDS, Lambda Functions, Cloud9, KMS, Sagemaker), PostgreSQL

### Predictions

The models folder contains two zip files, with a total of 30 models:

tr_pickles.zip contains nine pickled trade recommender models.

arb_models.zip contains 21 pickled arbitrage models.

All 30 models use a RandomForestClassifier algorithm.

Each trade recommender model recommends trades for a particular trading pair on a particular exchange by predicting whether the closing price will increase by enough to cover the costs of executing a trade.

The arbitrage models predict arbitrage opportunities between two exchanges for a particular trading pair.  Predictions are made ten minutes in advance.  To count as an arbitrage opportunity, a price disparity between two exchanges must last for at least thirty minutes, and the disparity must be great enough to cover the costs of buying on one exchange and selling on the other.

### Features

Each of the nine trade recommender models is trained on 67 features.  Of those 67 features, five are taken directly from the OHLCV data (open, high, low, close, base_volume), one indicates where gaps were present in the data (nan_ohlcv), three indicate the time (year, month, day), and the remainder are technical analysis features.

Each of the 21 arbitrage models is trained on 91 features.  Of those 91 features, three features indicate the time (year, month, day), and four indicate the degree and length of price disparities between two exchanges (higher_closing_price, pct_higher, arbitrage_opportunity, window_length).  Half of the remaining 84 features are specific to the first of the two exchanges in a given arbitrage dataset and are labelled with the suffix "exchange_1"; the other half are specific to the second of those two exchanges and are labelled with the suffix "exchange_2".  In each of these two sets of 42 features, two are taken directly from the OHLCV data (close_exchange_#, base_volume_exchange_#), one indicates where gaps were present in the data (nan_ohlcv), and the remainder are technical analysis features.

Technical analysis features were engineered with the Technical Analysis Library; they fall into five types:

(1) Momentum indicators

(2) Volume indicators

(3) Volatility indicators

(4) Trend indicators

(5) Others indicators

Documentation for the technical analysis features features is available here:

[Technical Analysis Library Documentation](https://technical-analysis-library-in-python.readthedocs.io/en/latest/ta.html)

### Data Sources

We obtained all of our data from the Cryptowatch, Bitfinex, Coinbase Pro, and HitBTC APIs. Documentation for obtaining that data is listed below:

[Cryptowatch API OHLCV Data Documentation](https://developer.cryptowat.ch/reference/rest-api-markets#market-ohlc-candlesticks)

[Bitfinex API OHLCV Data Documentation](https://docs.bitfinex.com/reference#rest-public-candles)

[Coinbase Pro API OHLCV Data Documentation](https://docs.pro.coinbase.com/?r=1#get-historic-rates)

[HitBTC OHLCV Data Documentation](https://api.hitbtc.com/#candles)

[Kraken OHLCV Data Documentation](https://www.kraken.com/features/api)

[Gemini OHLCV Data Documentation](https://docs.gemini.com/rest-api/)

### Python Notebooks

[Data Processing Notebook](https://github.com/Cryptolytic-app/cryptolyticapp/tree/master/modeling/1_arbitrage_data_processing.ipynb)
[Modeling Notebook](https://github.com/Cryptolytic-app/cryptolyticapp/tree/master/modeling/2_arbitrage_model_training.ipynb)
[Model Evaluation Notebook](https://github.com/Cryptolytic-app/cryptolyticapp/tree/master/modeling/3_arbitrage_model_evaluation.ipynb)


## How to connect to the Cryptolytic API
 http://www.cryptolyticapp.com/ (running on AWS but models outdated!)

### Trade API [/trade](http://www.cryptolyticapp.com/trade)

 Method: ["GET"]

 Returns: ``` {"results":
"{('exchange', 'trading_pair'): [{
'p_time': 'time',
‘period’: ‘minutes’,
'prediction': 'result'}], }"} ```
  
### Arbitrage API [/arbitrage](http://www.cryptolyticapp.com/arbitrage)
Note: Unavailable between 7pm - 5am PST  

Method: ["GET"]

Returns: ``` {"results":"{
('exchange_1', 'exchange_2', 'trading_pair'): [
{'p_time': 'time',
'prediction': 'result'}
]} ```


## Contributing

When contributing to this repository, please first discuss the change you wish to make via issue, email, or any other method with the owners of this repository before making a change.

Please note we have a [code of conduct](./code_of_conduct.md.md). Please follow it in all your interactions with the project.

### Issue/Bug Request

 **If you are having an issue with the existing project code, please submit a bug report under the following guidelines:**
 - Check first to see if your issue has already been reported.
 - Check to see if the issue has recently been fixed by attempting to reproduce the issue using the latest master branch in the repository.
 - Create a live example of the problem.
 - Submit a detailed bug report including your environment & browser, steps to reproduce the issue, actual and expected outcomes,  where you believe the issue is originating from, and any potential solutions you have considered.

### Feature Requests

We would love to hear from you about new features which would improve this app and further the aims of our project. Please provide as much detail and information as possible to show us why you think your new feature should be implemented.

### Pull Requests

If you have developed a patch, bug fix, or new feature that would improve this app, please submit a pull request. It is best to communicate your ideas with the developers first before investing a great deal of time into a pull request to ensure that it will mesh smoothly with the project.

Remember that this project is licensed under the MIT license, and by submitting a pull request, you agree that your work will be, too.

#### Pull Request Guidelines

- Ensure any install or build dependencies are removed before the end of the layer when doing a build.
- Update the README.md with details of changes to the interface, including new plist variables, exposed ports, useful file locations and container parameters.
- Ensure that your code conforms to our existing code conventions and test coverage.
- Include the relevant issue number, if applicable.
- You may merge the Pull Request in once you have the sign-off of two other developers, or if you do not have permission to do that, you may request the second reviewer to merge it for you.

### Attribution

These contribution guidelines have been adapted from [this good-Contributing.md-template](https://gist.github.com/PurpleBooth/b24679402957c63ec426).
