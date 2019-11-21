# Cryptolytic

You can find the project at [Cryptolytic](http://www.cryptolyticapp.com/).

## Contributers
### Team Lead
 |                                       [Stanley Kusmier](https://github.com/standroidbeta)                                        |
| :-----------------------------------------------------------------------------------------------------------: |
|                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Stan.png?raw=true" width = "200" />](https://github.com/standroidbeta)                       |
|                 [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/standroidbeta)                 |
| [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/stanley-kusmier-0563a153/) |
### Data Science


|                                       [Alfredo Quintana](https://github.com/alqu7095)                                        |                                       [Elizabeth Ter Sahakyan](https://www.github.com/elizabethts)                                        |                                       [Marvin A Davila](https://github.com/MAL3X-01)                                        |                                       [Nathan Van Wyck](https://github.com/nrvanwyck)                                        |                                       [Taylor Bickell](https://github.com/tcbic)                                        |
| :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: | :-----------------------------------------------------------------------------------------------------------: |
|                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Alfredo.jpeg?raw=true" width = "200" />](https://github.com/alqu7095)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Elizabeth.jpeg?raw=true" width = "200" />](https://www.github.com/elizabethts)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Marvin.jpeg?raw=true" width = "200" />](https://github.com/MAL3X-01)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Nathan.png?raw=true" width = "200" />](https://github.com/nrvanwyck)                       |                      [<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/Taylor.jpeg?raw=true" width = "200" />](https://github.com/tcbic)                       |
|                 [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/alqu7095)                 |            [<img src="https://github.com/favicon.ico" width="15"> ](https://www.github.com/elizabethts)             |           [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/MAL3X-01)            |          [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/nrvanwyck)           |            [<img src="https://github.com/favicon.ico" width="15"> ](https://github.com/tcbic)             |
| [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/alfredo-quintana-98248a76/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/elizabethts) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/marvin-davila/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/nathan-van-wyck-48586718a/) | [ <img src="https://static.licdn.com/sc/h/al2o9zrvru7aqj8e1x2rzsrca" width="15"> ](https://www.linkedin.com/in/taylorbickell/) |




![MIT](https://img.shields.io/packagist/l/doctrine/orm.svg)
![Typescript](https://img.shields.io/npm/types/typescript.svg?style=flat)
[![Netlify Status](https://api.netlify.com/api/v1/badges/b5c4db1c-b10d-42c3-b157-3746edd9e81d/deploy-status)](netlify link goes in these parenthesis)
[![code style: prettier](https://img.shields.io/badge/code_style-prettier-ff69b4.svg?style=flat-square)](https://github.com/prettier/prettier)

🚫 more info on using badges [here](https://github.com/badges/shields)

## Project Overview


[Trello Board](https://trello.com/invite/b/HY6gCjh2/3f8eb169fd2f1e2415abf535c20accb3/labs17-cryptolytic)

[Product Canvas](https://www.notion.so/e563b27ab8e94ce2a3f7b536fc365715?v=3781e3eb9e72447f9262ebacd1e21fa9)


Cryptolytic is a platform for beginners tinkering with cryptocurrency to the seasoned trader. It provides you with recommendations on when to buy and sell based on technical indicators and assesses the markets to predict arbitrage opportunities before they even happen.


### Figma Prototype 
<img src="https://github.com/alqu7095/Cryptolytic_README/blob/master/cryptolytic_thumbnail.png?raw=true" width = "500" />

### Tech Stack

Python, AWS, PostgreSQL, SQL, Flask

### Predictions

The Models folder contains two zip files. tr_pickles.zip contains nine pickled trade recommender models. arb_models.zip contains 21 arbitrage models.


The following nine RandomForestClassifier models are contained in tr_pickles.zip.


bitfinex_btc_usd.pkl

bitfinex_eth_usd.pkl

bitfinex_ltc_usd.pkl

coinbase_pro_btc_usd.pkl

coinbase_pro_eth_usd.pkl

coinbase_pro_ltc_usd.pkl

hitbtc_btc_usdt.pkl

hitbtc_eth_usdt.pkl

hitbtc_ltc_usdt.pkl


The following twenty-one RandomForestClassifier models are contained in arb_models.zip.


bitfinex_coinbase_pro_bch_btc.pkl

bitfinex_coinbase_pro_bch_usd.pkl

bitfinex_coinbase_pro_etc_usd.pkl

bitfinex_coinbase_pro_eth_btc.pkl

bitfinex_coinbase_pro_ltc_btc.pkl

bitfinex_coinbase_pro_ltc_usd.pkl

bitfinex_gemini_bch_btc.pkl

bitfinex_gemini_ltc_btc.pkl

bitfinex_hitbtc_bch_usdt.pkl

bitfinex_hitbtc_eos_usdt.pkl

bitfinex_hitbtc_eth_btc.pkl

bitfinex_hitbtc_ltc_btc.pkl

bitfinex_hitbtc_ltc_usdt.pkl

coinbase_pro_gemini_bch_btc.pkl

coinbase_pro_gemini_ltc_btc.pkl

coinbase_pro_hitbtc_bch_btc.pkl

coinbase_pro_hitbtc_eth_usdc.pkl

gemini_hitbtc_bch_btc.pkl

gemini_hitbtc_ltc_btc.pkl

kraken_gemini_bch_btc.pkl

kraken_gemini_ltc_btc.pkl


### Features

-   Features 1
-   Features 2
-   Features 3
-   Features 4
-   Features 5

### Data Sources
🚫  Add to or delete souce links as needed for your project


-   [Source 1] (🚫add link to python notebook here)
-   [Source 2] (🚫add link to python notebook here)
-   [Source 3] (🚫add link to python notebook here)
-   [Source 4] (🚫add link to python notebook here)
-   [Source 5] (🚫add link to python notebook here)

### Python Notebooks

🚫  Add to or delete python notebook links as needed for your project

[Python Notebook 1](🚫add link to python notebook here)

[Python Notebook 2](🚫add link to python notebook here)

[Python Notebook 3](🚫add link to python notebook here)

## How to connect to the Cryptolytic API
 http://www.cryptolyticapp.com/

### Trade API [/trade](http://www.cryptolyticapp.com/trade)

 Method: ["GET"]

 Returns: ``` {"results":
"{('exchange', 'trading_pair'): [{
'p_time': 'time',
‘period’: ‘minutes’,
'prediction': 'result'}], }"} ```
  
### Arbitrage API [/arbitrage](http://www.cryptolyticapp.com/arbitrage)
  
Method: ["GET"]

Returns: ``` {"results":"{
('exchange_1', 'exchange_2', 'trading_pair'): [
{'p_time': 'time',
'prediction': 'result'}
]} ```


### How to connect to the data API

🚫 List directions on how to connect to the API here

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

## Documentation

See [Backend Documentation](_link to your backend readme here_) for details on the backend of our project.

See [Front End Documentation](_link to your front end readme here_) for details on the front end of our project.

