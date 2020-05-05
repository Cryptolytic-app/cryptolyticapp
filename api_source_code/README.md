# Flask App Documentation <img align="right" src="https://www.clipartkey.com/mpngs/m/145-1450071_flask-python-logo-transparent.png" width="120" height="120">


[Current Deployment Link](http://cryptolytic-env.niu7nzrmmi.us-east-1.elasticbeanstalk.com)
Work in progress

[API Documentation](http://cryptolytic-env.niu7nzrmmi.us-east-1.elasticbeanstalk.com/api)

# Table of Contents
1. [Quick Rundown](#sum)
2. [Flask App Endpoints](#endpoints)
3. [Run Locally](#local)
4. [Deployment on AWS EB](#deployment)
    * [GUI Upload](#gui)
    * [CLI Upload](#cli)
5. [Built With](#dependency)
6. [What's Next](#next)

## Quick Rundown  <a name="sum"></a>

The flask app allows user to request a json format of crypto prediction data from the api endpoints or display prediction 
results based on user input. There's an user friendly front end for user that was built with `html`.  

Two type of predictions:
- Trading Price Movement Predictions 
- Arbitrage Predictions

Our application has six routes with rate limiting and depends on two `.py` files. 
- config.py - gets environment variable for aws credentials(`.env`)
- utils.py - functions that retrieves prediction data from a AWS RDS Database


## Flask App Endpoints <a name="endpoints"></a>

### Homepage - [/]
A `homepage` render template
 
### API Documentation - [/api]
A `api` doc render template page

### Trade Prediction - [/trade_rec]
Returns this endpoint if user requested trade predictions

Method: ["GET", "POST"]

### Arbitrage Prediction - [/arb]
Returns this endpoint if user requested arbitrage predictions

Method: ["GET", "POST"]

### Trade API - [/trade]
Returns a json result of all available trade predictions

Method: ["GET"]

Rate Limit: 10 per minute by IP Address

 Returns: ``` {"results":
"{('exchange', 'trading_pair'): [{
'p_time': 'time',
‘period’: ‘minutes’,
'prediction': 'result'}], }"} ```
  
### Arbitrage API - [/arbitrage]
Returns a json result of all available arbitrage predictions

Method: ["GET"]

Rate Limit: 60 per minute by IP Address

Returns: ``` {"results":"{
('exchange_1', 'exchange_2', 'trading_pair'): [
{'p_time': 'time',
'prediction': 'result'}
]} ```

## Run Locally <a name="local"></a>
>Note: Won't work correctly without the same data in your RDS Database but will be able to see application without predictions.

[How to set up a PostgreSQL RDS DB](https://towardsdatascience.com/how-to-set-up-a-postgresql-database-on-amazon-rds-64e8d144179e)

Missing: `.env` - Hidden AWS Credentials

If you have the data readily available in a DB, create `.env` file and add your credentials corresponding to `config.py`

#### Step 1:
Clone this repo 
Git Clone: `https://github.com/Cryptolytic-app/cryptolytics.git` 
#### Step 2:
Open terminal and go to your clone repo directory location. 
#### Step 3:
Use a virtual environment with the same dependencies in `requirements.txt`
#### Step 4:
Run `python application.py`

Server will be and accessible at http://127.0.0.1:5000/ 

## Deployment Instruction for AWS Elastic Beanstalk <a name="deployment"></a>

### GUI Upload <a name="gui"></a>
>Note: Using MacOS

[Elastic Beanstalk Upload Guide](https://medium.com/analytics-vidhya/deploying-a-flask-app-to-aws-elastic-beanstalk-f320033fda3c)

#### Step 1:
Zip all flask files including hidden folder `.ebextensions`,`.env` file and `requirements.txt`. A total of 8 items should be zipped.

#### Step 2: 
Go to terminal and go to your folder where the zip flask file is in.
Run `zip -d flask_app.zip __MACOSX/\*` 

#### Step 3:
Go to your EB environment and upload your flask zip file

### CLI Upload <a name="cli"></a>
Coming Soon

## Built With <a name="dependency"></a>
* [flask](https://pypi.org/project/Flask/) - Application Framework
    * [flask-limiter](https://flask-limiter.readthedocs.io/en/stable/) - Flask extension to rate limit by ip address
* [psycopg2](https://pypi.org/project/psycopg2/) - PostgresSQL database adapter
* [AWS RDS](https://aws.amazon.com/rds/?nc2=h_ql_prod_fs_rds) - Flask queries a RDS DB
* [AWS Elastic BeanStalk](https://aws.amazon.com/elasticbeanstalk/) - Flask Deployment

## What's Next <a name="next"></a>
* Change trade recommender to only predict crypto currency price movement without exchange input
* Add more exchanges and trading pair options for arbitrage predictions
* Track top currency price change and display it with the predictions
* Notification system 
* Configure a flask endpoint for a trading bot and give instructions
