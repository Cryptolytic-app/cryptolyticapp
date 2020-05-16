import boto3
from botocore.exceptions import ClientError
import psycopg2 as ps
import pandas as pd
import datetime as dt
import os

""" Lambda Function to retreive Email Alerts for current arbitrage predictions  """


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
    """ Creating DB Connection"""

    # creating connection
    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    # creating cursor
    cur = conn.cursor()
    
    return conn, cur
               
               
# Replace sender@example.com with your "From" address.
# This address must be verified with Amazon SES.
SENDER = "add_email"

# Replace recipient@example.com with a "To" address. If your account 
# is still in the sandbox, this address must be verified.
RECIPIENT = "add_email"

# Specify a configuration set. If you do not want to use a configuration
# set, comment the following variable, and the 
# ConfigurationSetName=CONFIGURATION_SET argument below.
#CONFIGURATION_SET = "ConfigSet"

# If necessary, replace us-east-1 with the AWS Region you're using for Amazon SES.
AWS_REGION = "us-east-1"

# The subject line for the email.
SUBJECT = "Arbitrage Alert"

# The email body for recipients with non-HTML email clients.
BODY_TEXT = ("Arbitrage in 5 Minutes\r\n"
             "This email was sent with Amazon SES using the "
             "an example Exchange 1 to exchange 2, trading_pair"
            )
            

# Create a new SES resource and specify a region.
client = boto3.client('ses',region_name=AWS_REGION)

def send_notification(results):
    """ Creates email and send alert """
    
    # the email main body html
    full_html = """<html>
        <head></head>
        <body>
          <h1>Arbitrage in 5 Minutes</h1>
          <p>
            <a href='https://pro.coinbase.com/'>Coinbase Pro</a>
            <br>
            <a href='https://www.kraken.com/en-us/'>Kraken</a>
            <br>
            <a href='https://gemini.com/'>Gemini</a>.</p>
        </body>
        </html> <br>"""
    
    # looping thru the result to add more text to the full_html body
    for result in results:
        
        # The HTML extra body of the email.
        BODY_HTML = """ time {time} <br> prediction {prediction} <br> exchange_1 {exchange_1} <br> exchange_2 {exchange_2}
                        <br> trading_pair {trading_pair}
                    <br> <br> <br>
                    """.format(time=result[0], prediction=result[1], exchange_1=result[2], exchange_2=result[3], trading_pair=result[4] ) 
        
        # Adding more html to the body depending on the result
        full_html = full_html + BODY_HTML
        
    # The character encoding for the email.
    CHARSET = "UTF-8"
    
    # Try to send the email.
    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Html': {
                        'Charset': CHARSET,
                        'Data': full_html,
                    },
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER,
            # If you are not using a configuration set, comment or delete the
            # following line
           # ConfigurationSetName=CONFIGURATION_SET,
        )
    # Display an error if something goes wrong.	
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])


def retrieve_arp_pred():
    """ Retrieves arbitrage predictions from DB to find arb opportunities """

    # create connection and cursor
    conn, cur = create_conn(credentials)

    # Gets last 500 prediction results from arp table
    cur.execute("""SELECT * FROM prediction.arp
                   ORDER by p_time desc limit 500;""")
    result = cur.fetchall()

    # creates dataframe from results and rename columns
    result = pd.DataFrame(result)
    result = result.rename(
        columns={0: 'p_time', 1: 'c_time', 2: 'exchange_1', 3: 'exchange_2', 4: 'trading_pair', 5: 'prediction'})

    # result = result.drop(columns='c_time')
    # result = result.drop_duplicates(subset=['exchange_1', 'exchange_2', 'trading_pair'])

    # converts p_time column to datetime 
    result['datetime'] = pd.to_datetime(result['p_time'])

    # create time threshold to 15 minutes, to only return results in the last 15 min
    # filters result to the last 15 min
    t = dt.datetime.now() - dt.timedelta(minutes=15)
    
    #result = result[(result['datetime'] > t) & (result['prediction'] != 'no_arbitrage')] 
    #result = result[result['prediction'] != 'no_arbitrage'] 
    
    result = result[(result['datetime'] > t)]
        
    # creates a list values to find arbitrage
    arb = result['prediction'].values
    
    # creates a list values to append results if arbitrage found
    e1 = result['exchange_1'].values
    e2 = result['exchange_2'].values
    t = result['p_time'].values
    pair = result['trading_pair'].values
    
    # counter for indexing into listS ^ 
    n = 0
    # empty list where appended arb results will go
    results = []
    
    # looping thru arb list to find arbitrage
    for a in arb:
        if a != 'no_arbitrage':
            a = a
            ex1 = e1[n]
            ex2 = e2[n]
            pt = t[n]
            p = pair[n]
            results.append([pt, a , ex1, ex2, p])
        else:
            print('no arb for index {n}'.format(n=n))
        n += 1
        
    # send function if there is an arbitrage result in list  
    if len(results) > 0:    
        return send_notification(results)


def lambda_handler(event, context):
    retrieve_arp_pred()
    return 'Success'