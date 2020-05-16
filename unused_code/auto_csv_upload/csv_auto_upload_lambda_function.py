import psycopg2 as ps
import boto3
import pandas as pd
from io import StringIO


'''
Gathering DB new data to Export updated CSV to S3 Bucket!
'''

# Add more exchanges/trading pair if needed
coinbase_pro_pairs = ['btc_usd', 'eth_usd', 'ltc_usd']
bitfinex_pairs = ['btc_usd', 'eth_usd', 'ltc_usd']
hitbtc_pairs = ['btc_usdt', 'eth_usdt', 'ltc_usdt']

bitfinex_table_list = ['bitfinex_' + pair for pair in bitfinex_pairs]

coinbase_pro_table_list = ['coinbase_pro_' + pair for pair in
                           coinbase_pro_pairs]

hitbtc_table_list = ['hitbtc_' + pair for pair in hitbtc_pairs]

table_names = bitfinex_table_list + coinbase_pro_table_list + hitbtc_table_list

# Connection to S3 bucket
s3 = boto3.resource('s3')
print("Connected to S3")

# Add your credentials - Don't push to GH
credentials = {'POSTGRES_ADDRESS' : '',
               'POSTGRES_PORT' : '',
               'POSTGRES_USERNAME' : '',
               'POSTGRES_PASSWORD' : '',
               'POSTGRES_DBNAME' : ''}
print("credentials stored")


def retrieve_data(table_name, schema, h_or_ml):

    """Retrieves data from a database where a connection is already established.
        Returns the whole table."""

    conn = ps.connect(host=credentials['POSTGRES_ADDRESS'],
                      database=credentials['POSTGRES_DBNAME'],
                      user=credentials['POSTGRES_USERNAME'],
                      password=credentials['POSTGRES_PASSWORD'],
                      port=credentials['POSTGRES_PORT'])

    cur = conn.cursor()
    

    # retreive all available data for historical CSVs
    if h_or_ml == 'h':
        
        cur.execute("""SELECT * FROM {schema}.{table_name}""".format(table_name=table_name, schema=schema))
        
        result = cur.fetchall()[::-1]

        # creating result to df and renaming columns
        result = pd.DataFrame(result)
        result = result.rename(
        columns={0: 'closing_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'base_volume'})
    
        # close connection
        conn.close()
    
        return result
        
    # retrieve a limited amount or data for model retraining CSVs
    else:

        # Change limit number to whatever amount of rows you want to retrieve
        cur.execute("""SELECT * FROM {schema}.{table_name}
                       ORDER BY closing_time DESC LIMIT 500""".format(table_name=table_name, schema=schema))

        result = cur.fetchall()

        # creating result to df and renaming columns
        result = pd.DataFrame(result)
        result = result.rename(
        columns={0: 'closing_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'base_volume'})
        
        # close connection
        conn.close()
    
        return result

def bucketdir(h_or_ml):
    """ 
    Look thru S3 directory structure and return useful names, version # 
    to make new folders for uploading

    """

    s3 = boto3.resource('s3')

    # return folder name for historical data one hour/ 
    if h_or_ml == 'h':

        # Grabbing all Buckets in S3
        for bucket in s3.buckets.all():
            # iterating thru objects in bucket 
            for obj in bucket.objects.filter():
                # grabbing the correct bucket 
                if bucket.name == 'your-bucket-name':
                    # split object to find folder name
                    if obj.key.split('/')[1] == 'onehourohlcv':
                        #print('{0}:{1}'.format(bucket.name, obj.key))

                        # save object (folder name)
                        onehourohlcv = obj.key.split('/')[0:2]

                    # split object to find folder name
                    if obj.key.split('/')[1] == 'fiveminuteohlcv':
                        
                        # save object (folder name)
                        fiveminuteohlcv = obj.key.split('/')[0:2]
                        
        return onehourohlcv, fiveminuteohlcv
        
    # returning folder structure for mldata csv and make a new version #
    else:

        # Grabbing all Buckets in S3
        for bucket in s3.buckets.all():
            # iterating thru objects in bucket 
            for obj in bucket.objects.filter():
                # grabbing the correct bucket
                if bucket.name == 'your-bucket-name':
                    # split object to find folder name
                    if obj.key.split('/')[1] == 'trp' and obj.key.split('/')[0] == 'mldata':
                       # print('{0}:{1}'.format(bucket.name, obj.key))

                        onehourohlcv = obj.key.split('/')[0:2]
                        # get version # of folder
                        n = obj.key.split('/')[2][1:]
                    
                    if obj.key.split('/')[1] == 'arp':
                        # get version # of folder
                        n1 = obj.key.split('/')[2][1:]
                    
                        fiveminuteohlcv = obj.key.split('/')[0:2]
        
        # add 1 to version number to create new folder                
        n = str(int(n)+1)
        n1 = str(int(n1)+1)
        
        return(onehourohlcv, fiveminuteohlcv, n, n1)
        
   
def upload_historical_data(filename):
    """ Function to upload updated historical CSVs """
    
    # connect to S3 Bucket
    s3_resource = boto3.resource('s3')
    
    # use function to return folder names (historical)
    onehourohlcv, fiveminuteohlcv = bucketdir('h')
    
    candles = [onehourohlcv, fiveminuteohlcv]
    schema = ['onehour', 'fiveminute']
    
    # loop to get correct candles and names to upload in S3 dir
    for s in schema:
        for name in filename:
            if s == 'onehour':
                t = '3600'
                c = candles[0]
            else:
                t = '300'
                c = candles[1]
                    
            # retrieve historical data function
            df = retrieve_data(name, s, 'h')

            # create df to csv in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)

            # Upload csv to the S3 Bucket in the correct folder
            s3_resource.Object('your-bucket-name', ("{ohlcv1}/{ohlcv2}/{name}_{t}.csv").format(
            ohlcv1=c[0],ohlcv2=c[1], name=name, t=t)).put(Body=csv_buffer.getvalue())
            
            # print what is getting uploaded
            print(("{ohlcv1}/{ohlcv2}/{name}_{t}.csv").format(ohlcv1=c[0],ohlcv2=c[1], name=name, t=t), 'Historical CSV Added')
            

def upload_ml_data(filename):
    """ Function to upload ml data CSVs """

    # connect to S3 Bucket 
    s3_resource = boto3.resource('s3')
    
    # use function to return folder names (machine learning)
    onehourohlcv, fiveminuteohlcv, n, n1 = bucketdir('ml')
    
    candles = [onehourohlcv, fiveminuteohlcv]
    schema = ['onehour', 'fiveminute']
    
    # loop to get the correct candles and names to upload in S3 dir
    for s in schema:
        for name in filename:
            # Historical Data
            if s == 'onehour':
                t = '3600'
                c = candles
                c = c[0]
                n = n
            else:
                t = '300'
                c = candles
                c = [c[0][0], c[1][1]]
                n = n1

            # retrieve ML data function
            df = retrieve_data(name, s, 'ml')

            # create df to csv in memory
            csv_buffer = StringIO()
            df.to_csv(csv_buffer)

            # Upload csv to the S3 Bucket in the correct folder
            s3_resource.Object('your-bucket-name', ("{ohlcv1}/{ohlcv2}/V{n}/{name}_{t}.csv").format(
            ohlcv1=c[0],ohlcv2=c[1], name=name, t=t, n=n)).put(Body=csv_buffer.getvalue())
            
            # Print what is getting uploaded
            print(("{ohlcv1}/{ohlcv2}/V{n}/{name}_{t}.csv").format(ohlcv1=c[0],ohlcv2=c[1], name=name, t=t, n=n), 'ML CSV Added')
                
        
def lambda_handler(event, context):
    upload_historical_data(table_names)
    upload_ml_data(table_names)
    return 'Success'
