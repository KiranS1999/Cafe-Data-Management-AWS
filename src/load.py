import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import json 

#fetching .env from aws paramater store
ssm = boto3.client('ssm')
parameter = ssm.get_parameter(Name='Team4', WithDecryption=True)
secret_json = json.loads(parameter['Parameter']['Value'])
username = secret_json['username']
password = secret_json['password']
host = secret_json['host']
port = secret_json['port']

#Set up logger
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
s3 = boto3.client('s3')


def handler(event, context):
    LOGGER.info(f'Event structure: {event}')
    print(event["Records"][0]['messageAttributes']['bucket']['stringValue'])
    print(event["Records"][0]['messageAttributes']['bucket_key']['stringValue'])
    
    try: 
        s3_bucket = event["Records"][0]['messageAttributes']['bucket']['stringValue']
        print("Bucket name is {}".format(s3_bucket))
        s3_object = event["Records"][0]['messageAttributes']['bucket_key']['stringValue']
        print("Bucket key name is {}".format(s3_object))
         
        
    except Exception as e:
        print(e, 'Error!')
    
    file_type = [f'order-{s3_object}', f'product-{s3_object}', f'transaction-{s3_object}']
    
    obj_t = s3.get_object(Bucket=s3_bucket, Key= file_type[2])
    obj_p = s3.get_object(Bucket=s3_bucket, Key= file_type[1])
    obj_o = s3.get_object(Bucket=s3_bucket, Key= file_type[0])
    
    #csv to pandas
    transaction_df = pd.read_csv(obj_t['Body'])
    transaction_df['date']=pd.to_datetime(transaction_df['date'])
    
    product_df = pd.read_csv(obj_p['Body'])
    product_df['price']= product_df['price'].astype(str)
    
    order_df = pd.read_csv(obj_o['Body'])
    print('transaction', transaction_df.head)
    print('product', product_df.head)
    print('order', order_df.head)

    ### LOAD ###
    dbname = os.environ.get("dbname")
    conn = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')

    transaction_df.to_sql('transaction_data', conn, index=False, if_exists='append')
    product_df.to_sql('product_data', conn, index=False, if_exists='append')
    order_df.to_sql('order_data', conn, index=False, if_exists='append')           
    print('....executed....')
