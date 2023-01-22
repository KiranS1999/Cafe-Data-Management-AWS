import logging
import boto3
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
import os
import awswrangler as wr
from io import StringIO  

## EXTRACT ##
def extract_clean_data(filename):
    """take csv file and remove sensitive data

    Args:
        file_to_clean (string): path to file that would be cleaned

    Returns:
        Dataframe: file with senstitve information removed
    """
    
    df = pd.read_csv(filename['Body'], header=None)
    df.drop(df.columns[[2,6]], axis=1, inplace=True)
    return df

# Transform fucntions
def create_transaction_df(df):
    """creates transaction dataframe for 3NF

    Args:
        df (dataframe): cleaned dataframe

    Returns:
        transaction_df: dataframe with transaction data
    """
    transaction_df = df.copy()
    transaction_df.reset_index(drop=True, inplace=True)
    transaction_df.drop((df.columns[2]), axis=1, inplace=True)
    transaction_df.rename(columns = { 0:'purchase_date', 1:'branch_location', 4:'total_spend', 5:'payment_method'}, inplace=True)

    date_time = transaction_df.purchase_date.str.split(" +", expand=True)
    date_time.rename(columns = {0:'date', 1:'time'})
    date= date_time[0]
    time=date_time[1]
    transaction_df.insert(1, "time", time)
    transaction_df.insert(1, "date", date)
    transaction_df.drop(['purchase_date'], axis=1, inplace=True)
    transaction_df['date'] = pd.to_datetime(transaction_df['date'], dayfirst=True)
    return transaction_df

def create_product_df(df):
    """creates product info dataframe for 3NF

    Args:
        df (dataframe): cleaned dataframe

    Returns:
        product_df: product dataframe 
    """
    product_items = df.copy()
    product_items.drop(product_items.columns[[0, 1, 3, 4]], axis=1, inplace=True)
    products_list = product_items.values.tolist()
    product_df = pd.DataFrame(columns=['product_name', 'product_flavour', 'price'])

    for items in products_list:
        
        for sep_order in items:
            
            sep_order = sep_order.split(',')

            for item in sep_order: 
                product_info = item.split('-') 
    
                if len(product_info) == 3:
                    product, flavour, price = product_info
                    product = product.strip()
                    flavour = flavour.strip()
                    price = price.strip()
                    row = pd.Series({'product_name':product, 'product_flavour':flavour, 'price':price}) 
                    product_df = pd.concat([product_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
                
                elif len(product_info) == 2:
                    product, price = product_info
                    product = product.strip()
                    price = price.strip()
                    row = pd.Series({'product_name':product, 'product_flavour':'', 'price':price}) 
                    product_df = pd.concat([product_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
            
    
    product_df.drop_duplicates(subset=None, keep="first", inplace=True)
    return product_df

def create_order_df(df, product_df, last_transaction_id, last_product_id):
    """creates order info dataframe for 3NF

    Args:
        df (dataframe): cleaned dataframe

    Returns:
        order_df: order dataframe
    """
    dataframe_items = df.copy()
    dataframe_items.drop(dataframe_items.columns[[0, 1, 3, 4]], axis=1, inplace=True)
    products_list = dataframe_items.values.tolist()

    order_df = pd.DataFrame(columns=['transaction_id', 'product_name', 'product_flavour', 'price'])
    
    
    t_id = last_transaction_id + 1

    for items in products_list:
        
        for sep_order in items:
            
            sep_order = sep_order.split(',')

            for item in sep_order: 
                product_info = item.split('-') 

                if len(product_info) == 3:
                    product, flavour, price = product_info
                    product = product.strip()
                    flavour = flavour.strip()
                    price = price.strip()
                    row = pd.Series({'transaction_id':t_id,'product_name':product, 'product_flavour':flavour, 'price':price}) 
                    order_df = pd.concat([order_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
                
                elif len(product_info) == 2:
                    product, price = product_info
                    product=product.strip()
                    price = price.strip()
                    row = pd.Series({'transaction_id':t_id,'product_name':product, 'product_flavour':'', 'price':price}) 
                    order_df = pd.concat([order_df, pd.DataFrame([row], columns=row.index)]).reset_index(drop=True)
              
            t_id+=1
                
    p_id = last_product_id +1
    product_df.insert(0, 'product_id', range(p_id, p_id + len(product_df)))
    order_df = pd.merge(order_df, product_df, on=['product_name','product_flavour', 'price'], how='left')
    order_df.drop(order_df.columns[[1, 2, 3]], axis=1, inplace=True)
    return order_df

def third_nf(df, last_transaction_id, last_product_id):
    """normalises a dataframe to 3NF

    Args:
        df (dataframe): cleaned dataframe
    Returns:
        normalised (list): a list of dataframes that are in 3NF
    """
    
    transaction_df = create_transaction_df(df)
    product_df = create_product_df(df)
    order_df = create_order_df(df, product_df, last_transaction_id, last_product_id)
    return transaction_df, product_df, order_df



## DATABASE CONN and ID Fetch ##
def setup_db_connection(host, user, password, dbname):
    """establishes connection to database
    Returns:
        connection: the object to connect to database
        cursor: the cursor to work on the database with
    """
    print(host, user, password, dbname)
    connection = psycopg2.connect(host=host, user=user, password=password, port=5439, dbname=dbname)
    cursor = connection.cursor()
    return connection, cursor


def fetch_last_transaction_id(connection, cursor):
    """fetches the id of the last transaction to reference foreign keys without being in the database
    Args:
        connection (psycopg2 connection): database connection
        cursor (psycopg2 cursor): the connections curosr for interfacing
    Returns:
        last_id(int): the id value of the last item in the transation_data table
    """    
    try: 
        last_transaction_id = """
        SELECT id FROM transaction_data ORDER BY id DESC LIMIT 1
        """
        cursor.execute(last_transaction_id)
        last_transaction_id = cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()
        return last_transaction_id[0][0]
    except IndexError:
        return 0    

def fetch_last_product_id(connection, cursor):
    """fetches the id of the last product to reference foreign keys without being in the database
    Args:
        connection (psycopg2 connection): database connection
        cursor (psycopg2 cursor): the connections curosr for interfacing
    Returns:
        last_product_id(int): the id value of the last item in the product_data table
    """    
    try: 
        last_product_id = """
        SELECT id FROM product_data ORDER BY id DESC LIMIT 1
        """
        cursor.execute(last_product_id)
        last_product_id = cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()
        return last_product_id[0][0]
    except IndexError:
        return 0    

def copy_to_s3(client, df, bucket, filepath):
    csv_buf = StringIO()
    df.to_csv(csv_buf, header=True, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)
    print(f'Copy {df.shape[0]} rows to S3 Bucket {bucket} at {filepath}, Done!')


LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
s3 = boto3.client('s3')

def handler(event, context):
    LOGGER.info(f'Event structure: {event}')

    try: 
        s3_bucket = event["Records"][0]['s3']['bucket']['name']
        print("Bucket name is {}".format(s3_bucket))
        s3_object = event["Records"][0]["s3"]["object"]["key"]
        print("Bucket key name is {}".format(s3_object))
        obj = s3.get_object(Bucket=s3_bucket, Key= s3_object) 
  
    except Exception as e:
        print(e)
        print('Error!')

    ### EXTRACT ###
    data_norm = extract_clean_data(obj)

    ### TRANSFORM ###
    dbname = os.environ.get("dbname") 
    host = os.environ.get("host")
    user = os.environ.get("user")
    password=os.environ.get("password")
    port=5439
    connection, cursor = setup_db_connection(host, user, password, dbname)
    last_transaction_id = fetch_last_transaction_id(connection,cursor)
    connection, cursor = setup_db_connection(host, user, password, dbname)
    last_product_id = fetch_last_product_id(connection, cursor)
    transaction_df, product_df, order_df = third_nf(data_norm, last_transaction_id, last_product_id)
    product_df=product_df.drop(columns='product_id')
     
    ##df to csv##
    bucket=os.environ.get("bucket")
    s3_object=s3_object.replace('/', '-') #account for files in folders
    copy_to_s3(client=s3, df=transaction_df, bucket=bucket, filepath=f'transaction-{s3_object}')
    copy_to_s3(client=s3, df=product_df, bucket=bucket, filepath=f'product-{s3_object}')
    copy_to_s3(client=s3, df=order_df, bucket=bucket, filepath=f'order-{s3_object}')
    
    ##Send message to the FIFO Queue##
    sqs = boto3.client(service_name='sqs', endpoint_url='https://sqs.eu-west-1.amazonaws.com')
    queue_url = 'https://sqs.eu-west-1.amazonaws.com/239598709205/team4.fifo'


    csv_file = f'{s3_object}'
    resp = sqs.send_message(
        QueueUrl=queue_url,
        MessageAttributes={
            'bucket': {'DataType': 'String', 'StringValue': bucket },
            'bucket_key': {'DataType': 'String', 'StringValue': csv_file},
        },
        MessageBody=(
            csv_file
        ),
        MessageGroupId=csv_file
    )
    
