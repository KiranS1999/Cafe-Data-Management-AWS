"""File to connect to/access the db and extract/transform/load csv data """

import sys
sys.path.append('utils')
from postgres_utils import setup_db_connection, create_db_tables, create_engine_for_load_step, fetch_last_id, create_db_names, create_engine
from extract import extract_clean_data 
from normalise import third_nf


#### SET UP CONNECTION ####
engine = create_engine_for_load_step() 
connection, cursor = setup_db_connection()
connection, cursor = create_db_names(connection, cursor)
create_db_tables(connection, cursor) 
last_id = fetch_last_id(connection,cursor)


### EXTRACT ###
data_norm = extract_clean_data('chesterfield_25-08-2021_09-00-00.csv')

### TRANSFORM ###
transaction_df, product_df, order_df = third_nf(data_norm)
product_df=product_df.drop(columns='id')

transaction_df.to_csv('transaction.csv', index=False)
product_df.to_csv('product.csv', index=False)
order_df.to_csv('order.csv', index=False)

### LOAD ###
transaction_df.to_sql('transaction_data', engine, schema='public', index=False, if_exists='append')
print('Appended transaction data to the transaction table')

product_df.to_sql('product_data', engine, schema='public', index=False, if_exists='append')
print('Appended product data to the transaction table')

order_df.to_sql('order_data', engine, schema='public', index=False, if_exists='append')
print('Appended order data to the order table')
