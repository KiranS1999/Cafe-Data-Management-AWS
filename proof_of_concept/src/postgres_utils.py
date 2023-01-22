"""Functions to create the db connection, db name (cafe_db), db tables(transaction,product,order) 
and to fetch the last id (ensure unique ids for multiple csv entries)"""

import psycopg2 
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

load_dotenv()
HOST = os.environ.get("postgresql_host")
USER = os.environ.get("postgresql_user")
PASSWORD = os.environ.get("postgresql_pass")
WAREHOUSE_DB_NAME = os.environ.get("postgresql_db")


DB_DATA = 'postgresql+psycopg2://' + USER + ':' + PASSWORD + '@' + 'localhost' + ':5432/' \
       + WAREHOUSE_DB_NAME 

print('Connecting to the PostgreSQL database...')

def setup_db_connection():
    """establishes connection to database

    Returns:
        connection: the object to connect to database
        cursor: the cursor to work on the database with
    """
    connection = psycopg2.connect(host=HOST, user=USER, password=PASSWORD, port=5432)
    cursor = connection.cursor()
    return connection, cursor

def create_db_names(connection, cursor):
    """creates databases

    Args:
        connection (psycopg2 connection): database connection
        cursor (psycopg2 cursor): the connections cursor

    Returns:
        connection,cursor: the new connection and its cursor
    """
    connection.autocommit = True
    drop_database = f'''DROP DATABASE IF EXISTS {WAREHOUSE_DB_NAME}''' #should delete
    create_database = f'''CREATE DATABASE {WAREHOUSE_DB_NAME}''';
    cursor.execute(drop_database) 
    cursor.execute(create_database) 
    connection = psycopg2.connect(host=HOST, user=USER, database=WAREHOUSE_DB_NAME, password=PASSWORD, port=5432)
    cursor = connection.cursor()
    return connection, cursor

def create_db_tables(connection, cursor):
    """makes the tables in the database

    Args:
        connection (psycopg2 connection): database connection
        cursor (psycopg2 cursor): that connections cursor
    """    

    create_product_table = \
    """
        CREATE TABLE IF NOT EXISTS product_data(
            id SERIAL,
            product_name VARCHAR(200),
            Product_flavour VARCHAR(200),
            Price numeric(10,2),
            PRIMARY KEY(id)
        );
    """    

    create_transaction_table= \
    """
        CREATE TABLE IF NOT EXISTS transaction_data(
            id SERIAL,
            date DATE,
            time TIME,
            branch_location VARCHAR(200),
            total_spend numeric(10,2),
            payment_method VARCHAR(200),
            primary key(id)     
        );
    """

    create_order_table = \
    """
        CREATE TABLE IF NOT EXISTS order_data(
            id SERIAL,
            transaction_id INT,
            product_id INT,
            PRIMARY KEY(id),
            FOREIGN KEY(transaction_id) REFERENCES transaction_data(id),
            FOREIGN KEY(product_id) REFERENCES product_data(id)
        );
    """
    cursor.execute(create_transaction_table)
    cursor.execute(create_product_table)
    cursor.execute(create_order_table)
    
    connection.commit()

def fetch_last_id(connection, cursor):
    """fetches the id of the last transaction to reference foreign keys without being in the database

    Args:
        connection (psycopg2 connection): database connection
        cursor (psycopg2 cursor): the connections curosr for interfacing

    Returns:
        last_id(int): the id value of the last item in the transation_data table
    """    
    try: 
        last_id = """
        SELECT id FROM transaction_data ORDER BY id DESC LIMIT 1
        """
        cursor.execute(last_id)
        last_id = cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()
        return last_id[0][0]
    except IndexError:
        return 0    

def create_engine_for_load_step(db_data=DB_DATA):
    """calls sql alchemy function
    """
    return create_engine(db_data)


print('[+] Done ')
