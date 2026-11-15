import pandas as pd
import psycopg2
from airflow.hooks.base import BaseHook
from pathlib import Path
import sys

SCRIPTS_DIR = Path("/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def gold_to_db():

    # Database connection parameters
    conn = BaseHook.get_connection('postgres_default')
    db_params = {
        'host': conn.host,
        'port': conn.port,
        'database': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    # Connect to the PostgreSQL database
    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    
    # Create the table if it doesn't exist
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS aggregated_orders (
            id SERIAL PRIMARY KEY,
            order_approved_at DATE,
            total_orders INT,
            total_revenue FLOAT
        );
        CREATE TABLE IF NOT EXISTS aggregated_revenue_bycategory (
            id SERIAL PRIMARY KEY,
            product_category_name VARCHAR,
            total_orders INT,
            total_revenue FLOAT
        );

        CREATE TABLE IF NOT EXISTS ratio_shipandprice_bycity (
            id SERIAL PRIMARY KEY,
            customer_state VARCHAR,
            total_orders INT,
            total_revenue FLOAT,
            total_shipping_charges FLOAT,
            ratio_shipping_price DECIMAL(7,3)
        );
    '''
    cursor.execute(create_table_query)

    # Insert data into the table
    try:
        tables = {
       'aggregated_orders': ['order_approved_at', 'total_orders', 'total_revenue'],
       'aggregated_revenue_bycategory': ['product_category_name', 'total_orders', 'total_revenue'],
       'ratio_shipandprice_bycity': ['customer_state', 'total_orders', 'total_revenue',
                                     'total_shipping_charges', 'ratio_shipping_price']}

        for table, cols in tables.items():
            path = f'/opt/airflow/data/gold/{table}.csv'
            with open(path, 'r') as f:
                cursor.copy_expert(
               f"COPY {table} ({', '.join(cols)}) FROM STDIN WITH (FORMAT csv, HEADER true)",f
           )

                
        connection.commit()


    except Exception:
       connection.rollback()
       raise

    
    finally:
       cursor.close()
       connection.close()


