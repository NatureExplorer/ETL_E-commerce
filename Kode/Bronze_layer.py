import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path("/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


bronze_path = Path("/opt/airflow/data/bronze")
bronze_path.mkdir(parents=True, exist_ok=True)

def ingest_customer_to_bronze():
    customer_train = pd.read_csv('/opt/airflow/data/train/df_Customers.csv')
    customer_test = pd.read_csv('/opt/airflow/data/test/df_Customers.csv')

    df_customer = pd.concat([customer_train,customer_test],ignore_index=True)
    print('===ingest and concat customer data done===')
    return df_customer.to_csv('/opt/airflow/data/bronze/df_Customers')

def ingest_orderitems_to_bronze():
    orderitems_train = pd.read_csv('/opt/airflow/data/train/df_OrderItems.csv')
    orderitems_test = pd.read_csv('/opt/airflow/data/test/df_OrderItems.csv')

    df_orderitems = pd.concat([orderitems_train,orderitems_test],ignore_index=True)
    print('===ingest and concat order items data done===')
    return df_orderitems.to_csv('/opt/airflow/data/bronze/df_OrderItems')

def ingest_order_to_bronze():
    order_train = pd.read_csv('/opt/airflow/data/train/df_Orders.csv')
    order_test = pd.read_csv('/opt/airflow/data/test/df_Orders.csv')

    df_order = pd.concat([order_train,order_test],ignore_index=True)
    print('===ingest and concat order data done===')
    return df_order.to_csv('/opt/airflow/data/bronze/df_Orders')

def ingest_payments_to_bronze():
    payments_train = pd.read_csv('/opt/airflow/data/train/df_Payments.csv')
    payments_test = pd.read_csv('/opt/airflow/data/test/df_Payments.csv')

    df_payments = pd.concat([payments_train,payments_test],ignore_index=True)
    print('===ingest and concat payments data done===')
    return df_payments.to_csv('/opt/airflow/data/bronze/df_Payments')

def ingest_Products_to_bronze():
    Products_train = pd.read_csv('/opt/airflow/data/train/df_Products.csv')
    Products_test = pd.read_csv('/opt/airflow/data/test/df_Products.csv')

    df_Products = pd.concat([Products_train,Products_test],ignore_index=True)
    print('===ingest and concat Products data done===')
    return df_Products.to_csv('/opt/airflow/data/bronze/df_Products')


