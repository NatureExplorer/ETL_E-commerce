import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import dag, task

SCRIPTS_DIR = Path("/opt/airflow/scripts")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from Bronze_layer import ingest_customer_to_bronze, ingest_orderitems_to_bronze ,ingest_order_to_bronze ,ingest_payments_to_bronze ,ingest_Products_to_bronze
from Silver_layer import table_silver_transaction, silver_transform_customers, silver_transform_orders, silver_transform_orders_item, silver_transform_products, silver_transform_payments
from Gold_layer import agregated_order_bytime, agregated_revenue_bycategory, ratio_shipandprice_bycity
from load_db import gold_to_db


@dag(
    dag_id="E-Commerce Medallion Architecture",
    start_date=datetime(2026, 11, 9),
    schedule=timedelta(days=1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["ETL", "Medallion Architecture", "E-Commerce"],
)
def orders_medallion_pipe():

    @task()
    def bronze_layer():
        ingest_customer_to_bronze()
        ingest_orderitems_to_bronze()
        ingest_order_to_bronze()
        ingest_payments_to_bronze()
        ingest_Products_to_bronze()
        

    @task()
    def silver_layer():
        silver_transform_customers()
        silver_transform_orders()
        silver_transform_orders_item()
        silver_transform_products()
        silver_transform_payments()
        table_silver_transaction()

    @task()
    def gold_layer():
        agregated_order_bytime()
        agregated_revenue_bycategory()
        ratio_shipandprice_bycity()

    @task()
    def load_to_db():
        gold_to_db()


    silver_layer() >> gold_layer() >> load_to_db()

orders_medallion_pipe()
