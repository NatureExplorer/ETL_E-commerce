import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow.decorators import dag, task

SCRIPTS_DIR = Path("/opt/airflow/scripts")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


@dag(
    dag_id="ETL_Medallion_Architecture",
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
        from Bronze_layer import ingest_customer_to_bronze,ingest_orderitems_to_bronze ,ingest_order_to_bronze ,ingest_payments_to_bronze ,ingest_Products_to_bronze
        ingest_customer_to_bronze()
        ingest_orderitems_to_bronze()
        ingest_order_to_bronze()
        ingest_payments_to_bronze()
        ingest_Products_to_bronze()
        

    @task()
    def silver_layer():
        from Silver_layer import table_silver_transaction
        table_silver_transaction()

    @task()
    def gold_layer():
        from Gold_layer import run_gold
        run_gold()

    @task()
    def load_to_db():
        from load_db import gold_to_db
        gold_to_db()
        
    bronze_layer() >> silver_layer() >> gold_layer() >> load_to_db()

orders_medallion_pipe()
