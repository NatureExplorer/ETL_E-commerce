import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path("/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

gold_path = Path("/opt/airflow/data/gold")
gold_path.mkdir(parents=True, exist_ok=True)


def agregated_order_bytime():
    data_transaction = pd.read_csv('/opt/airflow/data/silver/silver_transaction.csv')
    agg = (data_transaction.groupby('order_approved_at').agg(
        total_orders=('order_id', 'count'),
        total_revenue =('price', 'sum')
    ).reset_index())

    agg.to_csv('/opt/airflow/data/gold/aggregated_orders.csv', index=False)


def agregated_revenue_bycategory():
    data_transaction = pd.read_csv('/opt/airflow/data/silver/silver_transaction.csv')
    agg = (data_transaction.groupby('product_category_name').agg(
        total_orders=('order_id', 'count'),
        total_revenue =('price', 'sum')
    ).reset_index())

    agg.to_csv('/opt/airflow/data/gold/aggregated_revenue_bycategory.csv', index=False)



def ratio_shipandprice_bycity():
    data_transaction = pd.read_csv('/opt/airflow/data/silver/silver_transaction.csv')
    agg = (data_transaction.groupby('customer_state').agg(
        total_orders=('order_id', 'count'),
        total_revenue =('price', 'sum'),
        total_shipping_charges=('shipping_charges', 'sum')
    ).reset_index())

    agg['ratio_shipping_price'] = agg['total_shipping_charges'] / agg['total_revenue']

    agg.to_csv('/opt/airflow/data/gold/ratio_shipandprice_bycity.csv', index=False)