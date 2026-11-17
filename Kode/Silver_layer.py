import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path("/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

silver_path = Path("/opt/airflow/data/silver")
silver_path.mkdir(parents=True, exist_ok=True)

def silver_transform_customers():
    # Load the bronze customers data
    bronze_customers = pd.read_csv('/opt/airflow/data/bronze/df_Customers.csv')

    # Perform transformations to create the silver customers data
    silver_customers = bronze_customers.copy()
    print('before transformation')
    for i in silver_customers.columns:
        if silver_customers[i].isnull().sum()>0:
            print(f"column {silver_customers[i]} have missing value")
        else:
            print(f"column {silver_customers[i]} no have missing value")

    print(f"how many duplicated before transformation:{silver_customers['customer_id'].duplicated().sum()}")

    silver_customers = silver_customers.dropna()
    silver_customers = silver_customers.drop(columns=['customer_zip_code_prefix'])
    silver_customers['customer_id'] = silver_customers['customer_id'].astype(str).drop_duplicates()
    silver_customers['customer_city'] = silver_customers['customer_city'].astype(str)
    silver_customers['customer_state'] = silver_customers['customer_state'].astype(str)

    print('after transformation')
    print(f"how many duplicated after transformation:{silver_customers['customer_id'].duplicated().sum()}")
    for i in silver_customers.columns:
        if silver_customers[i].isnull().sum()>0:
            print(f"column {silver_customers[i]} have missing value")
        else:
            print(f"column {silver_customers[i]} no have missing value")
    

    print("=============================done transformation silver customer=============================")
    return silver_customers

def silver_transform_orders():
    # Load the bronze orders data
    bronze_orders = pd.read_csv('/opt/airflow/data/bronze/df_Orders.csv')

    # transformations to create the silver orders data
    silver_orders = bronze_orders.copy()

    print('before transformation')
    for i in silver_orders.columns:
        if silver_orders[i].isnull().sum()>0:
            print(f"column {silver_orders[i]} have missing value")
        else:
            print(f"column {silver_orders[i]} no have missing value")

    print(f"how many duplicated data in order_id before transformation:{silver_orders['order_id'].duplicated().sum()}")


    silver_orders = silver_orders.dropna()
    silver_orders['order_id'] = silver_orders['order_id'].astype(str).drop_duplicates()
    silver_orders['customer_id'] = silver_orders['customer_id'].astype(str)
    silver_orders['order_status'] = silver_orders['order_status'].astype(str)

    # convert from y-m-d h:m:s to month-year format
    silver_orders['order_purchase_timestamp'] = pd.to_datetime(silver_orders['order_purchase_timestamp']).dt.date
    silver_orders['order_approved_at'] = pd.to_datetime(silver_orders['order_approved_at']).dt.date
    silver_orders['order_estimated_delivery_date'] = pd.to_datetime(silver_orders['order_estimated_delivery_date']).dt.date
    silver_orders['order_delivered_timestamp'] = pd.to_datetime(silver_orders['order_delivered_timestamp']).dt.date


    print('after transformation')
    print(f"how many duplicated data in order_id after transformation:{silver_orders['order_id'].duplicated().sum()}")
    for i in silver_orders.columns:
        if silver_orders[i].isnull().sum()>0:
            print(f"column {silver_orders[i]} have missing value")
        else:
            print(f"column {silver_orders[i]} no have missing value")
    

    print("=============================done transformation silver orders=============================")


    # Save the silver orders data
    return silver_orders

def silver_transform_orders_item():
    # Load the bronze order items data
    bronze_order_items = pd.read_csv('/opt/airflow/data/bronze/df_OrderItems.csv')

    # Perform transformations to create the silver order items data
    silver_order_items = bronze_order_items.copy()

    print('before transformation')
    for i in silver_order_items.columns:
        if silver_order_items[i].isnull().sum()>0:
            print(f"column {silver_order_items[i]} have missing value")
        else:
            print(f"column {silver_order_items[i]} no have missing value")
    
    print(f"how many duplicated data in order_id before transformation:{silver_order_items['order_id'].duplicated().sum()}")


    silver_order_items = silver_order_items.dropna()
    silver_order_items['order_id'] = silver_order_items['order_id'].astype(str).drop_duplicates()
    silver_order_items['product_id'] = silver_order_items['product_id'].astype(str)
    silver_order_items['seller_id'] = silver_order_items['seller_id'].astype(str)
    silver_order_items['price'] = silver_order_items['price'].astype(float)
    silver_order_items['shipping_charges'] = silver_order_items['shipping_charges'].astype(float)

    print('after transformation')
    print(f"how many duplicated data in product_id after transformation:{silver_order_items['order_id'].duplicated().sum()}")
    for i in silver_order_items.columns:
        if silver_order_items[i].isnull().sum()>0:
            print(f"column {silver_order_items[i]} have missing value")
        else:
            print(f"column {silver_order_items[i]} no have missing value")
            
        
    print("=============================done transformation silver order items=============================")
    
    return silver_order_items

def silver_transform_products():
    # Load the bronze products data
    bronze_products = pd.read_csv('/opt/airflow/data/bronze/df_Products.csv')

    # Perform transformations to create the silver products data
    silver_products = bronze_products.copy()
    print('before transformation')
    for i in silver_products.columns:
        if silver_products[i].isnull().sum()>0:
            print(f"column {silver_products[i]} have missing value")
        else:
            print(f"column {silver_products[i]} no have missing value")
        
    print(f"how many duplicated data in order_id before transformation:{silver_products['product_id'].duplicated().sum()}")
    
    #delete column that are not needed
    silver_products = silver_products.drop(columns=['product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm'])

    silver_products = silver_products.dropna()
    silver_products['product_id'] = silver_products['product_id'].astype(str)
    silver_products['product_category_name'] = silver_products['product_category_name'].astype(str)

    print('after transformation')
    print(f"how many duplicated data in product_id after transformation:{silver_products['product_id'].duplicated().sum()}")
    for i in silver_products.columns:
        if silver_products[i].isnull().sum()>0:
            print(f"column {silver_products[i]} have missing value")
        else:
            print(f"column {silver_products[i]} no have missing value")
        
    
    print("=============================done transformation silver products=============================")
    return silver_products

def silver_transform_payments():
    # Load the bronze payments data
    bronze_payments = pd.read_csv('/opt/airflow/data/bronze/df_Payments.csv')

    # Perform transformations to create the silver payments data
    silver_payments = bronze_payments.copy()
    print('before transformation')
    for i in silver_payments.columns:
        if silver_payments[i].isnull().sum()>0:
            print(f"column {silver_payments[i]} have missing value")
        else:
            print(f"column {silver_payments[i]} no have missing value")
            
    print(f"how many duplicated data in order_id before transformation:{silver_payments['order_id'].duplicated().sum()}")

    silver_payments = silver_payments.drop(columns=['payment_installments', 'payment_sequential'])

    silver_payments = silver_payments.dropna()
    silver_payments['order_id'] = silver_payments['order_id'].astype(str).drop_duplicates()
    silver_payments['payment_type'] = silver_payments['payment_type'].astype(str)
    silver_payments['payment_value'] = silver_payments['payment_value'].astype(float)

    print('after transformation')
    print(f"how many duplicated data in product_id after transformation:{silver_payments['order_id'].duplicated().sum()}")
    for i in silver_payments.columns:
        if silver_payments[i].isnull().sum()>0:
            print(f"column {silver_payments[i]} have missing value")
        else:
            print(f"column {silver_payments[i]} no have missing value")
            
        
        print("=============================done transformation silver payments=============================")
    return silver_payments


def table_silver_transaction():
    # Load the silver data
    silver_orders = silver_transform_orders()
    silver_order_items = silver_transform_orders_item()
    silver_product = silver_transform_products()
    silver_customers = silver_transform_customers()
    silver_payments = silver_transform_payments()

    # Merge the dataframes to create the silver transaction table
    silver_transaction = pd.merge(silver_orders, silver_order_items, on='order_id', how='inner')
    silver_transaction = pd.merge(silver_transaction, silver_product, on='product_id', how='inner')
    silver_transaction = pd.merge(silver_transaction, silver_customers, on='customer_id', how='inner')
    silver_transaction = pd.merge(silver_transaction, silver_payments, on='order_id', how='inner')

    print("=============================TRANSFORMATION LAYER IS DONE=============================")

    return silver_transaction.to_csv('/opt/airflow/data/silver/silver_transaction.csv', index=False)
