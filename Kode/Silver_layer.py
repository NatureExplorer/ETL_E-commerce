import json
import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path("/opt/airflow/data")

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

bronze_path = Path("/opt/airflow/data/bronze")
silver_path = Path("/opt/airflow/data/silver")
silver_path.mkdir(parents=True, exist_ok=True)

NULLABLE_COLUMNS = {"order_approved_at", "order_delivered_timestamp"}

QUARANTINE_COLUMNS = [
    "source_table",
    "key_column",
    "key_value",
    "row_index",
    "null_columns",
    "null_count",
    "row_data",
]


def read_bronze(file_name):
    df = pd.read_csv(bronze_path / file_name)
    return df.loc[:, ~df.columns.str.startswith("Unnamed")].copy()


def print_nulls(df, label):
    print(label)
    nulls = df.isna().sum()
    nulls = nulls[nulls > 0]
    if not nulls.empty:
        print("Any Null in Column:")
        print(nulls)


def split_null_rows(df, table, key_col):
    check_cols = [c for c in df.columns if c not in NULLABLE_COLUMNS]
    null_flags = df[check_cols].isna()
    mask = null_flags.any(axis=1)
    print(f"{table}: {int(mask.sum())} baris dikarantina dari {len(df)} baris")

    if not mask.any():
        return df.copy(), pd.DataFrame(columns=QUARANTINE_COLUMNS)

    bad = df[mask]
    flags = null_flags[mask]
    quarantine = pd.DataFrame(
        {
            "source_table": table,
            "key_column": key_col,
            "key_value": bad[key_col].astype("string"),
            "row_index": bad.index,
            "null_columns": flags.apply(lambda r: ",".join(r.index[r]), axis=1),
            "null_count": flags.sum(axis=1),
            "row_data": bad.apply(
                lambda r: json.dumps(
                    {k: (None if pd.isna(v) else v) for k, v in r.items()},
                    default=str,
                    ensure_ascii=False,
                ),
                axis=1,
            ),
        }
    ).reset_index(drop=True)

    return df[~mask].copy(), quarantine


def save_quarantine(frames):
    frames = [f for f in frames if not f.empty]
    if frames:
        quarantine = pd.concat(frames, ignore_index=True)
    else:
        quarantine = pd.DataFrame(columns=QUARANTINE_COLUMNS)

    quarantine.insert(0, "quarantined_at", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    quarantine.to_csv(silver_path / "quarantine_null_rows.csv", index=False)

    print("ringkasan karantina per tabel:")
    print(quarantine["source_table"].value_counts().to_string() if not quarantine.empty else "tidak ada baris dikarantina")


def silver_transform_customers():
    silver_customers = read_bronze("df_Customers.csv")
    print_nulls(silver_customers, "silver_customers before transformation")

    silver_customers = silver_customers.drop(columns=["customer_zip_code_prefix"])
    silver_customers, quarantine = split_null_rows(silver_customers, "customers", "customer_id")

    silver_customers = silver_customers.drop_duplicates(subset="customer_id")
    silver_customers["customer_id"] = silver_customers["customer_id"].astype(str)
    silver_customers["customer_city"] = silver_customers["customer_city"].astype(str)
    silver_customers["customer_state"] = silver_customers["customer_state"].astype(str)

    print_nulls(silver_customers, "silver_customers after transformation")
    print(f"how many duplicate data in customer_id after transformation:{silver_customers['customer_id'].duplicated().sum()}")
    print("=============================done transformation silver customer=============================")
    return silver_customers, quarantine


def silver_transform_orders():
    silver_orders = read_bronze("df_Orders.csv")
    print_nulls(silver_orders, "silver_orders before transformation")
    print(f"how many duplicated data in order_id before transformation:{silver_orders['order_id'].duplicated().sum()}")

    silver_orders, quarantine = split_null_rows(silver_orders, "orders", "order_id")

    silver_orders = silver_orders.drop_duplicates(subset="order_id")
    silver_orders["order_id"] = silver_orders["order_id"].astype(str)
    silver_orders["customer_id"] = silver_orders["customer_id"].astype(str)
    silver_orders["order_status"] = silver_orders["order_status"].astype(str)

    silver_orders["order_purchase_timestamp"] = pd.to_datetime(silver_orders["order_purchase_timestamp"]).dt.date
    silver_orders["order_approved_at"] = pd.to_datetime(silver_orders["order_approved_at"]).dt.date
    silver_orders["order_delivered_timestamp"] = pd.to_datetime(silver_orders["order_delivered_timestamp"]).dt.date
    silver_orders["order_estimated_delivery_date"] = pd.to_datetime(silver_orders["order_estimated_delivery_date"]).dt.date

    print_nulls(silver_orders, "silver_orders after transformation")
    print(f"how many duplicated data in order_id after transformation:{silver_orders['order_id'].duplicated().sum()}")
    print("=============================done transformation silver orders=============================")
    return silver_orders, quarantine


def silver_transform_orders_item():
    silver_order_items = read_bronze("df_OrderItems.csv")
    print_nulls(silver_order_items, "silver_order_items before transformation")
    print(f"how many duplicated data in order_id before transformation:{silver_order_items['order_id'].duplicated().sum()}")

    silver_order_items, quarantine = split_null_rows(silver_order_items, "order_items", "order_id")

    silver_order_items = silver_order_items.drop_duplicates(subset="order_id")
    silver_order_items["order_id"] = silver_order_items["order_id"].astype(str)
    silver_order_items["product_id"] = silver_order_items["product_id"].astype(str)
    silver_order_items["seller_id"] = silver_order_items["seller_id"].astype(str)
    silver_order_items["price"] = silver_order_items["price"].astype(float)
    silver_order_items["shipping_charges"] = silver_order_items["shipping_charges"].astype(float)

    print_nulls(silver_order_items, "silver_order_items after transformation")
    print(f"how many duplicated data in order_id after transformation:{silver_order_items['order_id'].duplicated().sum()}")
    print("=============================done transformation silver order items=============================")
    return silver_order_items, quarantine


def silver_transform_payments():
    silver_payments = read_bronze("df_Payments.csv")
    print_nulls(silver_payments, "silver_payments before transformation")
    print(f"how many duplicated data in order_id before transformation:{silver_payments['order_id'].duplicated().sum()}")

    silver_payments = silver_payments.drop(columns=["payment_installments", "payment_sequential"])
    silver_payments, quarantine = split_null_rows(silver_payments, "payments", "order_id")

    silver_payments = silver_payments.drop_duplicates(subset="order_id")
    silver_payments["order_id"] = silver_payments["order_id"].astype(str)
    silver_payments["payment_type"] = silver_payments["payment_type"].astype(str)
    silver_payments["payment_value"] = silver_payments["payment_value"].astype(float)

    print_nulls(silver_payments, "silver_payments after transformation")
    print(f"how many duplicated data in order_id after transformation:{silver_payments['order_id'].duplicated().sum()}")
    print("=============================done transformation silver payments=============================")
    return silver_payments, quarantine


def silver_transform_products():
    silver_products = read_bronze("df_Products.csv")
    print_nulls(silver_products, "silver_products before transformation")

    silver_products = silver_products.drop(
        columns=["product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"]
    )
    silver_products, quarantine = split_null_rows(silver_products, "products", "product_id")

    silver_products["product_id"] = silver_products["product_id"].astype(str)
    silver_products["product_category_name"] = silver_products["product_category_name"].astype(str)

    print_nulls(silver_products, "silver_products after transformation")
    print("=============================done transformation silver products=============================")

    silver_products.to_csv(silver_path / "silver_products.csv", index=False)
    return quarantine


def table_silver_transaction():
    silver_orders, q_orders = silver_transform_orders()
    silver_order_items, q_items = silver_transform_orders_item()
    silver_customers, q_customers = silver_transform_customers()
    silver_payments, q_payments = silver_transform_payments()
    q_products = silver_transform_products()

    silver_transaction = silver_orders.merge(silver_order_items, on="order_id", how="left", validate="1:1")
    silver_transaction = silver_transaction.merge(silver_customers, on="customer_id", how="left", validate="1:1")
    silver_transaction = silver_transaction.merge(silver_payments, on="order_id", how="left", validate="1:1")

    silver_transaction, q_transaction = split_null_rows(silver_transaction, "silver_transaction", "order_id")

    save_quarantine([q_orders, q_items, q_customers, q_payments, q_products, q_transaction])
    silver_transaction.to_csv(silver_path / "silver_transaction.csv", index=False)

    print("=============================TRANSFORMATION LAYER IS DONE=============================")