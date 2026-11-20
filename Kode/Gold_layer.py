import os
from pathlib import Path
import pandas as pd

BASE_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
SILVER_DIR = BASE_DIR / "silver"
GOLD_DIR = BASE_DIR / "gold"
GOLD_DIR.mkdir(parents=True, exist_ok=True)

DATE_COLS = [
    "order_purchase_timestamp",
    "order_delivered_timestamp",
    "order_estimated_delivery_date",
]


def load_silver():
    tx = pd.read_csv(SILVER_DIR / "silver_transaction.csv")
    prod = pd.read_csv(SILVER_DIR / "silver_products.csv")

    for col in DATE_COLS:
        tx[col] = pd.to_datetime(tx[col], errors="coerce")

    return tx, prod


def prepare_transaction(tx, prod):
    n_rows = len(tx)

    conflict = prod.groupby("product_id")["product_category_name"].nunique().gt(1).sum()
    if conflict:
        print(f"PERINGATAN: {conflict} product_id memiliki lebih dari satu kategori "
              f"(diambil yang pertama)")
    product_map = prod.drop_duplicates("product_id")[["product_id", "product_category_name"]]

    tx = tx.merge(product_map, on="product_id", how="left", validate="m:1")
    if len(tx) != n_rows:
        raise ValueError(f"Jumlah baris berubah setelah join produk: {n_rows} -> {len(tx)}")

    tx["product_category_name"] = tx["product_category_name"].fillna("tidak terkategori")
    tx["customer_state"] = tx["customer_state"].fillna("tidak diketahui")
    tx["payment_type"] = tx["payment_type"].fillna("tidak diketahui")

    tx["year_month"] = tx["order_purchase_timestamp"].dt.strftime("%Y-%m")

    is_delivered = tx["order_status"].eq("delivered")

    delivered_day = tx["order_delivered_timestamp"].dt.normalize()
    estimated_day = tx["order_estimated_delivery_date"].dt.normalize()
    purchase_day = tx["order_purchase_timestamp"].dt.normalize()
    measurable = is_delivered & delivered_day.notna()
    delay_days = (delivered_day - estimated_day).dt.days

    tx["orders_total"] = 1
    tx["orders_delivered"] = is_delivered.astype(int)
    tx["orders_canceled"] = tx["order_status"].eq("canceled").astype(int)
    tx["revenue_product"] = tx["price"].where(is_delivered, 0)
    tx["revenue_shipping"] = tx["shipping_charges"].where(is_delivered, 0)
    tx["payment_value_delivered"] = tx["payment_value"].where(is_delivered, 0)
    tx["orders_on_time"] = (measurable & (delay_days <= 0)).astype(int)
    tx["orders_late"] = (measurable & (delay_days > 0)).astype(int)

    return tx


def build_dimension(tx, column, key_name):
    dim = (
        tx[[column]]
        .drop_duplicates()
        .sort_values(column)
        .reset_index(drop=True)
    )
    dim.insert(0, key_name, dim.index + 1)
    return dim


def build_fact(tx, dim_region, dim_product, dim_payment):
    n_rows = len(tx)

    tx = (
        tx.merge(dim_region, on="customer_state", how="left", validate="m:1")
        .merge(dim_product, on="product_category_name", how="left", validate="m:1")
        .merge(dim_payment, on="payment_type", how="left", validate="m:1")
    )
    if len(tx) != n_rows:
        raise ValueError("Jumlah baris berubah setelah join dimensi")

    fact = tx.groupby(
        ["year_month", "region_key", "product_key", "payment_key"], as_index=False
    ).agg(
        orders_total=("orders_total", "sum"),
        orders_delivered=("orders_delivered", "sum"),
        orders_canceled=("orders_canceled", "sum"),
        revenue_product=("revenue_product", "sum"),
        revenue_shipping=("revenue_shipping", "sum"),
        payment_value_delivered=("payment_value_delivered", "sum"),
        orders_on_time=("orders_on_time", "sum"),
        orders_late=("orders_late", "sum")
    )

    if fact["orders_total"].sum() != n_rows:
        raise ValueError("Total order di fakta tidak sama dengan data sumber")
    if abs(fact["revenue_product"].sum() - tx["revenue_product"].sum()) > 0.01:
        raise ValueError("Total revenue di fakta tidak sama dengan data sumber")

    return fact


def run_gold():
    tx, prod = load_silver()
    tx = prepare_transaction(tx, prod)

    dim_region = build_dimension(tx, "customer_state", "region_key")
    dim_product = build_dimension(tx, "product_category_name", "product_key")
    dim_payment = build_dimension(tx, "payment_type", "payment_key")

    fact_sales = build_fact(tx, dim_region, dim_product, dim_payment)

    dim_region.to_csv(GOLD_DIR / "dim_region.csv", index=False)
    dim_product.to_csv(GOLD_DIR / "dim_product.csv", index=False)
    dim_payment.to_csv(GOLD_DIR / "dim_payment.csv", index=False)
    fact_sales.to_csv(GOLD_DIR / "fact_sales.csv", index=False)

    print(f"dim_region : {len(dim_region)} baris")
    print(f"dim_product: {len(dim_product)} baris")
    print(f"dim_payment: {len(dim_payment)} baris")
    print(f"fact_sales : {len(fact_sales)} baris")

if __name__ == "__main__":
    run_gold()