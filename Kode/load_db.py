import sys
from pathlib import Path
import psycopg2
from airflow.hooks.base import BaseHook

SCRIPTS_DIR = Path("/opt/airflow/data")
GOLD_DIR = SCRIPTS_DIR / "gold"

if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))


def gold_to_db():
    SCHEMA = "gold"
    TABLES = {
    "dim_region": ["region_key", "customer_state"],
    "dim_product": ["product_key", "product_category_name"],
    "dim_payment": ["payment_key", "payment_type"],
    "fact_sales": [
        "year_month", "region_key", "product_key", "payment_key",
        "orders_total", "orders_delivered", "orders_canceled",
        "revenue_product", "revenue_shipping", "payment_value_delivered",
        "orders_on_time", "orders_late",
    ],
    }

    CREATE_TABLES = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA};

    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_region (
        region_key      INT PRIMARY KEY,
        customer_state  VARCHAR NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_product (
        product_key            INT PRIMARY KEY,
        product_category_name  VARCHAR NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.dim_payment (
        payment_key   INT PRIMARY KEY,
        payment_type  VARCHAR NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS {SCHEMA}.fact_sales (
        year_month               VARCHAR(7) NOT NULL,
        region_key               INT NOT NULL REFERENCES {SCHEMA}.dim_region (region_key),
        product_key              INT NOT NULL REFERENCES {SCHEMA}.dim_product (product_key),
        payment_key              INT NOT NULL REFERENCES {SCHEMA}.dim_payment (payment_key),
        orders_total             INT NOT NULL,
        orders_delivered         INT NOT NULL,
        orders_canceled          INT NOT NULL,
        revenue_product          NUMERIC(14, 2) NOT NULL,
        revenue_shipping         NUMERIC(14, 2) NOT NULL,
        payment_value_delivered  NUMERIC(14, 2) NOT NULL,
        orders_on_time           INT NOT NULL,
        orders_late              INT NOT NULL,
        PRIMARY KEY (year_month, region_key, product_key, payment_key)
    );
    """

    conn = BaseHook.get_connection('postgres_default')
    db_params = {
        'host': conn.host,
        'port': conn.port,
        'database': conn.schema,
        'user': conn.login,
        'password': conn.password
    }

    connection = psycopg2.connect(**db_params)
    cursor = connection.cursor()

    try:
        cursor.execute(CREATE_TABLES)

        all_tables = ", ".join(f"{SCHEMA}.{t}" for t in TABLES)
        cursor.execute(f"TRUNCATE {all_tables}")

        for table, cols in TABLES.items():
            path = GOLD_DIR / f"{table}.csv"
            with open(path, 'r') as f:
                cursor.copy_expert(
                    f"COPY {SCHEMA}.{table} ({', '.join(cols)}) "
                    f"FROM STDIN WITH (FORMAT csv, HEADER true)",
                    f,
                )
            print(f"{SCHEMA}.{table}: {cursor.rowcount} baris dimuat")

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        cursor.close()
        connection.close()