
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

STAGE_VAR = 'stage_core_loaded'

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='load_nds_from_raw',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['clickhouse', 'core', '3nf']
) as dag:

    def build_sql_task(task_id, sql):
        return SimpleHttpOperator(
            task_id=task_id,
            http_conn_id='clickhouse_http_default',
            method='POST',
            data=sql,
            headers={"Content-Type": "application/sql"},
            log_response=True,
        )

    task_sequence = []

    sql_task_defs = [
        ("core_customer_type_create", '''CREATE TABLE IF NOT EXISTS core_layer.customer_type (
            customer_type_id UInt32,
            customer_type String
        ) ENGINE = MergeTree() ORDER BY customer_type_id;'''),
        ("core_customer_type_truncate", "TRUNCATE TABLE core_layer.customer_type;"),
        ("core_customer_type", '''INSERT INTO core_layer.customer_type
            SELECT row_number() OVER () AS customer_type_id, "Customer_type" AS customer_type
            FROM (SELECT DISTINCT "Customer_type" FROM raw_layer.sales_raw_layer);'''),

        ("core_gender_create", '''CREATE TABLE IF NOT EXISTS core_layer.gender (
            gender_id UInt32,
            gender String
        ) ENGINE = MergeTree() ORDER BY gender_id;'''),
        ("core_gender_truncate", "TRUNCATE TABLE core_layer.gender;"),
        ("core_gender", '''INSERT INTO core_layer.gender
            SELECT row_number() OVER () AS gender_id, "Gender" AS gender
            FROM (SELECT DISTINCT "Gender" FROM raw_layer.sales_raw_layer);'''),

        ("core_store_create", '''CREATE TABLE IF NOT EXISTS core_layer.store (
            store_id UInt32,
            branch String,
            city String
        ) ENGINE = MergeTree() ORDER BY store_id;'''),
        ("core_store_truncate", "TRUNCATE TABLE core_layer.store;"),
        ("core_store", '''INSERT INTO core_layer.store
            SELECT row_number() OVER () AS store_id, "Branch", "City"
            FROM (SELECT DISTINCT "Branch", "City" FROM raw_layer.sales_raw_layer);'''),

        ("core_product_create", '''CREATE TABLE IF NOT EXISTS core_layer.product (
            product_id UInt32,
            product_line String,
            unit_price Float64
        ) ENGINE = MergeTree() ORDER BY product_id;'''),
        ("core_product_truncate", "TRUNCATE TABLE core_layer.product;"),
        ("core_product", '''INSERT INTO core_layer.product
            SELECT row_number() OVER () AS product_id, "Product_line", "Unit_price"
            FROM (SELECT DISTINCT "Product_line", "Unit_price" FROM raw_layer.sales_raw_layer);'''),

        ("core_payment_create", '''CREATE TABLE IF NOT EXISTS core_layer.payment (
            payment_id UInt32,
            payment_type String
        ) ENGINE = MergeTree() ORDER BY payment_id;'''),
        ("core_payment_truncate", "TRUNCATE TABLE core_layer.payment;"),
        ("core_payment", '''INSERT INTO core_layer.payment
            SELECT row_number() OVER () AS payment_id, "Payment"
            FROM (SELECT DISTINCT "Payment" FROM raw_layer.sales_raw_layer);'''),

        ("core_sales_create", '''CREATE TABLE IF NOT EXISTS core_layer.sales (
            invoice_id String,
            customer_type_id UInt32,
            gender_id UInt32,
            store_id UInt32,
            product_id UInt32,
            payment_id UInt32,
            datetime DateTime,
            quantity UInt8,
            tax Float64,
            total Float64,
            cogs Float64,
            gross_income Float64,
            rating Float64
        ) ENGINE = MergeTree() ORDER BY invoice_id;'''),
        ("core_sales_truncate", "TRUNCATE TABLE core_layer.sales;"),
        ("core_sales", '''INSERT INTO core_layer.sales
            SELECT
                s."Invoice_ID",
                ct.customer_type_id,
                g.gender_id,
                st.store_id,
                p.product_id,
                pay.payment_id,
                parseDateTimeBestEffort(concat(s."Date", ' ', s."Time")) AS datetime,
                s."Quantity",
                s."Tax_5" AS tax,
                s."Total",
                s."cogs",
                s."gross_income",
                s."Rating"
            FROM raw_layer.sales_raw_layer s
            JOIN core_layer.customer_type ct ON s."Customer_type" = ct.customer_type
            JOIN core_layer.gender g ON s."Gender" = g.gender
            JOIN core_layer.store st ON s."Branch" = st.branch AND s."City" = st.city
            JOIN core_layer.product p ON s."Product_line" = p.product_line AND s."Unit_price" = p.unit_price
            JOIN core_layer.payment pay ON s."Payment" = pay.payment_type;'''),
    ]

    last_task = None
    for task_id, sql in sql_task_defs:
        task = build_sql_task(task_id, sql)
        if last_task:
            last_task >> task
        last_task = task

    set_stage_done = PythonOperator(
        task_id='set_stage_core_loaded',
        python_callable=lambda: Variable.set(STAGE_VAR, 1),
    )

    trigger_next = TriggerDagRunOperator(
        task_id='trigger_load_dds_from_core',
        trigger_dag_id='load_dds_from_core',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    last_task >> set_stage_done >> trigger_next
