
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

STAGE_VAR = 'stage_dds_loaded'

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

def build_sql_task(task_id, sql):
    return SimpleHttpOperator(
        task_id=task_id,
        http_conn_id='clickhouse_http_default',
        method='POST',
        data=sql,
        headers={"Content-Type": "application/sql"},
        log_response=True,
    )

with DAG(
    dag_id='load_dds_from_core',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['clickhouse', 'datamart', 'star_schema']
) as dag:

    # customer_dim
    customer_create = build_sql_task("create_customer_dim", '''
        CREATE TABLE IF NOT EXISTS datamart_layer.customer_dim (
            customer_type_id UInt32,
            customer_type String
        ) ENGINE = MergeTree() ORDER BY customer_type_id;
    ''')
    customer_truncate = build_sql_task("truncate_customer_dim", "TRUNCATE TABLE datamart_layer.customer_dim;")
    customer_insert = build_sql_task("insert_customer_dim", '''
        INSERT INTO datamart_layer.customer_dim
        SELECT * FROM core_layer.customer_type;
    ''')

    # gender_dim
    gender_create = build_sql_task("create_gender_dim", '''
        CREATE TABLE IF NOT EXISTS datamart_layer.gender_dim (
            gender_id UInt32,
            gender String
        ) ENGINE = MergeTree() ORDER BY gender_id;
    ''')
    gender_truncate = build_sql_task("truncate_gender_dim", "TRUNCATE TABLE datamart_layer.gender_dim;")
    gender_insert = build_sql_task("insert_gender_dim", '''
        INSERT INTO datamart_layer.gender_dim
        SELECT * FROM core_layer.gender;
    ''')

    # store_dim
    store_create = build_sql_task("create_store_dim", '''
        CREATE TABLE IF NOT EXISTS datamart_layer.store_dim (
            store_id UInt32,
            branch String,
            city String
        ) ENGINE = MergeTree() ORDER BY store_id;
    ''')
    store_truncate = build_sql_task("truncate_store_dim", "TRUNCATE TABLE datamart_layer.store_dim;")
    store_insert = build_sql_task("insert_store_dim", '''
        INSERT INTO datamart_layer.store_dim
        SELECT * FROM core_layer.store;
    ''')

    # product_dim
    product_create = build_sql_task("create_product_dim", '''
        CREATE TABLE IF NOT EXISTS datamart_layer.product_dim (
            product_id UInt32,
            product_line String,
            unit_price Float64
        ) ENGINE = MergeTree() ORDER BY product_id;
    ''')
    product_truncate = build_sql_task("truncate_product_dim", "TRUNCATE TABLE datamart_layer.product_dim;")
    product_insert = build_sql_task("insert_product_dim", '''
        INSERT INTO datamart_layer.product_dim
        SELECT * FROM core_layer.product;
    ''')

    # payment_dim
    payment_create = build_sql_task("create_payment_dim", '''
        CREATE TABLE IF NOT EXISTS datamart_layer.payment_dim (
            payment_id UInt32,
            payment_type String
        ) ENGINE = MergeTree() ORDER BY payment_id;
    ''')
    payment_truncate = build_sql_task("truncate_payment_dim", "TRUNCATE TABLE datamart_layer.payment_dim;")
    payment_insert = build_sql_task("insert_payment_dim", '''
        INSERT INTO datamart_layer.payment_dim
        SELECT * FROM core_layer.payment;
    ''')

    # month_dim
    month_create = build_sql_task("create_month_dim", '''
        CREATE TABLE IF NOT EXISTS datamart_layer.month_dim (
            month_id UInt32,
            month_name String,
            number_of_month UInt8,
            year_of_month UInt32
        ) ENGINE = MergeTree() ORDER BY month_id;
    ''')
    month_truncate = build_sql_task("truncate_month_dim", "TRUNCATE TABLE datamart_layer.month_dim;")
    month_insert = build_sql_task("insert_month_dim", '''
        INSERT INTO datamart_layer.month_dim
        SELECT DISTINCT
            toYYYYMM(datetime) AS month_id,
            dateName('month', datetime) AS month_name,
            toMonth(datetime) AS number_of_month,
            toYear(datetime) AS year_of_month
        from (
    select
        DateAdd(day, n.number, toDate('2019-01-01')) as datetime
    from
        numbers(0, 365 * 1) as n
);
    ''')

    # sales_fact
    sales_create = build_sql_task("create_sales_fact", '''
        CREATE TABLE IF NOT EXISTS datamart_layer.sales_fact (
            invoice_id String,
            customer_type_id UInt32,
            gender_id UInt32,
            store_id UInt32,
            product_id UInt32,
            payment_id UInt32,
            month_id UInt32,
            datetime DateTime,
            quantity UInt8,
            tax Float64,
            total Float64,
            cogs Float64,
            gross_income Float64,
            rating Float64
        ) ENGINE = MergeTree() ORDER BY invoice_id;
    ''')
    sales_truncate = build_sql_task("truncate_sales_fact", "TRUNCATE TABLE datamart_layer.sales_fact;")
    sales_insert = build_sql_task("insert_sales_fact", '''
        INSERT INTO datamart_layer.sales_fact
        SELECT
            s.invoice_id,
            s.customer_type_id,
            s.gender_id,
            s.store_id,
            s.product_id,
            s.payment_id,
            toYYYYMM(s.datetime) AS month_id,
            s.datetime,
            s.quantity,
            s.tax,
            s.total,
            s.cogs,
            s.gross_income,
            s.rating
        FROM core_layer.sales s;
    ''')

    finalize = PythonOperator(
        task_id='set_stage_dds_loaded',
        python_callable=lambda: Variable.set(STAGE_VAR, 1),
    )

    (
        customer_create >> customer_truncate >> customer_insert >>
        gender_create >> gender_truncate >> gender_insert >>
        store_create >> store_truncate >> store_insert >>
        product_create >> product_truncate >> product_insert >>
        payment_create >> payment_truncate >> payment_insert >>
        month_create >> month_truncate >> month_insert >>
        sales_create >> sales_truncate >> sales_insert >>
        finalize
    )
