from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
import pandas as pd

STAGE_VAR = 'stage_num_2_done'
FILE_PATH = '/opt/airflow/dags/files/sales.csv'
TABLE_NAME = 'raw_layer.sales_raw_layer'

default_args = {
    'owner': 'airflow',
    'retries': 1,
}

with DAG(
    dag_id='load_to_raw_clickhouse',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=['clickhouse', 'raw']
) as dag:

    drop_table = HttpOperator(
        task_id='drop_sales_raw_table',
        http_conn_id='clickhouse_http_default',
        endpoint='',
        method='POST',
        data=f"DROP TABLE IF EXISTS {TABLE_NAME}",
        headers={"Content-Type": "application/sql"},
        log_response=True,
    )

    create_table = HttpOperator(
        task_id='create_sales_raw_table',
        http_conn_id='clickhouse_http_default',
        endpoint='',
        method='POST',
        data=f"""
        CREATE TABLE {TABLE_NAME} (
            Invoice_ID String,
            Branch String,
            City String,
            Customer_type String,
            Gender String,
            Product_line String,
            Unit_price Float64,
            Quantity UInt8,
            Tax_5 Float64,
            Total Float64,
            Date String,
            Time String,
            Payment String,
            cogs Float64,
            gross_margin_percentage Float64,
            gross_income Float64,
            Rating Float64
        ) ENGINE = MergeTree()
        ORDER BY Invoice_ID
        """,
        headers={"Content-Type": "application/sql"},
        log_response=True,
    )

    def insert_sales_data():
        df = pd.read_csv(FILE_PATH)
        df.columns = [c.replace(" ", "_").replace("%", "") for c in df.columns]
        values = []

        for _, row in df.iterrows():
            val = "(" + ", ".join([
                f"'{str(row['Invoice_ID'])}'",
                f"'{row['Branch']}'",
                f"'{row['City']}'",
                f"'{row['Customer_type']}'",
                f"'{row['Gender']}'",
                f"'{row['Product_line']}'",
                f"{row['Unit_price']}",
                f"{int(row['Quantity'])}",
                f"{row['Tax_5']}",
                f"{row['Total']}",
                f"'{row['Date']}'",
                f"'{row['Time']}'",
                f"'{row['Payment']}'",
                f"{row['cogs']}",
                f"{row['gross_margin_percentage']}",
                f"{row['gross_income']}",
                f"{row['Rating']}"
            ]) + ")"
            values.append(val)

        query = f"INSERT INTO {TABLE_NAME} VALUES " + ",\n".join(values)
        from airflow.providers.http.hooks.http import HttpHook
        hook = HttpHook(method='POST', http_conn_id='clickhouse_http_default')
        hook.run(endpoint="", data=query, headers={"Content-Type": "application/sql"})
        Variable.set(STAGE_VAR, 1)
        print("Данные успешно загружены в ClickHouse.")

    insert_task = PythonOperator(
        task_id='insert_sales_data',
        python_callable=insert_sales_data
    )

    trigger_nds_dag = TriggerDagRunOperator(
        task_id='trigger_load_nds_from_raw',
        trigger_dag_id='load_nds_from_raw',
        wait_for_completion=False,
        reset_dag_run=True
    )

    drop_table >> create_table >> insert_task >> trigger_nds_dag

