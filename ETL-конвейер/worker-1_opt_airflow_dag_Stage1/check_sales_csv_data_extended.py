from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import Variable
from datetime import datetime
import hashlib
import pandas as pd

FILE_PATH = '/opt/airflow/dags/files/sales.csv'
HASH_VAR_KEY = 'sales_csv_md5'
STAGE_DONE_VAR = 'stage_num_1_done'
STAGE2_DONE_VAR ='stage_num_2_done'
STAGE3_DONE_VAR ='stage_num_3_done'
STAGE4_DONE_VAR ='stage_num_4_done'

Variable.set(STAGE_DONE_VAR, 0)
Variable.set(STAGE2_DONE_VAR, 0)
Variable.set(STAGE3_DONE_VAR, 0)
Variable.set(STAGE4_DONE_VAR, 0)



def calculate_md5(file_path):
    with open(file_path, 'rb') as f:
        return hashlib.md5(f.read()).hexdigest()

def check_if_file_changed(**kwargs):
    current_hash = calculate_md5(FILE_PATH)
    stored_hash = Variable.get(HASH_VAR_KEY, default_var=None)

    if stored_hash == current_hash:
        print("Файл не изменился — загрузка не требуется.")
        return 'skip_validation'
    else:
        Variable.set(HASH_VAR_KEY, current_hash)
        return 'run_validation'

def validate_data_and_proceed(**kwargs):
    df = pd.read_csv(FILE_PATH)
    errors = []

    if df.isnull().sum().sum() > 0:
        errors.append("Присутствуют пропущенные значения")
    if df.duplicated().sum() > 0:
        errors.append("Присутствуют дубликаты строк")
    if (df.select_dtypes(include='number') < 0).any().any():
        errors.append("Есть отрицательные значения")
    try:
        pd.to_datetime(df['Date'])
    except Exception:
        errors.append("Ошибка преобразования даты")

    if errors:
        Variable.set(STAGE_DONE_VAR, 0)
        raise ValueError("Ошибки в данных:\n" + "\n".join(errors))
    else:
        print("Данные прошли проверку успешно.")
        Variable.set(STAGE_DONE_VAR, 1)

def skip_validation():
    print("Пропуск валидации, файл не изменился.")

with DAG(
    dag_id='check_sales_csv_data_extended_to_stg2',
    schedule_interval='@daily',
    start_date=datetime(2019, 1, 1),
    catchup=False,
    tags=['validation', 'sales']
) as dag:

    check_file = BranchPythonOperator(
        task_id='check_if_file_changed',
        python_callable=check_if_file_changed,
        provide_context=True
    )

    validate = PythonOperator(
        task_id='run_validation',
        python_callable=validate_data_and_proceed
    )

    skip = PythonOperator(
        task_id='skip_validation',
        python_callable=skip_validation
    )

    trigger_load_raw = TriggerDagRunOperator(
        task_id='trigger_load_to_raw_clickhouse',
        trigger_dag_id='load_to_raw_clickhouse',
        wait_for_completion=False,
        reset_dag_run=True
    )

    check_file >> [validate, skip]
    validate >> trigger_load_raw
