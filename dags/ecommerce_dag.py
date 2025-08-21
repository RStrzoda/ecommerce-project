import os
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from dotenv import load_dotenv

load_dotenv()

SCRIPT_PATH = os.getenv("SCRIPT_PATH")
DBT_DIR = os.getenv("DBT_DIR")

def generate_data():
    os.system(f"python {SCRIPT_PATH}/generate_fake_data.py")

default_args = {
    'owner': 'R',
    'retries':1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id="ecommerce_analytics",
    default_args=default_args,
    start_date=datetime(2025,8,2),
    schedule="@weekly",
    catchup=False,
    tags=["eccomerce", "analytics", "dbt", "churn"]
) as dag:
    
    # ----------- task 01: gerar dados fake ---------
    t1_generate_data = PythonOperator(
        task_id="generate_fake_data",
        python_callable= generate_data
    )

    # ---------- task 02: executar o dbt seed -------
    t2_dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_DIR} && dbt seed --profiles-dir /usr/local/airflow/include/.dbt"
    )

    # --------- task 03: executar dbt run --------
    t3_dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir /usr/local/airflow/include/.dbt"
    )

    # ----------- task 04: executar dbt test ------
    t4_dbt_test = BashOperator(
         task_id="dbt_test",
         bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir /usr/local/airflow/include/.dbt"
    )
    
    # t5_generate_report = BashOperator(
    #     task_id = "generate_report",
    # )

    t1_generate_data >> t2_dbt_seed >> t3_dbt_run >> t4_dbt_test