from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from Nasa_ETL import Extract_Data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'Nasa_ETL_dag',
    default_args=default_args,
    description='ETL',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complete_etl',
    python_callable=Extract_Data,
    dag=dag, 
)

run_etl