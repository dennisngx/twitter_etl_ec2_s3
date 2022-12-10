from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
from twitter_etl_function import twitter_etl

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2022, 12, 9),
    'email' : ['ngx.dennis@gmail.com'],
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delay' : timedelta(minutes=1)
}

dag = DAG(
    'twitter_airflow_dag',
    default_args = default_args,
    description = 'Twitter ETL code'
)

run_etl = PythonOperator(
    task_id = 'complete_twitter_etl',
    python_callable = twitter_etl,
    dag = dag,
)

run_etl
