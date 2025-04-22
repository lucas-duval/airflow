from airflow import DAG
from datetime import datetime

with DAG('my_dag_id', start_date=datetime(2023, 1, 1), schedule_interval='@daily') as dag:
    pass
