# hello_world_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello():
    print("Hello, world!")

with DAG(
    dag_id='hello_world',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = PythonOperator(
        task_id='say_hello_task',
        python_callable=say_hello,
    )
