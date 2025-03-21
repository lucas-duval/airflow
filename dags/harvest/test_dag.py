from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
import os

# Configuration du DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}
dag = DAG(
    'eco2mix_snowflake_dag',
    default_args=default_args,
    description='DAG pour extraire et charger des données Eco2mix dans Snowflake',
    schedule_interval='@daily'
)
