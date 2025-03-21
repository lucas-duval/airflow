from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from eco2mix_operator import Eco2MixOperator
import pandas as pd
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

# Variables de configuration
output_path = '/tmp/eco2mix_data.csv'
snowflake_conn_id = 'snowflake_conn'
stage_name = 'eco2mix_stage'
table_name = 'eco2mix_data'

# Extraction des données
extract_task = Eco2MixOperator(
    task_id='extract_eco2mix_data',
    output_path=output_path,
    start_date='2023-01-01',
    end_date='2023-12-31',
    dag=dag
)

# Création de la table Snowflake si elle n'existe pas
def create_table():
    df = pd.read_csv(output_path, sep=';')
    columns = ', '.join([f'{col} STRING' for col in df.columns])
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        {columns}
    );
    """
    return create_query

create_table_task = SnowflakeOperator(
    task_id='create_table_if_not_exists',
    snowflake_conn_id=snowflake_conn_id,
    sql=create_table(),
    dag=dag
)

# Chargement du fichier CSV dans Snowflake (PUT)
put_task = SnowflakeOperator(
    task_id='put_file_to_stage',
    snowflake_conn_id=snowflake_conn_id,
    sql=f"PUT file://{output_path} @{stage_name}",
    dag=dag
)

# Copie des données du stage vers la table
copy_task = SnowflakeOperator(
    task_id='copy_into_table',
    snowflake_conn_id=snowflake_conn_id,
    sql=f"COPY INTO {table_name} FROM @{stage_name}/{os.path.basename(output_path)} FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
    dag=dag
)

# Dépendances des tâches
extract_task >> create_table_task >> put_task >> copy_task
