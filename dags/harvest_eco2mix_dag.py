from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
# from operators.eco2mix_operator import Eco2mixOperator
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
zip_output_path = '/tmp/eco2mix_data.zip'
xls_output_path = '/tmp/eco2mix_data.xls'
csv_output_path = '/tmp/eco2mix_data.csv'
snowflake_conn_id = 'snowflake_conn'
stage_name = 'eco2mix_stage'
table_name = 'eco2mix_data'

# Extraction des données avec l'opérateur Eco2Mix
#extract_task = Eco2MixOperator(
#    task_id='extract_eco2mix_data',
#    output_path=zip_output_path,
#    start_date='2023-01-01',
#    end_date='2023-12-31',
#    dag=dag
#)

# Conversion du fichier .xls extrait vers .csv
def convert_xls_to_csv(xls_path, csv_path):
    import pandas as pd
    df = pd.read_excel(xls_path)
    df.to_csv(csv_path, index=False)

convert_task = PythonOperator(
    task_id='convert_xls_to_csv',
    python_callable=convert_xls_to_csv,
    op_args=[xls_output_path, csv_output_path],
    dag=dag
)

# Création de la table Snowflake si elle n'existe pas
create_table_task = SnowflakeOperator(
    task_id='create_table_if_not_exists',
    snowflake_conn_id=snowflake_conn_id,
    sql=f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        date STRING,
        consommation STRING,
        production STRING
    );
    """,
    dag=dag
)

# Chargement du fichier CSV dans Snowflake (PUT)
put_task = SnowflakeOperator(
    task_id='put_file_to_stage',
    snowflake_conn_id=snowflake_conn_id,
    sql=f"PUT file://{csv_output_path} @{stage_name}",
    dag=dag
)

# Copie des données du stage vers la table
copy_task = SnowflakeOperator(
    task_id='copy_into_table',
    snowflake_conn_id=snowflake_conn_id,
    sql=f"COPY INTO {table_name} FROM @{stage_name}/{os.path.basename(csv_output_path)} FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '\"')",
    dag=dag
)

# extract_task Dépendances des tâches
convert_task >> create_table_task >> put_task >> copy_task
