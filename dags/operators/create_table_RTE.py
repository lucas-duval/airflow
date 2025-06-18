from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd

def create_table_from_csv():
    # Chemin du fichier CSV
    csv_path = '/opt/airflow/data/eco2mix_data.csv'
    # Charger le CSV en DataFrame
    df = pd.read_csv(csv_path, sep=';', nrows=10)  # lire juste 10 lignes pour analyse

    # Mapping pandas -> Snowflake SQL types (simplifié)
    type_map = {
        'int64': 'NUMBER',
        'float64': 'FLOAT',
        'object': 'STRING',
        'bool': 'BOOLEAN',
        'datetime64[ns]': 'TIMESTAMP_NTZ',
    }

    # Construire la liste des colonnes et leurs types SQL
    cols_with_types = []
    for col, dtype in df.dtypes.items():
        sf_type = type_map.get(str(dtype), 'STRING')
        col_safe = col.replace(' ', '_').replace('-', '_')  # nettoie noms de colonnes
        cols_with_types.append(f'"{col_safe.upper()}" {sf_type}')

    # Construire la requête CREATE TABLE IF NOT EXISTS
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS RTE.RTE_DATA (
        {', '.join(cols_with_types)}
    );
    """

    # Exécuter la requête via SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='SnowflakeConnection')
    hook.run(create_table_sql)
    print("Table créée ou existante, requête exécutée.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 0,
}

with DAG(
    'create_RTE_table_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    create_table_task = PythonOperator(
        task_id='create_table_dynamic',
        python_callable=create_table_from_csv,
    )


    create_table_task