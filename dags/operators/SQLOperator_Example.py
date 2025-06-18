# === IMPORTS ===
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

# === FONCTION définie en haut ===
def create_table_from_csv(csv_file, table_name, database_name, schema_name, conn_id):
    df = pd.read_csv(csv_file)

    def pandas_to_snowflake_type(dtype):
        if pd.api.types.is_integer_dtype(dtype):
            return "NUMBER"
        elif pd.api.types.is_float_dtype(dtype):
            return "FLOAT"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "TIMESTAMP"
        else:
            return "VARCHAR"

    columns = []
    for col in df.columns:
        sf_type = pandas_to_snowflake_type(df[col].dtype)
        columns.append(f'"{col}" {sf_type}')

    columns_sql = ",".join(columns)
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {database_name}.{schema_name}.{table_name} (
      {columns_sql}
    );
    """

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    hook.run(f"USE DATABASE {database_name}")
    hook.run(f"USE SCHEMA {schema_name}")

    try:
        cursor.execute(create_table_sql)
        print(f"✅ Table {table_name} créée (ou déjà existante).")
    finally:
        cursor.close()
        conn.close()

# === DAG défini ensuite ===
with DAG(
    "create_snowflake_table_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    create_table = PythonOperator(
        task_id="create_table_from_csv",
        python_callable=create_table_from_csv,
        op_kwargs={
            "csv_file": "/opt/airflow/data/eco2mix_data.csv",
            "table_name": "RTE_RAW_DATA",
            "database_name": "BRONZE",
            "schema_name": "RTE",
            "conn_id": "SnowflakeConnection"
        }
    )