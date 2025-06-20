# === IMPORTS ===
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os

# === FONCTION définie en haut ===
def create_table_from_csv(csv_file, table_name, database_name, schema_name, stage_name, warehouse_name, auto_compress, overwrite, conn_id, file_name_in_stage, on_error):
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

    columns_sql = ",\n  ".join(columns)
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
    hook.run(f"USE WAREHOUSE {warehouse_name}")

    try:
        cursor.execute(create_table_sql)
        print(f"✅ Table {table_name} créée (ou déjà existante).")
    finally:
        cursor.close()
        conn.close()
        
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"Fichier introuvable : {csv_file}")

    

    put_cmd = (
        f"PUT file://{csv_file} @{database_name}.{schema_name}.{stage_name} "
        f"{'AUTO_COMPRESS=TRUE' if auto_compress else ''} "
        f"{'OVERWRITE=TRUE' if overwrite else ''};"
    )

    print(f"Exécution de la commande PUT : {put_cmd}")
    hook.run(put_cmd)
    print("Upload vers le stage terminé.")
    
    
    def format_value(v):
        # Si c'est un int, on ne met pas de quotes, sinon on entoure de quotes
        if isinstance(v, int):
            return str(v)
        else:
            return f"'{v}'"


    copy_sql = f"""
    COPY INTO {database_name}.{schema_name}.{table_name}
    FROM @{database_name}.{schema_name}.{stage_name}/{file_name_in_stage};
    """

    print(f"Exécution de la commande COPY INTO : {copy_sql}")
    hook.run(copy_sql)
    print("COPY INTO terminé.")

# === DAG défini ensuite ===
with DAG(
    "create_snowflake_table_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    create_table = PythonOperator(
        task_id="create_ingest_table_from_csv",
        python_callable=create_table_from_csv,
        op_kwargs={
            "csv_file": "/opt/airflow/data/eco2mix_data.csv",
            "table_name": "RTE_RAW_DATA",
            "database_name": "BRONZE",
            "schema_name": "RTE",
            "stage_name": "RTE_STAGE",
            "warehouse_name": "INGEST_WH",
            "auto_compress": "TRUE",
            "overwrite": "TRUE",
            "conn_id": "SnowflakeConnection",
            "file_name_in_stage": "FILE_NAME_IN_STAGE",
            "on_error": "ON_ERROR"
        }
    )