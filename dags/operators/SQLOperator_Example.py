from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

with DAG("create_snowflake_table_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    
    create_table = PythonOperator(
        task_id="create_table_from_csv",
        op_kwargs={
            "csv_file": "/opt/airflow/data/enedis_bilan_electrique.csv",
            "table_name": "ENEDIS_RAW_DATA",
            "database_name": "BRONZE",
            "schema_name": "ENEDIS",
            "conn_id": "SnowflakeConnection"
        }
    )