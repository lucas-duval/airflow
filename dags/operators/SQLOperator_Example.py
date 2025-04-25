from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 4, 1),
    'retries': 0,
}

# Create the DAG
with DAG(
    'snowflake_sql_query_example',
    default_args=default_args,
    schedule_interval=None,  # Manually trigger the DAG
    catchup=False,
) as dag:

    # Define the SQL query to be executed
    snowflake_query = """
    SELECT 1;
    """

    # Use the SQLExecuteQueryOperator to execute the SQL query
    execute_query_task = SQLExecuteQueryOperator(
        task_id='execute_snowflake_query',
        sql=snowflake_query,
        conn_id='snowflake_conn',  # The connection id to Snowflake (change here)
    )

    execute_query_task