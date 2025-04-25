import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def create_table_from_csv(csv_file, table_name, database_name, schema_name, conn_id):
    # === Lecture du CSV ===
    df = pd.read_csv(csv_file)

    # === Conversion pandas -> Snowflake ===
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

    # === Exécution via SnowflakeHook ===
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute(create_table_sql)
        print(f"✅ Table {table_name} créée (ou existante déjà).")
    finally:
        cursor.close()
        conn.close()