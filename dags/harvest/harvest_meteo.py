
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
from operators.meteo_extract import MeteoExtractorOperator
from operators.ungz_operator_operator import UngzOperator

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'meteo_extraction',
    default_args=default_args,
    description='Extraction des données Meteo et stockage en CSV',
    schedule_interval='@daily',  # Exécution quotidienne
)

# Exécuter l'opérateur personnalisé pour une date spécifique
extract_task = MeteoExtractorOperator(
    task_id="extract_eco2mix_data",
    output_path='/opt/airflow/data',  # Chemin de sortie du zip'
    dag=dag
)

unzip_task = UngzOperator(
    task_id="ungz",
    input_path='/opt/airflow/data',
    output_path='/opt/airflow/data',
    dag=dag
)



extract_task >> unzip_task
