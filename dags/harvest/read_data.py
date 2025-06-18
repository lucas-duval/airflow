
from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime,timedelta
from operators.eco2mix_extract_operator import Eco2MixExtractorOperator
from operators.excel_to_csv_operator import ConvertXlsToCsvOperator
from operators.unzip_operator import UnzipOperator
from operators.enedis_extract import EnedisExtractorOperator

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'eco2mix_extraction',
    default_args=default_args,
    description='Extraction des données Eco2Mix et stockage en CSV',
    schedule_interval='@daily',  # Exécution quotidienne
)

# Exécuter l'opérateur personnalisé pour une date spécifique
extract_task = Eco2MixExtractorOperator(
    task_id="extract_eco2mix_data",
    date="19/04/2025",  # Date spécifique à traiter
    output_path='/opt/airflow/data/eco2mix_data.zip',  # Chemin de sortie du zip'
    dag=dag
)

unzip_task = UnzipOperator(
    task_id="unzip",
    input_path='/opt/airflow/data/eco2mix_data.zip',
    output_path='/opt/airflow/data/eco2mix_datas',
    dag=dag
)

to_csv_task = ConvertXlsToCsvOperator(
    task_id="convert_to_csv",
    start = '2025-04-19',
    end = '2025-04-19',
    xls_dir = '/opt/airflow/data',
    csv_dir = '/opt/airflow/data',
    dag=dag
)


extract_task >> unzip_task >> to_csv_task
