from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'bronze_ingestion_pipeline',
    default_args=default_args,
    description='Capa Bronze: Ingesta de Eater NY y Spoonacular API',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze', 'ingestion', 'raw'],
) as dag:

    scrape_eater_ny = BashOperator(
        task_id='scrape_eater_ny',
        # Añadimos export PYTHONPATH para que Python encuentre la carpeta scripts
        bash_command='export PYTHONPATH=$PYTHONPATH:/opt/airflow/dags && python /opt/airflow/dags/scripts/bronze/WebScrapping_NY.py',
    )

    extract_spoonacular_api = BashOperator(
        task_id='extract_spoonacular_api',
        bash_command='export PYTHONPATH=$PYTHONPATH:/opt/airflow/dags && python /opt/airflow/dags/scripts/bronze/api_ingestion.py',
    )

    # En la capa Bronze, la ingesta de las dos fuentes puede ocurrir al mismo tiempo
    [scrape_eater_ny, extract_spoonacular_api]