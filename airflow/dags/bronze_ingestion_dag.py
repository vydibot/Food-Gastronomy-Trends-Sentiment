from airflow.decorators import dag, task
from datetime import datetime
import sys

# Aseguramos que Python vea la carpeta de scripts
sys.path.append('/opt/airflow/dags')

# IMPORTACIÓN CORRECTA (fíjate en las mayúsculas)
from scripts.bronze.WebScrapping_NY import main as run_web_scraping
from scripts.bronze.api_ingestion import main as run_api_ingestion

@dag(
    dag_id='bronze_ingestion_pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bronze']
)
def bronze_ingestion_pipeline():
    
    @task()
    def extract_api_data():
        run_api_ingestion()
        return "API OK"

    @task()
    def extract_web_data():
        # Ahora sí llamará a la función main del script WebScrapping_NY.py
        run_web_scraping() 
        return "Web OK"

    extract_api_data() >> extract_web_data()

ingestion_dag = bronze_ingestion_pipeline()