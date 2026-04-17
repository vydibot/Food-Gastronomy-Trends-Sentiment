from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os

# Argumentos base
default_args = {
    'owner': 'data_engineering_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='silver_processing_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'taskflow']
)
def silver_processing_dag():

    # EL SENSOR: Se queda "escuchando" hasta que aparezca un JSON en Bronze
    wait_for_data = FileSensor(
        task_id='wait_for_bronze_files',
        filepath='api/*.json', # fs_default ya apunta a /opt/airflow/datalake/bronze
        fs_conn_id='fs_default',
        poke_interval=30, # Revisa cada 30 segundos
        timeout=600       # Se rinde después de 10 minutos
    )

    @task()
    def process_data_task():
        # Importamos la lógica de tu script de limpieza
        from scripts.silver.preprocess_datasets import main as run_silver_logic
        
        # Ejecutamos el procesamiento
        run_silver_logic()
        return "Capa Silver completada: JSONs convertidos a Parquet"

    # Definir la dependencia
    wait_for_data >> process_data_task()

# Instanciar el DAG
dag_instance = silver_processing_dag()