from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.time_delta import TimeDeltaSensor

default_args = {
    'owner': 'data_engineering_team',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'silver_processing_pipeline',
    default_args=default_args,
    description='Capa Silver: Limpieza, NLP y conversión a Parquet',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'processing', 'nlp'],
) as dag:

    # ESPERA AUTOMÁTICA DE 3 MINUTOS
    # Esto le da tiempo a la capa Bronze de terminar de escribir todos los archivos JSON
    wait_3_minutes = TimeDeltaSensor(
        task_id='wait_3_minutes',
        delta=timedelta(minutes=3),
    )

    # TAREA DE LIMPIEZA Y TRANSFORMACIÓN
    process_silver_layer = BashOperator(
        task_id='preprocess_and_clean_data',
        # Asegúrate de que la ruta al script sea correcta en tu contenedor
        bash_command='python /opt/airflow/dags/scripts/silver/preprocess_datasets.py --base-dir /opt/airflow/datalake',
    )

    # FLUJO AUTOMÁTICO
    wait_3_minutes >> process_silver_layer