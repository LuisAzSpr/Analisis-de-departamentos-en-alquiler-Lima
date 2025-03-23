import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

'''
pip install dotenv
'''

load_dotenv()

# Agregar la raíz del proyecto al PYTHONPATH

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from tasks.formateo import Formateo
from tasks.data_wrangling import Datawrangling
from tasks.limpieza import Limpieza

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/luis/Downloads/first-outlet-454113-e2-e353fd319c22.json'

with DAG(
        'proceso_etl',
        default_args=default_args,
        description='Proceso ETL para datos de departamentos y carga a BigQuery',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False
) as dag:

    def tarea_formateo(**context):
        formateo = Formateo(ruta_lectura='data/rawdata/')
        formateo_ruta = formateo.realizar_formateo()
        context['ti'].xcom_push(key='formateo_ruta', value=formateo_ruta)

    def tarea_datawrangling(**context):
        formateo_ruta = context['ti'].xcom_pull(key='formateo_ruta', task_ids='tarea_formateo')
        datawrangling = Datawrangling(ruta_lectura=formateo_ruta)
        datawrangling_ruta = datawrangling.realizar_data_wrangling()
        context['ti'].xcom_push(key='datawrangling_ruta', value=datawrangling_ruta)

    def tarea_limpieza(**context):
        datawrangling_ruta = context['ti'].xcom_pull(key='datawrangling_ruta', task_ids='tarea_datawrangling')
        datalimpieza = Limpieza(ruta_lectura=datawrangling_ruta)
        datalimpieza_ruta = datalimpieza.realizar_limpieza()
        context['ti'].xcom_push(key='datalimpieza_ruta', value=datalimpieza_ruta)

    tarea_formateo_op = PythonOperator(
        task_id='tarea_formateo',
        python_callable=tarea_formateo,
        provide_context=True
    )

    tarea_datawrangling_op = PythonOperator(
        task_id='tarea_datawrangling',
        python_callable=tarea_datawrangling,
        provide_context=True
    )

    tarea_limpieza_op = PythonOperator(
        task_id='tarea_limpieza',
        python_callable=tarea_limpieza,
        provide_context=True
    )

    # Nota: Se utiliza una plantilla en source_objects para obtener el valor de XCom
    carga_bq_op = GCSToBigQueryOperator(
        task_id='carga_csv_a_bq',
        bucket='us-central1-composer-dev-lu-620fcc1f-bucket',
        source_objects=["{{ ti.xcom_pull(task_ids='tarea_limpieza', key='datalimpieza_ruta') }}"],
        destination_project_dataset_table='first-outlet-454113-e2.alquiler_depas_lima.precios_depas_lima_actualizado',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE'
    )

    # Definir la secuencia de tareas
    tarea_formateo_op >> tarea_datawrangling_op >> tarea_limpieza_op >> carga_bq_op


