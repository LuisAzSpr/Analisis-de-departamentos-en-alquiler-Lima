
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

from tasks.formateo import Formateo
from tasks.data_wrangling import Datawrangling
from tasks.limpieza import Limpieza

from utils.manejador_bucket_gcp import list_cs_files_in_folder
from utils.manejador_bucket_gcp import leer_archivo_desde_gcp
from utils.configurar_logger import configurar_logger
from dotenv import load_dotenv
import warnings

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/luis/Downloads/proyecto-alquiler-en-lima-222da798c1fc.json'

warnings.filterwarnings('ignore')

logger = configurar_logger("logs/etl.log")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'proceso_etl',
        default_args=default_args,
        description='Proceso ETL para datos de departamentos y carga a BigQuery',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        params={
            "bucket_name": "bucket-proyectos",
            "folder_name": "data_alquileres_lima/rawdata"
        }
) as dag:

    def tarea_extraccion(**context):

        hora_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_guardado = f"data/json/data_json_{hora_actual}.json"

        bucket_name = context["params"].get("bucket_name")
        folder_name = context["params"].get("folder_name")

        archivos = list_cs_files_in_folder(bucket_name, folder_name)

        if not archivos or len(archivos) == 0:
            logger.error(f"No hay archivos en la carpeta '{folder_name}'")
            return None

        data_completa = []
        logger.info("Descargando todos los archivos")

        for i,archivo in enumerate(archivos[:20]):
            if not archivo.endswith(".json"):
                continue
            data = leer_archivo_desde_gcp(bucket_name, archivo)
            if isinstance(data, list):
                data_completa.extend(data)
            elif isinstance(data, dict):
                data_completa.append(data)
            else:
                print(f"Formato de datos no reconocido en el archivo {archivo}")
            if i%5 == 0:
                logger.info(f"Descargado {i+1} de {len(archivos)} archivos")

        with open(ruta_guardado, 'w', encoding='utf-8') as f:
            json.dump(data_completa, f, ensure_ascii=False, indent=4)

        context['ti'].xcom_push(key='archivo_json_ruta_local',value=ruta_guardado)


    def tarea_formateo(**context):
        ruta_json = context['ti'].xcom_pull(key='archivo_json_ruta_local',task_ids='tarea_extraccion')
        formateo = Formateo(ruta_lectura=ruta_json)
        formateo_ruta = formateo.realizar_formateo()
        context['ti'].xcom_push(key='formateo_ruta', value=formateo_ruta)

    def tarea_datawrangling(**context):
        formateo_ruta = context['ti'].xcom_pull(key='formateo_ruta', task_ids='tarea_formateo')
        datawrangling = Datawrangling(ruta_lectura=formateo_ruta)
        datawrangling_ruta = datawrangling.realizar_data_wrangling()
        context['ti'].xcom_push(key='datawrangling_ruta', value=datawrangling_ruta)

    def tarea_limpieza(**context):
        bucket_name = context["params"].get("bucket_name")
        datawrangling_ruta = context['ti'].xcom_pull(key='datawrangling_ruta', task_ids='tarea_datawrangling')
        datalimpieza = Limpieza(bucket_name=bucket_name,ruta_lectura=datawrangling_ruta)
        datalimpieza_ruta = datalimpieza.realizar_limpieza()
        context['ti'].xcom_push(key='datalimpieza_ruta', value=datalimpieza_ruta)

    tarea_extraccion_op = PythonOperator(
        task_id='tarea_extraccion',
        python_callable=tarea_extraccion,
        provide_context=True

    )

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
        bucket='{{ params.bucket_name }}',  # Referencia al parámetro 'bucket_name' definido en 'params',
        source_objects=["{{ ti.xcom_pull(task_ids='tarea_limpieza', key='datalimpieza_ruta') }}"],
        destination_project_dataset_table='proyecto-alquiler-en-lima.data_departamentos.data_lima',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE'
    )

    # Definir la secuencia de tareas
    tarea_extraccion_op >> tarea_formateo_op >> tarea_datawrangling_op >> tarea_limpieza_op >> carga_bq_op


