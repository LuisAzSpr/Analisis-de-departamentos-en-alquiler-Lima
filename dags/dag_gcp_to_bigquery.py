
import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/luis/Downloads/first-outlet-454113-e2-e353fd319c22.json'

with DAG(
        'gcp_to_bigquery',
        default_args=default_args,
        description='Proceso para guardar los datos desde un bucket de gcp '
                    ' a bigquery para poder analizarlos graficamente',
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False
) as dag:

    def get_transform_query(source_table, destination_table, columns):
        transformed_expressions = []
        for col, posicion in columns.items():
            expr = (
                f"CAST(CONCAT("
                f"SUBSTR(REPLACE({col}, '.', ''), 1, {posicion}), '.', "
                f"SUBSTR(REPLACE({col}, '.', ''), {posicion+1})"
                f") AS FLOAT64) AS {col}"
            )
            transformed_expressions.append(expr)

        query = f"""
            CREATE OR REPLACE TABLE `{destination_table}` AS
            SELECT
              {', '.join(transformed_expressions)}
            FROM `{source_table}`
            """
        return query

    espera_etl = ExternalTaskSensor(
        task_id='espera_tarea_limpieza',
        external_dag_id='proceso_etl',
        external_task_id='tarea_limpieza',
        poke_interval=30,
        timeout=600,  # ajusta según tus necesidades
        mode='reschedule'
    )

    carga_bq_op = GCSToBigQueryOperator(
        task_id='carga_csv_a_bq',
        bucket='us-central1-composer-dev-lu-620fcc1f-bucket',
        source_objects=["{{ ti.xcom_pull(task_ids='tarea_limpieza', key='datalimpieza_ruta') }}"],
        destination_project_dataset_table='first-outlet-454113-e2.alquiler_depas_lima.precios_depas_lima',
        source_format='CSV',
        skip_leading_rows=1,
        field_delimiter=',',
        autodetect=True,
        allow_jagged_rows=True,
        ignore_unknown_values=True,
        write_disposition='WRITE_TRUNCATE'
    )

    tarea_limpieza_bq = PythonOperator(
        task_id='tarea_limpieza',
        python_callable=get_transform_query,
        provide_context=True
    )

    espera_etl >> carga_bq_op >> tarea_limpieza_bq