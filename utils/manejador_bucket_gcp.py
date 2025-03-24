
# import packages
from google.cloud import storage
import os
import pandas as pd
import json
import io

# set key credentials file path
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = '/home/luis/Downloads/proyecto-alquiler-en-lima-222da798c1fc.json'


# define function that uploads a file from the bucket
def upload_cs_file(bucket_name, source_file_name, destination_file_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(destination_file_name)
    blob.upload_from_filename(source_file_name)

    return True


def list_cs_files(bucket_name):
    storage_client = storage.Client()

    file_list = storage_client.list_blobs(bucket_name)
    file_list = [file.name for file in file_list]

    return file_list


def download_cs_file(bucket_name, file_name, destination_file_name):
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(file_name)
    blob.download_to_filename(destination_file_name)

    return True


def list_cs_files_in_folder(bucket_name, folder_name):
    storage_client = storage.Client()

    blobs = storage_client.list_blobs(bucket_name, prefix=folder_name)
    file_list = [blob.name for blob in blobs]

    if file_list:
        print(f"Hay {len(file_list)} archivos en el folder {folder_name}")
        return file_list
    else:
        print(f"No se encontraron archivos en el folder {folder_name}")
        return None


def leer_archivo_desde_gcp(bucket_name: str, file_name: str):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        file_data = blob.download_as_bytes()
        extension = file_name.split(".")[-1].lower()

        if extension == "json":
            return json.loads(file_data)
        elif extension == "txt":
            return file_data.decode("utf-8")
        elif extension == "csv":
            return pd.read_csv(io.StringIO(file_data.decode("utf-8")))
        else:
            return file_data

    except Exception as e:
        print(f"[ERROR] Al leer el archivo '{file_name}' desde GCP: {e}")
        return None


def extraer_datos_de_folder_gcp(bucket_name, folder_name):
    archivos = list_cs_files_in_folder(bucket_name, folder_name)
    if not archivos or len(archivos) == 0:
        print(f"No hay archivos en la carpeta '{folder_name}'")
        return None

    data_completa = []
    for archivo in archivos:
        if not archivo.endswith(".json"):
            continue
        data = leer_archivo_desde_gcp(bucket_name, archivo)
        if isinstance(data, list):
            data_completa.extend(data)
        elif isinstance(data, dict):
            data_completa.append(data)
        else:
            print(f"Formato de datos no reconocido en el archivo {archivo}")

    return data_completa


if __name__=='__main__':
    jsons = extraer_datos_de_folder_gcp(
        "bucket-proyectos",
        "data_alquileres_lima/rawdata/barranco"
    )
    print(json.dumps(jsons, indent=2))



#lista = list_cs_files_in_folder("us-central1-composer-dev-lu-620fcc1f-bucket","data/rawdata/barranco")
    #for x in lista:
    #    print(f"{x}\n")

    '''download_cs_file(
        'us-central1-composer-dev-lu-620fcc1f-bucket',
        'data/rawdata/barranco/datos_departamentos_20250215_221210.json',
        '/home/luis/Downloads/datita_json.json'
    )'''

    '''upload_cs_file(
    'us-central1-composer-dev-lu-620fcc1f-bucket',
    '/home/luis/Downloads/cuenta_facturacion.csv',
    'facturacion/cuenta_facturacion1.csv'
    )
    '''