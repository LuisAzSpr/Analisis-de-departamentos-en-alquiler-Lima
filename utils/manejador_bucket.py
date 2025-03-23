import io
import os
import boto3
from botocore.client import Config
from dotenv import load_dotenv
import json
from utils.configurar_logger import configurar_logger
import pandas as pd


logger = configurar_logger("../logs/consumer.log")

load_dotenv()

session = boto3.session.Session()
SPACES_ENDPOINT = os.getenv("SPACES_ENDPOINT")
SPACES_ACCESS_KEY = os.getenv("SPACES_ACCESS_KEY")
SPACES_SECRET_KEY = os.getenv("SPACES_SECRET_KEY")
BUCKET_PROYECTOS = os.getenv("SPACES_BUCKET")



# Configura el cliente S3 de boto3 apuntando a DigitalOcean Spaces (región por defecto: 'nyc3')
s3_client = boto3.client(
    "s3",
    region_name="nyc3",
    endpoint_url=f"https://{SPACES_ENDPOINT}",
    aws_access_key_id=SPACES_ACCESS_KEY,
    aws_secret_access_key=SPACES_SECRET_KEY,
    config=Config(signature_version="s3v4"),
)


def subir_archivo_a_space(archivo_local: str, nombre_objeto_bucket: str = None):
    """
    Sube un archivo local a un Space de DigitalOcean (bucket fijo 'bucket-proyectos') usando boto3.

    :param archivo_local: Ruta local del archivo que deseas subir.
    :param nombre_objeto_bucket: Nombre con el que se guardará en el bucket (opcional).
    :return: True si se subió correctamente, False en caso de error.
    """

    # Si no se especifica un nombre, se usa el nombre base del archivo
    if not nombre_objeto_bucket:
        nombre_objeto_bucket = os.path.basename(archivo_local)

    try:
        s3_client.upload_file(
            Filename=archivo_local,
            Bucket=BUCKET_PROYECTOS,
            Key=nombre_objeto_bucket
        )
        logger.info(f"Archivo '{archivo_local}' subido correctamente a '{BUCKET_PROYECTOS}' como '{nombre_objeto_bucket}'.")
        return True
    except Exception as e:
        logger.info(f"Error al subir el archivo '{archivo_local}' a '{BUCKET_PROYECTOS}': {e}")
        return False


def listar_archivos_en_folder(folder: str):
    response = s3_client.list_objects_v2(Bucket=BUCKET_PROYECTOS, Prefix=folder)
    if "Contents" in response:
        file_keys = [obj['Key'] for obj in response["Contents"]]
        logger.info(f"Hay {len(file_keys)}  archivos en el folder")
        return file_keys
    else:
        logger.info("No se encontraron archivos en el folder.")
        return None


def leer_archivo_desde_space(nombre_archivo: str):
    try:
        obj = s3_client.get_object(Bucket=BUCKET_PROYECTOS, Key=nombre_archivo)
        file_data = obj["Body"].read()  # Contenido en bytes
        extension = nombre_archivo.split(".")[-1].lower()

        if extension == "json":
            return json.loads(file_data)
        elif extension == "txt":
            return file_data.decode("utf-8")
        elif extension == "csv":
            return pd.read_csv(io.StringIO(file_data.decode("utf-8")))
        else:
            # Si no reconocemos la extensión, devolvemos los bytes crudos
            return file_data

    except Exception as e:
        logger.info(f"[ERROR] Al leer el archivo '{nombre_archivo}' desde Spaces: {e}")
        return None


if __name__=="__main__":
    ruta_folder = "precios-depas-lima/rawdata/miraflores"
    archivos = listar_archivos_en_folder(ruta_folder)
    archivo = archivos[1]
    data=  leer_archivo_desde_space(archivo)
    print(data)

