import pandas as pd
import re
import numpy as np
from functools import reduce
from utils.manejador_bucket_gcp import list_cs_files_in_folder
from utils.manejador_bucket_gcp import upload_cs_file
from utils.configurar_logger import configurar_logger
from utils.manejador_bucket_gcp import leer_archivo_desde_gcp
from datetime import datetime
from utils.configurar_logger import configurar_logger

logger = configurar_logger("../logs/formateo.log")


class Formateo():
    def __init__(self,
                 bucket_name='us-central1-composer-dev-lu-620fcc1f-bucket',
                 ruta_lectura=''):
        self.timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.bucket_name = bucket_name
        self.ruta_lectura = ruta_lectura
        self.df_transformaciones = []
        self.logger = configurar_logger("../logs/formateo.log")

    def realizar_formateo(self):
        data_json = self.extraer_datos_de_folder_gcp(self.bucket_name,self.ruta_lectura)

        data = self.convertir_a_dataframe(data_json)
        data_preformato = self.pre_formateo(data)
        data_formateada = self.formateo(data_preformato)

        ruta_local, ruta_gcp = "../csv/data_formateada.csv", f"data/csv/data_formateada_{self.timestamp_str}.csv"
        data_formateada.to_csv(ruta_local,index=False)
        upload_cs_file(self.bucket_name,ruta_local,ruta_gcp)

        return ruta_gcp

    def extraer_datos_de_folder_gcp(self, bucket_name, folder_name):
        archivos = list_cs_files_in_folder(bucket_name, folder_name)
        if not archivos or len(archivos) == 0:
            print(f"No hay archivos en la carpeta '{folder_name}'")
            return None

        data_completa = []
        logger.info("Descargando todos los archivos")
        for i,archivo in enumerate(archivos):
            if not archivo.endswith(".json"):
                continue
            data = leer_archivo_desde_gcp(bucket_name, archivo)
            if isinstance(data, list):
                data_completa.extend(data)
            elif isinstance(data, dict):
                data_completa.append(data)
            else:
                print(f"Formato de datos no reconocido en el archivo {archivo}")
            if i//10==0:
                logger.info(f"Descargado {i+1} de {len(archivos)} archivos")

        return data_completa

    def convertir_a_dataframe(self, data):
        variables_comunes = reduce(
            lambda x, y: x & set(y.keys()),
            data,
            set(data[0].keys())
        )

        data_comun = reduce(
            lambda x, y: x + [{k: y[k] for k in variables_comunes if k in y}],
            data,
            []
        )

        self.df_transformaciones.append(data_comun)

        return pd.DataFrame(data_comun)

    def coordenadas_format(self, df: pd.DataFrame, lat_lon: list) -> np.array:
        '''
        :param df:
        :param lat_lon:
        :return:
        '''
        coordenadas = df[lat_lon].values
        coordenadas_f = []
        for coord in coordenadas:
            if coord[0] is None or coord[1] is None:
                coordenadas_f.append(np.array([None, None]))
                continue
            lat_f = float(coord[0].replace(",", "."))
            lon_f = float(coord[1].replace(",", "."))
            coordenadas_f.append(np.array([lat_f, lon_f]))
        return np.array(coordenadas_f)


    def pre_formateo(self, df: pd.DataFrame) -> pd.DataFrame:
        df_copia = df.copy()

        # fecha_publicacion : convertiremos todos los valores a "hace \d+ días"
        df_copia["fecha_publicacion"] = df_copia["fecha_publicacion"].replace(
            {
                "Publicado desde ayer": "hace 1 día",
                "Publicado hace más de 1 año": "hace 365 días",
                "Publicado hoy": "hace 0 días"
            }
        )
        df_copia["fecha_publicacion"] = np.where(df_copia["fecha_publicacion"].str.contains("minutos"), "hace 0 días",
                                                 df_copia["fecha_publicacion"])

        # antiguedad
        df_copia['antiguedad'] = np.where(df_copia['antiguedad'] == 'A estrenar', '0 años', df_copia['antiguedad'])

        # precio (soles o dolares)
        df_copia['moneda'] = [x[:3] if x else None for x in df_copia['precio_soles'].values]
        df_copia['precio_soles'] = (df_copia['precio_soles'].str.replace(",", "")
                                    .str.replace(".", ""))
        df_copia['precio_dolares'] = (df_copia['precio_dolares'].str.replace(",", "")
                                      .str.replace(".", ""))
        df_copia['precio_dolares'] = np.where(df_copia['precio_dolares'] == '', None, df_copia['precio_dolares'])

        # latitud y longitud
        df_copia['latitud'] = df_copia['latitud'].str.replace(",", ".")
        df_copia['longitud'] = df_copia['longitud'].str.replace(",", ".")

        # restringir
        df_copia['antiguedad'] = np.where(df_copia['antiguedad'].str.lower().str.contains("año"),
                                          df_copia['antiguedad'], None)
        df_copia['dormitorios'] = np.where(df_copia['dormitorios'].str.lower().str.contains("dorm."),
                                           df_copia['dormitorios'], None)
        df_copia['banos'] = np.where(df_copia['banos'].str.lower().str.contains("baño"),
                                     df_copia['banos'], None)
        df_copia['area_total'] = np.where(df_copia['area_total'].str.lower().str.contains("m²"),
                                          df_copia['area_total'], None)

        self.df_transformaciones.append(df_copia)

        return df_copia


    def integer_format(self, exp: re.search, df: pd.DataFrame, columna: str) -> list:
        '''
        :param exp: Expresion regular que buscara un patron especifico segun la estructura de la variable.
        :param df: Dataframe con los datos a procesar
        :param columna: Variable a procesar
        :return: Una lista con los valores convertidos a enteros segun la expresion regular.
        '''
        valores = df[columna].values
        valores_f = []
        for valor in valores:
            if valor is None:
                valores_f.append(None)
                continue
            try:
                match = exp.findall(valor.lower())
                valor_f = match[0] if type(match[0]) is not tuple else reduce(lambda x, y: x + y, match[0])
                valores_f.append(int(valor_f))
            except IndexError:
                self.logger.warning(f"Formato incorrecto en la columna '{columna}' para el valor '{valor.lower()}'")
                valores_f.append(None)
            except ValueError:
                self.logger.warning(f" {valor} no se puede convertir a entero en '{columna}'")
                valores_f.append(None)

        return valores_f


    def formateo(self, df: pd.DataFrame) -> pd.DataFrame:
        '''
        :param df: Dataframe con las variables a formatear
        :return: Dataframe con las variables formateadas
        '''
        df_copia = df.copy()
        df_copia[['latitud', 'longitud']] = self.coordenadas_format(df_copia, ['latitud', 'longitud'])
        df_copia['dormitorios'] = self.integer_format(re.compile(r"(\d+) dorm."), df_copia, "dormitorios")
        df_copia['banos'] = self.integer_format(re.compile(r"(\d+) baño"), df_copia, "banos")
        df_copia['banios/2'] = self.integer_format(re.compile(r"(\d+) medios? baños?"), df_copia, "banios/2")
        df_copia['antiguedad'] = self.integer_format(re.compile(r"(\d+) años|año de construcción\n(\d+)"), df_copia,
                                                     "antiguedad")
        df_copia['area_total'] = self.integer_format(re.compile(r"(\d+) m²"), df_copia, "area_total")
        df_copia['area_cubierta'] = self.integer_format(re.compile(r"(\d+) m²"), df_copia, "area_cubierta")
        df_copia['estacionamiento'] = self.integer_format(re.compile(r"(\d+) estac."), df_copia, "estacionamiento")
        df_copia['precio_soles'] = self.integer_format(re.compile(r"s/\s*(\d+)|us\$(\d+)"), df_copia, "precio_soles")
        df_copia['precio_dolares'] = self.integer_format(re.compile(r"usd\s*(\d+)"), df_copia, "precio_dolares")
        df_copia['fecha_publicacion'] = self.integer_format(re.compile(r"(\d+) días?"), df_copia, "fecha_publicacion")
        df_copia[['latitud', 'longitud']] = df_copia[['latitud', 'longitud']].astype(float)

        self.df_transformaciones.append(df_copia)

        return df_copia