import pandas as pd
import numpy as np
import concurrent.futures
from datetime import datetime
from utils.manejador_bucket_gcp import upload_cs_file
from utils.configurar_logger import configurar_logger

logger = configurar_logger("../logs/datawrangling.log")


def comparar_valores(data_real,data_convertida,columnas):

    data_real_filtrada = data_real.loc[data_convertida.index,columnas.keys()]
    data_convertida_col = data_convertida.rename(columns=dict([(col,col+"_imp")for col in columnas.keys()]))
    data_comparacion = pd.concat([data_real_filtrada, data_convertida_col], axis=1)
    coincidencias = []

    for col,val in columnas.items():

        data_comparacion_col = data_comparacion.query(f"{col}.notna() and {col}_imp.notna()")

        # diferencia entre la columna real y la convertida
        data_comparacion_col[f'{col}_diff'] = np.abs(data_comparacion_col[col] - data_comparacion_col[f'{col}_imp'])

        # el total de elementos comparados
        total = data_comparacion_col.shape[0]

        # el total de elementos que cumplen con el error de tolerancia
        cumplen = 0
        if val<1 and val>0: # es una proporcion (el error es menor que el x% del valor)
            cumplen = data_comparacion_col.query(f'{col}_diff/{col} <= {val}').shape[0]
        elif val>=1 or val==0: # es un valor (el error es igual o menor que el valor)
            cumplen = data_comparacion_col.query(f'{col}_diff <= {val}').shape[0]
        else:
            print('El valor de comparación debe ser un número mayor que 0.')

        # proporcion de los que cumplen con la tolerancia del error
        coincidencias_col = cumplen/total
        coincidencias.append(coincidencias_col)
        print(f"El % de coincidencias para la columna '{col}': {coincidencias_col*100:.2f}% de {total}")

    return data_comparacion,coincidencias


def procesar_en_paralelo(func, items, workers=5):
    """Ejecuta consultas en paralelo usando múltiples hilos.

    Args:
        func: Función a ejecutar (obtener_coordenadas u obtener_precision)
        items: Lista de elementos a procesar
        workers: Número de hilos paralelos
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        resultados = executor.map(func, items)

    return list(resultados)


def guardar_csv_en_gcp(bucket_name, df_format, ruta_local):
    df_format.to_csv(ruta_local, index=False)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    ruta_folder_bucket = f"data/csv/data-formateada_{timestamp_str}.csv"
    upload_cs_file(bucket_name, ruta_local, ruta_folder_bucket)
    return ruta_folder_bucket