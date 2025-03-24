import numpy as np
import pandas as pd
import re
from utils.LLM import procesamiento_batch_paralelo
from utils.general import comparar_valores
from utils.geocode import obtener_coordenadas
from utils.general import procesar_en_paralelo
from utils.geocode import obtener_direccion
from utils.manejador_bucket_gcp import leer_archivo_desde_gcp
from utils.manejador_bucket_gcp import upload_cs_file
from utils.configurar_logger import configurar_logger
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

logger = configurar_logger("logs/logs.log")


class Datawrangling:

    def __init__(self,ruta_lectura=''):
        self.timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.ruta_lectura = ruta_lectura
        self.df_transformaciones = []

        self.clasificacion_mensajes_sm = '''
         Eres un asistente experto en clasificar propiedades apartir de su descripcion.
         Tu objetivo es analizar el texto y determinar la propiedad que es:
          1. propiedad : Departamento, Local comercial, oficina, terreno, casa, cochera, otro. Existen 3 posibles salidas para esta variable:
         - 'departamento': Abarcar todos los tipos de departamento (mini departamento, flat o solo un piso, duplex o de dos pisos, triplex o de tres pisos,penthouse o pent house, etc.)
         - 'no_departamento': Local comercial, oficina, terreno, casa, cochera, otro.
         - null : Si no se puede determinar.
         2. propuesta: El tipo de propuesta que se esta haciendo, Existen 4 posibles salidas para esta variable:
         - 'venta': Unicamente se ofrece venta.
         - 'alquiler': Unicamente se ofrece alquiler.
         - 'alquiler y venta' : Se ofrecen ambos.
         - null : Si no se puede determinar.
         
         El resultado debe devolverse exclusivamente en formato JSON con la siguiente estructura, sin ningun comentario, ni anotaciones, ni explicaciones, SOLO EL JSON
        [{'id':<id>, 'variable':<variable>,'propiedad':<propiedad_o_null>,'propuesta':<tipo_proupuesta_o_null>},...]
        '''

        self.clasificacion_mensajes_um = '''
        Aquí tienes una lista de propiedades en formato JSON:\n
        {}\n\n
        Devuélveme únicamente el JSON en el formato especificado, sin comentarios, ni anotaciones, ni explicaciones adicionales.
        '''

        self.extraer_informacion_sm = (
            "Eres un asistente experto en procesar descripciones de propiedades en español. "
            "Tu tarea es extraer y determinar: número de dormitorios, baños, el área y la antiguedad de cada propiedad. "
            "En el texto, busca los valores de área (sinónimos: area, superficie, m2, metros, etc.) -> Ejemplos: '80m²', 'superficie 120 m²', 'area 120', etc.; "
            "dormitorios (sinónimos: dormitorios, cuartos, hab, habitaciones, etc.) -> Ejemplos: '2 dormitorios', '3 habitaciones', '2 dorm'; "
            "baños (sinónimos: baño, baños, wc, aseos, etc.) -> Ejemplos: '1 baño', '2 baños', etc.; "
            "antiguedad (sinónimos: antiguedad, edad, año, año de construcción, etc.) -> Ejemplos: '10 años', 'año de construccion 2015', etc.; "
            "Si no encuentras el dato en la descripción, devuelve None. "
            "Devuelve únicamente un JSON con la siguiente estructura: "
            "[{'id': <id>, 'area': <valor_o_None>, 'dormitorios': <valor_o_None>, 'banos': <valor_o_None>, 'antiguedad': <valor_o_None>},...]."
        )

        self.extraer_informacion_um = (
            "Aquí tienes la siguiente lista de propiedades en formato JSON:\n"
            "{}\n\n"  # Placeholder para .format(json_input_data)
            "Por favor, extrae el área, los dormitorios, los baños y antiguedad (o año de construccion) de cada descripción. "
            "- Devuelve únicamente el JSON en el formato solicitado, cada valor numerico sin ninguna unidad"
        )

        self.extraer_ubicacion_sm = (
            "Eres un asistente experto en la indentificacion y extraccion de direcciones de propiedades en español, específicamente para los distritos de Lima, Perú. "
            "Tu tarea es extraer las direcciones lo más exactas posibles:\n\n"
            " El formato de cada direccion para cada propiedad debe ser '<direccion>, distrito, Lima, Perú.' o null. "
            "EJEMPLOS: "
            " - Av. Venezuela 3252, Urb. Los Cipreses, Cercado de Lima, Lima, Perú "
            " - Av. Universitaria 1801, San Miguel, Lima, Perú "
            " - Av. Grau 651, Barranco, Lima, Perú "
            "El resultado debe ser devuelto únicamente en formato JSON, con la siguiente estructura:\n"
            "[{'id': <id>, 'direccion':<direccion_o_null>},...].\n"
        )

        self.extraer_ubicacion_um = (
            "Aquí tienes la siguiente lista de propiedades en formato JSON:\n"
            "{}\n\n"  # Placeholder para .format(json_input_data)
            "Por favor, corrige la dirección de cada propiedad según las instrucciones proporcionadas.\n"
            "- Si una dirección no puede corregirse alterando lo mínimo posible, asígnale el valor null.\n"
            "- Devuélveme únicamente el JSON en el formato especificado, sin ningún comentario o explicación adicional."
        )

    def realizar_data_wrangling(self):

        df = pd.read_csv(self.ruta_lectura)

        df_trans0 = self.formateo(df)
        df_trans1 = self.clasificacion_y_tratamiento_de_propiedades_con_data_inconsitente(df_trans0)
        df_trans2 = self.obtener_direccion_y_precision_de_coordenadas(df_trans1)
        df_trans3 = self.imputacion_nulos_usando_la_descripcion(df_trans2)
        df_trans4 = self.imputacion_coordenadas_nulas_con_direccion(df_trans3)

        ruta_local = f"data/csv/data_wrangling_{self.timestamp_str}.csv"
        df_trans4.to_csv(ruta_local,index=False)

        return ruta_local

    def formateo(self, data):
        """
        Aplica transformaciones sobre columnas relacionadas con precios, distritos y antigüedad.
        """
        data_copia = data.copy()
        # 1. Ajuste de precio_soles según la moneda
        data_copia['precio_soles'] = np.where(
            data_copia['moneda'] == 'US$',
            3.7 * data_copia['precio_soles'],
            data_copia['precio_soles']
        )
        # 2. Usa precio_dolares si precio_soles es nulo
        data_copia['precio_soles'] = np.where(
            data_copia['precio_soles'].isna(),
            3.7 * data_copia['precio_dolares'],
            data_copia['precio_soles']
        )
        # 3. Corrección de distritos
        data_copia['distrito'] = np.where(
            data_copia['distrito'].str.lower() == 'chorillos',
            'chorrillos',
            data_copia['distrito']
        )
        # 4. Ajuste de antigüedad
        data_copia['antiguedad'] = np.where(
            data_copia['antiguedad'] > 1e3,
            2025 - data_copia['antiguedad'],
            data_copia['antiguedad']
        )
        # 5. Eliminación de columnas innecesarias
        data_copia.drop(
            columns=['precio_dolares', 'banios/2', 'estacionamiento', 'area_cubierta', "moneda"],
            inplace=True
        )
        # 6. Renombrar columnas
        data_copia.rename(
            columns={"precio_soles": "precio", "area_total": "area"},
            inplace=True
        )
        # 7. Eliminar duplicados
        data_copia.drop_duplicates(
            subset=['latitud', 'longitud', 'area', 'dormitorios', 'banos', 'precio', 'distrito', 'antiguedad'],
            inplace=True
        )

        logger.info("1. Terminando formateo!!")
        self.df_transformaciones.append(data_copia)

        return data_copia

    def clasificacion_y_tratamiento_de_propiedades_con_data_inconsitente(self, data):
        data_copia = data.copy()
        queries = ["area < 25", "area > 700", "precio < 200", "precio > 2e4", "antiguedad > 50", "dormitorios > 7", "banos > 7"]
        exp = re.compile(r"\w+")
        entradas = []

        for querie in queries:
            variable = exp.findall(querie)[0]
            indices = data_copia.query(querie).index
            entradas += [{
                "id": f[0],
                "descripcion": f[1]['descripcion'],
                "variable": variable
            } for f in data_copia.loc[indices].iterrows()]

        clasificacion_de_propiedades = procesamiento_batch_paralelo(
            input_data=entradas,
            k=20,
            system_message=self.clasificacion_mensajes_sm,
            user_message=self.clasificacion_mensajes_um,
            max_workers=5
        )
        clasificacion_prop_df = pd.DataFrame(clasificacion_de_propiedades).set_index('id')

        # 1. Propiedades que no son departamentos
        indices1 = clasificacion_prop_df.query("propiedad != 'departamento'").index

        # 2. Verificar proporción de precios para departamentos en venta vs alquiler
        indices_no_alquiler = clasificacion_prop_df.query("propiedad=='departamento' and propuesta!='alquiler'").index
        indices_alquiler = clasificacion_prop_df.query("propiedad=='departamento' and propuesta=='alquiler'").index
        proporcion = data.loc[indices_no_alquiler, 'precio'].median() / data.loc[indices_alquiler, 'precio'].median()
        if np.round(proporcion) < 20:
            logger.error("Los clasificados como 'venta' no son considerablemente más caros que los de alquiler. Verificar información")
        logger.info(f"Los clasificados como 'venta' cuestan {np.round(proporcion, 0)} veces más que los de alquiler")
        indices2 = indices_no_alquiler

        # 3. Eliminar índices no deseados
        indices_drop = list(set(indices1) | set(indices2))
        data_copia.drop(index=indices_drop, inplace=True)

        # 4. Para propiedades que concuerdan, asignar valor nulo en la variable inconsistente
        for indice, fila in clasificacion_prop_df.loc[indices_alquiler].iterrows():
            variable = fila['variable']
            data_copia.at[indice, variable] = None

        logger.info("2. Terminando clasificación de propiedades con data inconsistente!!")
        self.df_transformaciones.append(data_copia)

        return data_copia

    def obtener_direccion_y_precision_de_coordenadas(self, data):
        data_coord = data.query("latitud.notna() and longitud.notna()")
        data_coord_json = [
            {"id": idx, "latitud": fila["latitud"], "longitud": fila["longitud"]}
            for idx, fila in data_coord[['latitud', 'longitud']].iterrows()
        ]
        direcciones_precision = procesar_en_paralelo(obtener_direccion, data_coord_json, 6)
        direc_precision_df = pd.DataFrame(direcciones_precision).set_index('id')
        data_merge = pd.merge(data, direc_precision_df, left_index=True, right_index=True, how='left')

        logger.info("3. Terminando obtención de dirección y precisión de coordenadas!!")

        self.df_transformaciones.append(data_merge)

        print(data_merge['precision'])

        logger.info(data_merge['type'].isnull().sum())
        logger.info(data_merge['precision'].isnull().sum())

        return data_merge

    def imputacion_nulos_usando_la_descripcion(self, data):

        data_copia = data.copy()
        data_null = data_copia.query("banos.isna() or dormitorios.isna() or antiguedad.isna() or area.isna()")
        notnull_size = data_copia.shape[0] - data_null.shape[0]
        pruebasize = int(notnull_size if notnull_size < 100 else 100 + (notnull_size - 100) / 2)
        data_prueba = data_copia.query("antiguedad.notna()").sample(pruebasize)
        data_null = pd.concat([data_null, data_prueba], axis=0)
        descripciones_para_procesar = [
            {"id": idx, "descripcion": descripcion}
            for idx, descripcion in data_null['descripcion'].items() if descripcion
        ]

        imputaciones = procesamiento_batch_paralelo(
            input_data=descripciones_para_procesar,
            k=20,
            system_message=self.extraer_informacion_sm,
            user_message=self.extraer_informacion_um,
            max_workers=4
        )
        imputaciones_df = pd.DataFrame(imputaciones).set_index('id')
        for col in imputaciones_df.columns:
            imputaciones_df[col] = pd.to_numeric(imputaciones_df[col], errors='coerce')

        imputaciones_df['antiguedad'] = np.where(
            imputaciones_df['antiguedad'] > 1e3,
            2025 - imputaciones_df['antiguedad'],
            imputaciones_df['antiguedad']
        )

        tolerancia = {
            "area": 0.1,        # tolerancia del 10% del valor real
            "dormitorios": 0,   # concordancia exacta
            "banos": 0,         # concordancia exacta
            "antiguedad": 2     # tolerancia de 2 años
        }

        data_comparacion, coincidencias = comparar_valores(data_copia, imputaciones_df, tolerancia)
        for col in tolerancia.keys():
            data_comparacion[f'{col}_fin'] = np.where(
                data_comparacion[col].isna(),
                data_comparacion[f'{col}_imp'],
                data_comparacion[col]
            )
            numero_reducido = data_comparacion[col].isnull().sum() - data_comparacion[f'{col}_fin'].isnull().sum()
            logger.info(f"La variable {col} redujo {numero_reducido} nulos")

        for coincidencia in coincidencias:
            if coincidencia < 0.7:
                logger.error("El % de aciertos es muy bajo")
                # Se podría lanzar una excepción aquí si es necesario

        columnas = list(tolerancia.keys())
        data_copia.loc[data_comparacion.index, columnas] = data_comparacion[[f'{col}_fin' for col in columnas]].values
        logger.info("4. Terminando imputación de nulos de variables básicas con la descripción!!")
        self.df_transformaciones.append(data_copia)
        return data_copia

    def imputacion_coordenadas_nulas_con_direccion(self, data):

        if data['latitud'].isnull().sum() == 0 and data['longitud'].isnull().sum() == 0:
            logger.info("NO Hay valores nulos en coordenadas")
            return data

        data_copia = data.copy()
        data_null = data_copia.query("latitud.isna() and longitud.isna()")
        direcciones_para_procesar = [
            {"id": idx, "direccion": direccion}
            for idx, direccion in data_null['descripcion'].items() if direccion
        ]

        direcciones = procesamiento_batch_paralelo(
            direcciones_para_procesar,
            k=20,
            system_message=self.extraer_ubicacion_sm,
            user_message=self.extraer_ubicacion_um,
            max_workers=5
        )

        direcciones_df = pd.DataFrame(direcciones).set_index('id')
        direcciones_df_join = pd.concat(
            [direcciones_df, data_copia.loc[direcciones_df.index, 'distrito']],
            axis=1
        )
        direcciones_coord = []
        for idx, row in direcciones_df_join.iterrows():
            direccion = row['direccion']
            distrito = row['distrito']
            nueva_direccion = f"{distrito}, Lima, Perú." if pd.isnull(direccion) or direccion in ['null', ''] else direccion
            direcciones_coord.append({"id": idx, "direccion": nueva_direccion})

        coordenadas = procesar_en_paralelo(obtener_coordenadas, direcciones_coord, 6)
        coordenadas_df = pd.DataFrame(coordenadas).set_index('id').rename(
            columns={"latitud": "latitud_api", "longitud": "longitud_api"}
        )
        data_coord = data_copia.loc[coordenadas_df.index, ['latitud', 'longitud']]
        coord_df_comparacion = pd.concat([data_coord, coordenadas_df], axis=1)

        columnas_originales = ['latitud', 'longitud', 'direccion', 'precision', 'type']
        columnas_nuevas = ['latitud_api', 'longitud_api', 'direccion', 'precision', 'type']
        direcciones_coord_df = pd.DataFrame(direcciones_coord).set_index('id')
        direcciones_procesadas_df = pd.merge(
            direcciones_coord_df,
            coord_df_comparacion,
            left_index=True,
            right_index=True,
            how="left"
        )
        indices = direcciones_procesadas_df.query("latitud.isna() and latitud_api.notna()").index
        data_copia.loc[indices, columnas_originales] = direcciones_procesadas_df.loc[indices, columnas_nuevas].values

        logger.info("5. Terminando imputación de nulos de coordenadas con la descripción!!")
        self.df_transformaciones.append(data_copia)
        return data_copia
