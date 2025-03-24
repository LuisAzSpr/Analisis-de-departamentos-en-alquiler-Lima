import pandas as pd
import numpy as np
from sklearn.cluster import DBSCAN
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import KFold
from sklearn.metrics import mean_absolute_percentage_error
from sklearn.ensemble import RandomForestRegressor
from sklearn.tree import DecisionTreeRegressor
from utils.configurar_logger import configurar_logger
from datetime import datetime
from utils.manejador_bucket_gcp import leer_archivo_desde_gcp
from utils.manejador_bucket_gcp import upload_cs_file


logger = configurar_logger("logs/logs.log")


class Limpieza:
    def __init__(self,bucket_name='', ruta_lectura=''):
        self.timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.bucket_name = bucket_name
        self.ruta_lectura = ruta_lectura
        self.df_transformaciones = []

    def realizar_limpieza(self):

        df = pd.read_csv(self.ruta_lectura)

        df_trans1 = self.pre_limpieza_analisis(df)
        df_trans2 = self.correccion_de_coordenadas_y_distrito(df_trans1)
        df_trans3 = self.correccion_de_preciosxm2_extremandamente_bajos(df_trans2)
        df_trans4 = self.tratamiento_de_duplicados(df_trans3)
        df_trans5 = self.tratamiento_de_nulos_en_area(df_trans4)
        df_trans6 = self.tratamiento_de_nulos_en_antiguedad(df_trans5)
        df_trans7 = self.ultima_limpieza_antes_de_EDA(df_trans6)

        ruta_local = f"data/csv/data_limpieza_{self.timestamp_str}.csv"
        ruta_gcp = f"data_alquileres_lima/csv/data_{self.timestamp_str}.csv"

        df_trans7.to_csv(ruta_local, index=False)
        upload_cs_file(self.bucket_name, ruta_local, ruta_gcp)

        return ruta_gcp

    def pre_limpieza_analisis(self, data):
        data_copia = data.copy()
        # Elimina duplicados por 'descripcion' (se ordena para inspección, aunque no se asigna el resultado)
        data_copia[data_copia.duplicated(subset=['descripcion'], keep=False)].sort_values(by='descripcion', ascending=False)
        # Elimina filas con nulos en variables críticas
        data_copia.dropna(subset=['precio', 'dormitorios', 'banos', 'fecha_publicacion', 'distrito'], inplace=True)
        porcentaje_eliminado = (data.shape[0] - data_copia.shape[0]) / data.shape[0]
        if porcentaje_eliminado > 0.1:
            logger.error(f"El % de nulos eliminados es {porcentaje_eliminado:.2%}")
        logger.info("1. Terminando pre-limpieza antes del análisis !!")
        self.df_transformaciones.append(data_copia)
        return data_copia

    def correccion_de_coordenadas_y_distrito(self, data):
        data_copia = data.copy()
        # 1. Corrección de departamentos cuyas coordenadas se alejan demasiado
        X = data_copia[['latitud', 'longitud']].dropna().copy()
        dbscan = DBSCAN(eps=1, metric='haversine', n_jobs=6)
        X['cluster'] = dbscan.fit_predict(X)
        X['outlier'] = np.where(X['cluster'] == -1, 1, 0)
        indices_outlier = X[X['outlier'] == 1].index
        data_copia.drop(index=indices_outlier, inplace=True)

        # 2. Corrección del distrito mediante KNN
        X_coords = data_copia[['latitud', 'longitud']].dropna().copy()
        y = data_copia.loc[X_coords.index,'distrito']
        knn = KNeighborsClassifier(n_neighbors=10, metric='haversine', n_jobs=6)
        knn.fit(X_coords, y)
        data_copia.loc[X_coords.index, 'distrito'] = knn.predict(X_coords)

        logger.info("2. Terminando corrección de coordenadas y distrito !!")
        self.df_transformaciones.append(data_copia)
        return data_copia

    def correccion_de_preciosxm2_extremandamente_bajos(self, data):
        data_copia = data.copy()
        distritos = data_copia['distrito'].unique()
        indices_out = []
        relaciones = []  # Se irán guardando las relaciones calculadas

        for distrito in distritos:
            data_distrito = data_copia.query("distrito == @distrito").copy()
            data_distrito['precioxm2'] = data_distrito['precio'] / data_distrito['area']
            mediana_precioxm2 = data_distrito['precioxm2'].median()
            data_distrito_out = data_distrito.query("2.1 * precioxm2 < @mediana_precioxm2")
            mediana_precioxm2_out = data_distrito_out['precioxm2'].median()
            relacion = np.round(mediana_precioxm2 / mediana_precioxm2_out if mediana_precioxm2_out else 0, 2)
            relaciones.append(relacion)
            logger.info(f"Relación en distrito {distrito}: {relacion}")
            indices_out += data_distrito_out.index.tolist()

        relaciones_m = (np.mean(relaciones) + np.median(relaciones)) / 2
        logger.info(f"Relación global entre preciosxm2: {relaciones_m}")
        if relaciones_m < 2:
            logger.error("La relación entre los preciosxm2 no indica que los datos con precioxm2 muy bajos estén en dólares.")
        data_copia.loc[indices_out, 'precio'] *= 3.7

        precioxm2 = data_copia['precio'] / data_copia['area']
        data_copia.drop(index=precioxm2[precioxm2 < 10].index, inplace=True)
        logger.info("3. Terminando corrección de precios extremadamente bajos !!")
        self.df_transformaciones.append(data_copia)
        return data_copia

    def tratamiento_de_duplicados(self, data):
        data_copia = data.copy()
        columnas = ['latitud', 'longitud', 'dormitorios', 'banos', 'area', 'precio']
        data_preprocessing = data_copia.dropna(subset=columnas)

        scaler = MinMaxScaler()
        X_transform = scaler.fit_transform(data_preprocessing[columnas])
        dbscan = DBSCAN(eps=0.005)
        clusters = dbscan.fit_predict(X_transform)
        data_preprocessing['cluster'] = clusters
        data_groupby = data_preprocessing.query("cluster != -1") \
            .groupby('cluster').agg({col: 'mean' for col in columnas})

        data_preprocessing_rep = data_preprocessing[data_preprocessing['cluster'] != -1].copy()
        clusters_rep = data_preprocessing_rep['cluster'].unique()
        for c in clusters_rep:
            cluster_mean = data_groupby.loc[c]
            data_preprocessing_rep.loc[data_preprocessing_rep['cluster'] == c, columnas] = cluster_mean.values

        # Actualiza las filas en el DataFrame original
        data_copia.update(data_preprocessing_rep)
        data_copia.drop_duplicates(subset=columnas, keep='first', inplace=True)
        data_copia.drop_duplicates(subset=['area', 'dormitorios', 'banos', 'descripcion'], keep='first', inplace=True)
        logger.info("4. Terminando tratamiento de duplicados !!")
        self.df_transformaciones.append(data_copia)
        return data_copia

    def tratamiento_de_nulos_en_area(self, data):
        if data['area'].isnull().sum()==0:
            print("No existen valores nulos en area")
            return data
        data_copia = data.copy()
        rfr = RandomForestRegressor(n_estimators=60, max_depth=20, min_samples_leaf=3)

        not_na = data_copia.query("area.notna()")
        is_na = data_copia.query("area.isna()")
        X = not_na[['banos', 'dormitorios', 'antiguedad', 'longitud', 'latitud']]
        y = not_na['area']

        kfold = KFold(n_splits=10, shuffle=False)
        mapes_rfr = []
        mapes_mean = []

        for train_index, test_index in kfold.split(X, y):
            X_train, X_test = X.iloc[train_index], X.iloc[test_index]
            y_train, y_test = y.iloc[train_index], y.iloc[test_index]
            rfr.fit(X_train, y_train)
            y_pred = rfr.predict(X_test)
            mapes_rfr.append(mean_absolute_percentage_error(y_test, y_pred))
            mapes_mean.append(mean_absolute_percentage_error(y_test, [np.mean(y_train)] * len(y_test)))

        logger.info(f"MAPE RFR promedio: {np.mean(np.sort(mapes_rfr)[1:-1])}")
        logger.info(f"MAPE imputación media promedio: {np.mean(np.sort(mapes_mean)[1:-1])}")
        data_copia.loc[is_na.index, 'area'] = rfr.predict(is_na[['banos', 'dormitorios', 'antiguedad', 'longitud', 'latitud']])
        logger.info("5. Terminando tratamiento de nulos en área !!")
        self.df_transformaciones.append(data_copia)
        return data_copia

    def tratamiento_de_nulos_en_antiguedad(self, data):
        if data['antiguedad'].isnull().sum()==0:
            print("No existen valores nulos en antiguedad")
            return data
        data_copia = data.copy()
        # Ajustar un árbol de decisión simple
        tree = DecisionTreeRegressor(max_depth=2)
        tree.fit(data_copia[['antiguedad']], data_copia['precio'])
        thresholds = np.sort(tree.tree_.threshold[tree.tree_.threshold > 0])
        bins = [-1] + list(thresholds) + [data_copia['antiguedad'].max() + 1]
        labels = [f'Grupo {i+1}' for i in range(len(bins) - 1)]
        data_copia['antiguedad_categoria'] = pd.cut(data_copia['antiguedad'], bins=bins, labels=labels)
        data_copia['antiguedad_categoria'] = np.where(data_copia['antiguedad_categoria'].isna(),
                                                      "desconocido",
                                                      data_copia['antiguedad_categoria'])
        data_copia.drop('antiguedad', axis=1, inplace=True)
        logger.info("6. Terminando tratamiento de nulos en antigüedad !!")
        self.df_transformaciones.append(data_copia)
        return data_copia

    def ultima_limpieza_antes_de_EDA(self, data):
        data_copia = data.copy()
        data_copia['precioxm2'] = data_copia['precio'] / data_copia['area']
        # 1. Elimina valores atípicos extremos
        data_copia.drop(index=data_copia.query("dormitorios > 6 or area > 600 or precioxm2 > 120 or precioxm2 < 8").index, inplace=True)
        data_copia.drop(index=data_copia.query("dormitorios <= 2 and banos == 1 and area >= 160").index, inplace=True)
        data_copia.drop(index=data_copia.query("dormitorios <= 2 and banos == 2 and area >= 250").index, inplace=True)
        data_copia.drop(index=data_copia.query("precio < 600").index, inplace=True)
        data_copia.drop(columns=['descripcion'],inplace=True)

        # 2. Elimina distribuciones inusuales de dormitorios y baños
        data_copia["dorm_ban"] = data_copia[['dormitorios', 'banos']].apply(lambda x: f"{x[0]},{x[1]}", axis=1)
        count = data_copia["dorm_ban"].value_counts().sort_values(ascending=True)
        distribuciones_raras = count[count <= 3].index
        indices = []
        for distrib in distribuciones_raras:
            dorm, ban = distrib.split(",")
            indices += data_copia.query("dormitorios == @dorm and banos == @ban").index.tolist()
        data_copia.drop(index=indices, inplace=True)

        # 3. Elimina residuos de preciosxm2 demasiado bajos
        data_distrito_out = data_copia.groupby('distrito')['precioxm2'].apply(
            lambda x: x.quantile(0.75) + 2.5 * (x.quantile(0.75) - x.quantile(0.25))
        )
        for dist, max_val in data_distrito_out.items():
            outliers = data_copia.query("distrito == @dist and precioxm2 > @max_val")
            data_copia.drop(index=outliers.index, inplace=True)

        logger.info("7. Terminando última limpieza antes de EDA !!")
        self.df_transformaciones.append(data_copia)

        return data_copia
