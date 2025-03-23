import logging


def configurar_logger(archivo_log):
    logger = logging.getLogger()
    if not logger.hasHandlers():  # Evitar agregar múltiples handlers
        logging.basicConfig(
            level=logging.INFO,  # Cambia esto a INFO para ocultar los DEBUG
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.StreamHandler(),  # Salida estándar
                logging.FileHandler(archivo_log, mode="a")  # Archivo de logs
            ]
        )
    return logger