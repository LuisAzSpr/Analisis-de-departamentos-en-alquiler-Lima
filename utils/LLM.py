
import os
from dotenv import load_dotenv
from openai import OpenAI
import json
import concurrent.futures
import time
import re
import ast


def funcion_gpt(input_data, system_message, user_message):
    """Envía un lote a la API de OpenAI y retorna el objeto `response`."""
    load_dotenv()
    json_input_data = json.dumps(input_data, ensure_ascii=False)
    user_message = user_message.format(json_input_data)

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    time.sleep(5)  # Espera base para cumplir con límites de tasa

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": user_message},
        ]
    )
    return response


def formatear_json(respuesta_gpt):
    """Extrae y parsea JSON de manera robusta con múltiples estrategias."""
    respuesta = respuesta_gpt.choices[0].message.content.strip()

    # Mejorar la extracción del JSON con regex más flexible
    json_match = re.search(r'(?s)(\{.*?\}|\[.*?\])', respuesta)
    if json_match:
        respuesta = json_match.group(1)

    # Normalización del contenido
    respuesta = respuesta.replace("None", "null")
    respuesta = respuesta.replace("'", '"')  # Intento de corregir comillas simples

    # Intentar múltiples métodos de parseo
    try:
        return json.loads(respuesta)
    except json.JSONDecodeError:
        try:
            # Intentar como literal Python (para manejar comillas simples y None)
            data = ast.literal_eval(respuesta)
            # Convertir a JSON válido
            return json.loads(json.dumps(data))
        except Exception as e:
            print(f"Error en parseo alternativo: {e}")
            return None


def procesar_batch(input_data, system_message, user_message, max_retries=3):
    """Procesa un lote con reintentos y manejo de errores mejorado."""
    for retry in range(max_retries + 1):
        try:
            response = funcion_gpt(input_data, system_message, user_message)
            parsed = formatear_json(response)
            if parsed:
                return parsed
            print(f"Reintento {retry + 1} por respuesta vacía")
        except Exception as e:
            print(f"Reintento {retry + 1} error: {str(e)}")

        if retry < max_retries:
            wait = 2 ** (retry + 1)  # Espera exponencial
            print(f"Esperando {wait}s para reintento...")
            time.sleep(wait)

    print(f"Batch fallido después de {max_retries} reintentos")
    return None


def procesamiento_batch_paralelo(input_data, k, system_message, user_message, max_workers=5, max_retries=3):
    """Procesamiento paralelo con manejo mejorado de errores."""
    batches = [input_data[i:i + k] for i in range(0, len(input_data), k)]

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                procesar_batch,
                batch,
                system_message,
                user_message,
                max_retries
            ): batch for batch in batches
        }

        for future in concurrent.futures.as_completed(futures):
            batch_result = future.result()
            if batch_result:
                results.extend(batch_result)

    return results
