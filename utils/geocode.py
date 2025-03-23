
import os
import requests
import time
import numpy as np
import concurrent.futures
from dotenv import load_dotenv

load_dotenv()

GEOCODE_API_KEY = os.getenv("GEOCODE_API_KEY")


def obtener_direccion(coordenada):
    """Consulta la API de Reverse Geocoding y devuelve la dirección de las coordenadas."""
    lat = coordenada['latitud']
    lng = coordenada['longitud']
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={lat},{lng}&key={GEOCODE_API_KEY}"

    try:
        response = requests.get(url).json()
        time.sleep(1)
        if response["status"] == "OK":
            result = response["results"][0]
            direccion = result["formatted_address"]
            location_type = result["geometry"]["location_type"]
            type_location = result["types"][0]
            resultado = {"id": coordenada["id"], "direccion": direccion, "precision": location_type,"type":type_location}
            print(resultado)
            return resultado
        else:
            return {"id": coordenada["id"], "direccion": None, "precision": None,"type":None}
    except Exception as e:
        print(f"Error con las coordenadas {coordenada}: {e}")
        return {"id": coordenada["id"], "direccion": None, "precision": None,"type":None}


def obtener_coordenadas(direccion):
    """Consulta la API de Geocoding y devuelve las coordenadas de la dirección."""
    url = f"https://maps.googleapis.com/maps/api/geocode/json?address={direccion['direccion']}&key={GEOCODE_API_KEY}"
    try:
        response = requests.get(url).json()
        time.sleep(1)
        if response["status"] == "OK":
            result = response["results"][0]
            location = result["geometry"]["location"]
            location_type = result["geometry"]["location_type"]
            type_location = result["types"][0]
            resultado = {"id":direccion["id"], "latitud":location["lat"], "longitud":location["lng"],"precision":location_type,"type":type_location}
            print(resultado)
            return resultado
        else:
            return {"id":direccion["id"], "latitud":None, "longitud":None,"precision":None,"type":None}
    except Exception as e:
        print(f"Error con la dirección {direccion}: {e}")
        return {"id":direccion["id"], "latitud":None, "longitud":None,"precision":None,"type":None}


def haversine(coord1,coord2):
    lat1,lon1 = coord1
    lat2,lon2 = coord2
    R = 6371  # Radio de la Tierra en km
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    return R * c