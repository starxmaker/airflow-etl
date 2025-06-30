import os
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from functools import lru_cache
import requests
import time
import math
from pangres import upsert

# caché para no llamar siempre a la API
@lru_cache(maxsize=1000)
def get_coordinates(city_name):
    time.sleep(1) # requisito de la API
    print(f"Obteniendo coordenadas de {city_name}")
    base_url = "https://nominatim.openstreetmap.org/search"
    params = {
        'q': f"{city_name}, Chile",
        'format': 'json',
        'limit': 1
    }
    headers = {
        'User-Agent': os.getenv('APP_IDENTIFIER')
    }

    response = requests.get(base_url, params=params, headers=headers)
    data = response.json()

    if data:
        lat = float(data[0]['lat'])
        lon = float(data[0]['lon'])
        return lat, lon
    else:
        return None

# calcular nuevas coordenadas en base a coordenada base, distancia y dirección
def calculate_new_coordinates(lat, lon, distance_km, direction):
    lat_rad = math.radians(lat)
    lon_rad = math.radians(lon)

    R = 6371.0

    directions_to_bearings = {
        'N': 0,
        'NE': math.pi / 4,
        'E': math.pi / 2,
        'SE': 3 * math.pi / 4,
        'S': math.pi,
        'SO': 5 * math.pi / 4,
        'O': 3 * math.pi / 2,
        'NO': 7 * math.pi / 4
    }
    
    if direction not in directions_to_bearings:
        raise ValueError("Dirección invalida. Posibles opciones: f 'N', 'NE', 'E', 'SE', 'S', 'SO', 'O', 'NO'.")
    
    bearing = directions_to_bearings[direction]

    new_lat_rad = lat_rad + (distance_km / R) * math.cos(bearing)
    new_lat = math.degrees(new_lat_rad)
    
    new_lon_rad = lon_rad + (distance_km / R) * math.sin(bearing) / math.cos(lat_rad)
    new_lon = math.degrees(new_lon_rad)
    
    return new_lat, new_lon

# cargar variables de entorno
load_dotenv()

# cargar parametros de conexión
PG_HOST = os.getenv('POSTGRES_HOST')
PG_PORT = os.getenv('POSTGRES_PORT')
PG_DB = os.getenv('POSTGRES_DB')
PG_USER = os.getenv('POSTGRES_USER')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD')

# conectar a base de datos
DATABASE_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
engine = create_engine(DATABASE_URL)

# declarar fuente y desinto
source = './data/sismos'
target = os.path.join(source, "procesados")
os.makedirs(target, exist_ok=True)

# filtrar y ordenar archivos por fecha ASC
files = sorted([
    f for f in os.listdir(source)
    if f.endswith(".csv") and os.path.isfile(os.path.join(source, f))
])

try:
    for filename in files:
        path_file = os.path.join(source, filename)
        df = pd.read_csv(path_file)
        # crea un índice único en el dataframe por fecha
        df.set_index('Fecha', inplace=True)
        lats = []
        longs = []
        for index, row in df.iterrows():
            city_name = row['Lugar']
            # obtiene coordenadas de lugar base
            coordinates = get_coordinates(city_name)
            distance = float(row["DistanciaKm"])
            direction = row["Dirección"]
            # obtiene nuevas coordenadas sumado distancia y dirección
            new_coordinates =  calculate_new_coordinates(coordinates[0], coordinates[1], distance, direction)
            lats.append(new_coordinates[0])
            longs.append(new_coordinates[1])
        # agrega nuevos datos al dataframe
        df["Latitud"] = lats
        df["Longitud"] = longs
        
        # inserta si no existe, actualiza si existe
        upsert(con=engine,
            df=df,
            table_name='sismos',
            if_row_exists='update')
        
        # Mover archivo procesado
        new_path = os.path.join(target, filename)
        os.rename(path_file, new_path)
        print(f"Archivo {filename} procesado con éxito")
        
except Exception as e:
    print(f"Error procesando archivo: {str(e)}")
finally:
    # Cerrar base de datos
    engine.dispose()