from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="clima_Ciudades",
    default_args=default_args,
    description="Proceso de ingesta para captura de datos de clima",
    schedule="0 */2 * * *",
    catchup=False
)

def obtener_datos_clima():
    ciudades= {
            "Santiago": {"lat": -33.45, "lon": -70.66},
            "Valparaiso": {"lat": -33.04, "lon": -71.62},
            "Concepcion": {"lat": -36.82, "lon": -73.05},
    }
    registros = []
    for ciudad, coords in ciudades.items():
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current_weather=true"
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json().get("current_weather", {})
            registros.append({
                "Ciudad": ciudad,
                "Fecha": datetime.now().isoformat(),
                "Temperatura": data.get("temperature"),
                "Viento": data.get("windspeed"),
                "CodigoClima": data.get("weathercode")
            })
    df = pd.DataFrame(registros)
    nombre_archivo = f"/opt/airflow/data/clima/clima_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    os.makedirs("/opt/airflow/data", exist_ok = True)
    df.to_csv(nombre_archivo, index = False)
    print(f"Archivo guardado: {nombre_archivo}")

tarea_ingesta = PythonOperator (
    task_id = "obtener_clima",
    python_callable=obtener_datos_clima,
    dag=dag
)

tarea_ingesta