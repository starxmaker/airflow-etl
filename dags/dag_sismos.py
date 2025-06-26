from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import re

pattern = r"(\d+)\s?km\s+al\s+([A-Z]{1,2})\s+de\s+(.+)"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="sismos_Chile",
    default_args=default_args,
    description="Proceso de ingesta para captura de sismos",
    schedule="0 */1 * * *",
    catchup=False
)

def obtener_datos_sismos():
    registros = []
    url = f"https://api.gael.cloud/general/public/sismos"
    response = requests.get(url)
    if response.status_code == 200:
        records = response.json()
        if isinstance(records, list):
            for item in records:
                match = re.search(pattern, item["RefGeografica"])
                if match:
                    distancia = match.group(1)
                    direccion = match.group(2)
                    lugar = match.group(3)
                    registros.append({
                        "Fecha": item["Fecha"],
                        "Profundidad": item["Profundidad"],
                        "Magnitud": item["Magnitud"],
                        "Lugar": lugar,
                        "DistanciaKm": distancia,
                        "Dirección": direccion,
                        "FechaUpdate": item["FechaUpdate"]
                    })
                else:
                    print("No match geografico.")
    df = pd.DataFrame(registros)
    nombre_archivo = f"/opt/airflow/data/sismos/sismos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    os.makedirs("/opt/airflow/data", exist_ok = True)
    df.to_csv(nombre_archivo, index = False)
    print(f"Archivo guardado: {nombre_archivo}")


tarea_ingesta_sismos = PythonOperator (
    task_id = "obtener_sismos",
    python_callable=obtener_datos_sismos,
    dag=dag
)

tarea_ingesta_sismos