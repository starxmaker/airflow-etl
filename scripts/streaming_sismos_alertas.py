import time
import os
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from twilio.rest import Client
from dotenv import load_dotenv
import os

load_dotenv()
account_sid = os.getenv("TWILIO_ACCOUNT_ID")
auth_token = os.getenv("TWILIO_AUTH_TOKEN")
phone_from = os.getenv("TWILIO_FROM")
phone_to = os.getenv("TWILIO_TO")

client = Client(account_sid, auth_token)

def send_message(phone_num, message):
    client.messages.create(
        from_=phone_from,
        body=message,
        to=phone_num
    )

CARPETAS = {"./data/sismos", "./data/sismos-test"}

class CSVHandler(FileSystemEventHandler): 
    _ultima_fecha = None
    def on_created(self, event):
        if event.src_path.endswith("csv"):
            print(f"Nuevo archivo detectado: {event.src_path}")
            self.procesar_archivo(event.src_path)

    def procesar_archivo (self, archivo_path): 
        try:
            df=pd.read_csv(archivo_path)
        except Exception as e:
            print(f"Error leyendo {archivo_path}: {e}. Saltando archivo.")
            return
        df["Fecha"] = pd.to_datetime(df["Fecha"])
        sorted_df=df.sort_values(by="Fecha")
        if self._ultima_fecha is not None:
            filtered_df = sorted_df[sorted_df['Fecha'] > self._ultima_fecha]
        else:
            filtered_df = sorted_df
        if len(filtered_df) == 0:
            print(f"{archivo_path}: Sin registros nuevos.")
            return
        self._ultima_fecha = df['Fecha'].max()
        for _, fila in filtered_df.iterrows():
            magnitud = float(fila["Magnitud"])
            if magnitud >= 4:
                lugar=fila["Lugar"]
                message=f"Alerta! Temblor magnitud {magnitud} en {lugar}"
                print(message)
                send_message(phone_to, message)

observer = Observer()
handler = CSVHandler()
for CARPETA in CARPETAS:
    observer.schedule(handler, CARPETA, recursive=False)
    print(f"Escuchando carpeta {CARPETA}")
    
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()

observer.join()