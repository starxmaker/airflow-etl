import os
import pandas as pd
import sqlite3

source = './data'
target = os.path.join(source, "procesados")
os.makedirs(target, exist_ok=True)

files = sorted([
    f for f in os.listdir(source)
    if f.endswith(".csv") and os.path.isfile(os.path.join(source, f))
])

conn = sqlite3.connect(os.path.join(source, "clima_historico.db"))

for filename in files:
    path_file = os.path.join(source, filename)
    df = pd.read_csv(path_file)
    filename_clean = filename.replace("clima", "").replace(".csv","")
    df["FechaArchivo"]=filename_clean.split("_")[0]
    df.to_sql("termperaturas", conn, if_exists="append", index=False)
    new_path = os.path.join(target, filename)
    os.rename(path_file, new_path)
conn.commit()
conn.close()