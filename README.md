# Evaluación final - Proyecto de Flujo Big Data Completo

El siguiente repositorio contiene un ejemplo de ingesta, procesamiento, almacenamiento y visualización de datos sísmicos en Chile.

## Requisitos

- Docker
- Al menos 4 GB de memoria disponible (8 GB recomendados)
- Python 3.10+

## Ejecución

### Pasos previos

Primero asegurarse de tener instalado Python 3

    python3 --version

Luego, crear un ambiente virtual

    python3 -m venv venv

Posteriormente se debe habilitar el ambiente local.

    source venv/bin/activate

A continuación, instalar dependencias

    pip install -r requirements.txt

### Ingesta

Primero, se debe clonar el repositorio actual.

Luego, crear las carpetas para airflow y los archivos generados 

    mkdir -p ./data ./data/sismos ./config ./logs ./plugins

Posteriormente, es necesario construir el archivo de variables de entorno:

    echo -e "AIRFLOW_UID=$(id -u)" > .env

Por último, levantar el stack con el siguiente comando:

    docker compose up -d

Una vez se levante la aplicación, dirigirse a `localhost:8080` e ingresar el usuario `airflow` y contraseña `airflow`. Dirigirse a la sección DAGs, y buscar la entrada `sismos_Chile`. Por último, habilitar el DAG (con el ícono del toggle) o ejecutar de forma manual.

Si la ejecución es correcta, se verán los resultados en la carpeta `./data/sismos`. En caso de fallas, revisar la carpeta `./logs`

### Procesamiento streaming

El presente proyecto cuenta con un demo de procesamiento streaming en los que revisa los sismos recibidos y alerta a través de SMS si la magnitud supera un umbral.

Abrir el archivo `.env` y agregar las siguientes variables:

 - `TWILIO_ACCOUNT_ID`: Identificador de cuenta de Twilio (se puede crear una cuenta gratuita)
 - `TWILIO_AUTH_TOKEN`: Token de autenticación con Twilio
 - `TWILIO_FROM`: Número de teléfono de origen (configurar en Twilio)
 - `TWILIO_TO`: Número de destino

Luego creamos una carpeta de prueba:

    mkdir -p ./data/sismos-test

Finalmente ejecutamos el script:

    python scripts/streaming_sismos_alertas.py

Para probar que funciona, copiar el archivo `sample.csv` de la raíz en `./data/sismos-test`. Deberá aparecer un mensaje en la consola y también recibir un SMS al número indicado.

### Almacenamiento y procesamiento batch

Abrir el archivo `.env` y agregar la siguiente variable:

    USER_EMAIL=email@personal.com

Este valor es necesario para identificar las llamadas a Nominatim API, lo cual está en sus términos de servicio.

Por defecto se utilizará la misma base de datos Postgres del stack de Airflow. Si se desea utilizar otra instancia, copiar los valores de `.env.template` en `.env` y dar los valores respetivos.

Antes de ejecutar el script, asegurarse de tener archivos en `data/sismos` de tipo csv.

Finalmente, ejecutar el siguiente comando

    python ./scripts/batch_sismos_postgres.py

Dependiendo de la cantidad de registros, puede tomar cierto tiempo.

### Visualización

Abrir navegador y dirigirse a `localhost:3000`. El usuario por defecto es `admin` y la contraseña es `admin`.

Abrir menú lateral y buscar `Data sources`. Hacer click en `Add datasource`, buscar postgres y hacer click en la opción.

Si se utiliza la misma instancia postgres de Airflow, rellenar con los siguientes valores:

- Host URL:  postgres (ojo, no es localhost, debido al networking interno de docker compose)
- Database name: airflow
- Username: airflow
- Password airflow
- TLS/SSL mode: disable

Hacer click en save & test y verificar conexión.

Una vez creada, hacer click en `Build a dashboard` y seleccionar visualización Geomap

En la fuente de datos colocar la siguiente consulta:

    select "Fecha", "Latitud", "Longitud", "Magnitud", "Profundidad", "Lugar", "DistanciaKm", "Dirección" from sismos 

En las opciones de visualización, configurar lo siguiente:

- Location Mode: Coords
- Latitude field: Latitud
- Longitude field: Longitud
- Color: Magnitud
- Threshold: Amarillo base, Naranjo desde 3 y Rojo desde 5

Si se realizaron los pasos de forma correcta, se verán puntos en el mapa indicando los epicentros de los sismos.