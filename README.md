# Pipeline de datos IoT con arquitectura Medallón

Pipeline completo que recoge telemetría de un sensor de aula (temperatura, humedad, CO2 y batería), la valida, la transforma y la deja lista para explotar en un cuadro de mandos. Todo orquestado con Apache Airflow y desplegado con Docker Compose.

```
Generación → HDFS (Bronce) → Spark → MinIO/Parquet (Plata) → Spark → MinIO (Oro) → PostgreSQL → Superset
                                 ↓
                           Cuarentena (HDFS)
```

## Lo que resuelve

Un sensor real falla: se queda sin batería, pierde la conexión o manda lecturas imposibles. El pipeline separa lo válido de lo defectuoso antes de que llegue a los cuadros de mando. En la última ejecución, de **1.000 registros procesados, 929 pasaron el control de calidad y 71 fueron a cuarentena**.

## Calidad de datos definida por metadatos

Las reglas no están escritas dentro del código de Spark, sino en `src/jobs/quality_rules.json`:

```json
{
  "sensor_aula_01": [
    "temperature BETWEEN -20 AND 80",
    "humidity BETWEEN 0 AND 100",
    "co2 > 0",
    "battery BETWEEN 0 AND 100"
  ]
}
```

Los jobs de PySpark leen ese JSON y construyen las condiciones con la función `expr()`. Así se pueden añadir sensores o cambiar umbrales sin tocar el código, que es como se hace en un equipo de datos real.

## Estructura

```
src/airflow/dags/iot_medallion_pipeline.py   DAG con toda la orquestación
src/jobs/                                     Jobs de Python y PySpark por capa
  generate_iot_data.py → load_bronze_hdfs.py → validate_quality.py
  → bronze_to_silver.py → silver_to_gold.py → publish_gold_superset.py
docs/memoria-tecnica.md                       Memoria del proyecto con resultados y capturas
extras/airflow-lab/                           DAGs adicionales: papermill y Jupyter como cliente REST de Airflow
extras/pipeline-kaggle-hdfs-minio.ipynb       Ingesta Kaggle → HDFS → MinIO con PyArrow
extras/spark-taxis-nyc-medallon.ipynb         Spark con funciones de ventana sobre los taxis de Nueva York
extras/spark-ecommerce-agregaciones.ipynb     Agregaciones sobre millones de eventos de e-commerce
```

## Cómo ejecutarlo

```bash
docker compose --profile core --profile orchestration up -d
```

Airflow queda en `localhost:8081`, HDFS en `9870`, MinIO en `9001` y Superset en `8089`. Se activa el DAG `iot_medallion_pipeline` y se lanza. Los pasos detallados están en `docs/guia-de-ejecucion-original.md`.

> Las credenciales del entorno son las de un laboratorio local y están pensadas solo para levantarlo en tu máquina.

## Tecnologías

Apache Airflow · Apache Spark (PySpark) · Hadoop HDFS · MinIO (S3) · Parquet · PostgreSQL · Apache Superset · Docker Compose
