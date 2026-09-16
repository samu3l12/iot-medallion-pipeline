# Proyecto IoT Medallón

Este repositorio contiene todo el código fuente del proyecto final para procesar telemetría IoT mediante arquitectura Medallón (Bronce, Plata, Oro). Todo el flujo está orquestado con Apache Airflow, transformado con PySpark, y almacenado entre Hadoop HDFS y MinIO.

## Estructura del ZIP

-`src/airflow/dags/iot_medallion_pipeline.py`: Archivo principal del DAG de Airflow con toda la orquestación.
-`src/jobs/`: Carpeta con todos los scripts de procesamiento en Python y PySpark.
  -`generate_iot_data.py`: Simulación de datos IoT de temperatura/humedad.
  -`validate_raw.py`: Comprobación de formato básico inicial.
  -`load_bronze_hdfs.py`: Ingesta de datos crudos a HDFS (Capa Bronce).
  -`validate_quality.py`: Reglas de calidad del negocio (separación entre registros válidos y cuarentena).
  -`bronze_to_silver.py`: Limpieza y conversión a formato Parquet en S3/MinIO (Capa Plata).
  -`validate_silver.py`: Verificación de la capa Plata.
  -`silver_to_gold.py`: Agregación de datos horarios en S3/MinIO (Capa Oro).
  -`publish_gold_superset.py`: Traspaso de los datos agregados a PostgreSQL para visualización en Superset.

## Instrucciones de Ejecución

1. **Requisitos previos**: Es necesario tener instalado Docker y Docker Compose.
2. **Levantar la infraestructura**:
   En la raíz del proyecto (donde se ubica el`docker-compose.yml`), ejecuta:
```bash
   docker compose --profile core --profile orchestration up -d
   ```
   *Nota: El servicio`airflow-scheduler` debe tener asignado al menos 1GB de RAM en el compose para evitar que el OOM Killer detenga las transformaciones de Spark.*
3. **Acceder a Airflow**:
   Abre el navegador en`http://localhost:8081` y entra con el usuario`admin` y contraseña`admin`.
4. **Lanzar el Pipeline**:
   Busca el DAG llamado`iot_medallion_pipeline`. Desactiva la pausa (toggle) y dale al botón de "Play" (Trigger DAG).
5. **Verificar Resultados**:
   - Puedes entrar a HDFS (`http://localhost:9870`) para ver la capa Bronce y la Cuarentena.
   - Puedes entrar a MinIO (`http://localhost:9001`) con credenciales`admin / adminadmin` para ver los archivos Parquet en las capas Plata y Oro.
   - Entra a Superset (`http://localhost:8089`) para visualizar los dashboards finales.
