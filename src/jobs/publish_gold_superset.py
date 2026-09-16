# ==========================================
# JOB: Database Publishing (Capa de Servicio)
# PATTERN: DATA EXPORT TO RDBMS
# DESCRIPTION: Extrae el dataset agregado (Gold) de MinIO y lo materializa en PostgreSQL 
# utilizando SQLAlchemy. Habilita lectura sin latencia para los tableros analiticos en Apache Superset.
# ==========================================
import pandas as pd
from sqlalchemy import create_engine

def publicar_dashboard():
    # He creado este script para automatizar la publicacion de la capa Oro en Superset.
    # Dado que Superset requiere una base de datos relacional para funcionar de forma nativa en este entorno,
    # leo los datos en formato Parquet desde MinIO y los cargo directamente en PostgreSQL.
    
    print("Voy a conectarme a MinIO para leer las metricas por hora de la capa Oro...")
    gold_path = "s3://datalake/gold/iot/sensor_aula_01/hourly_metrics/"
    
    try:
        # Utilizo s3fs por debajo para leer directamente el parquet de MinIO
        df = pd.read_parquet(gold_path, storage_options={
            "key": "admin",
            "secret": "adminadmin",
            "client_kwargs": {"endpoint_url": "http://minio:9000"}
        })
        
        print(f"He leido {len(df)} registros. Voy a insertarlos en PostgreSQL para Superset...")
        
        # Me conecto a la base de datos postgres del docker-compose (usuario airflow, base de datos airflow)
        # que esta disponible en el mismo cluster
        engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
        
        # Guardo la tabla reemplazando los datos si ya existia
        df.to_sql('gold_iot_metrics', engine, if_exists='replace', index=False)
        
        print("He publicado los datos correctamente.")
        print("El siguiente paso es entrar en Superset, añadir la base de datos 'airflow' y crear los graficos sobre la tabla 'gold_iot_metrics'.")
        
    except Exception as e:
        print("He tenido un problema al publicar en la base de datos:")
        print(str(e))

if __name__ == "__main__":
    publicar_dashboard()
