# ==========================================
# JOB: Promocion a Capa Silver
# PATTERN: REFINEMENT & COLUMNAR STORAGE
# DESCRIPTION: Refina la capa Bronze aplicando el filtro metadata-driven. 
# Garantiza idempotencia con dropDuplicates() y persiste los datos limpios en formato 
# columnar (Parquet) fuertemente tipado sobre Object Storage (MinIO) usando el conector S3A.
# ==========================================
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, expr
import json
import os

def load_rules(sensor_id="sensor_aula_01"):
    rules_path = os.path.join(os.path.dirname(__file__), 'quality_rules.json')
    with open(rules_path, 'r') as f:
        rules_doc = json.load(f)
    rules = rules_doc.get(sensor_id, [])
    if not rules:
        return "1=1"
    return " AND ".join(f"({rule})" for rule in rules)

def ensure_bucket_exists(bucket_name="datalake"):
    # Conecto a MinIO para crear el bucket si alguien lo ha borrado
    s3 = boto3.client('s3',
                      endpoint_url='http://minio:9000',
                      aws_access_key_id='admin',
                      aws_secret_access_key='adminadmin',
                      config=Config(signature_version='s3v4'))
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"El bucket '{bucket_name}' ya existe.")
    except:
        print(f"El bucket '{bucket_name}' no existe. Lo voy a crear ahora mismo...")
        s3.create_bucket(Bucket=bucket_name)

from datetime import datetime

def transformar_a_plata():
    ensure_bucket_exists()
    
    # He configurado Spark para poder escribir en MinIO usando s3a
    spark = SparkSession.builder \
        .appName("BronzeToSilverIoT") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    print("Voy a leer los datos crudos desde HDFS...")
    
    # Obtengo la fecha actual
    hoy = datetime.now()
    year = hoy.strftime("%Y")
    month = hoy.strftime("%m")
    day = hoy.strftime("%d")
    
    bronze_path = f"hdfs://namenode:9000/datalake/bronze/iot/sensor_aula_01/year={year}/month={month}/day={day}/raw_iot_data.jsonl"
    df = spark.read.json(bronze_path)
    
    # Aplico el mismo filtro que en la validacion pero de forma dinamica leyendo del JSON
    validation_condition = load_rules("sensor_aula_01")
    df_valid = df.filter(expr(validation_condition))
    
    # Elimino duplicados basandome en event_id
    df_clean = df_valid.dropDuplicates(["event_id"])
    
    # Anado columnas temporales que me serviran luego para particionar y analizar
    df_silver = df_clean.withColumn("timestamp_ts", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss")) \
                        .withColumn("year", year(col("timestamp_ts"))) \
                        .withColumn("month", month(col("timestamp_ts"))) \
                        .withColumn("day", dayofmonth(col("timestamp_ts"))) \
                        .withColumn("hour", hour(col("timestamp_ts")))
                        
    print("Voy a escribir la capa Plata en MinIO en formato Parquet...")
    silver_path = "s3a://datalake/silver/iot/sensor_aula_01/"
    
    # Escribo particionando por fecha
    df_silver.write \
        .partitionBy("year", "month", "day") \
        .mode("overwrite") \
        .parquet(silver_path)
        
    print("He completado la transformacion a la capa Plata.")
    spark.stop()

if __name__ == "__main__":
    transformar_a_plata()
