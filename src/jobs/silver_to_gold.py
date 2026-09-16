# ==========================================
# JOB: Promocion a Capa Gold (Modelado Analitico)
# PATTERN: DIMENSIONAL AGGREGATION
# DESCRIPTION: Ejecuta operaciones de agregacion multidimensional usando el Catalyst Optimizer.
# Consolida las metricas (temperatura media, volumen, alertas) por franja horaria 
# orientadas a la capa de explotacion de negocio.
# ==========================================
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max as spark_max, min as spark_min, count, sum as spark_sum, when

def ensure_bucket_exists(bucket_name="datalake"):
    s3 = boto3.client('s3',
                      endpoint_url='http://minio:9000',
                      aws_access_key_id='admin',
                      aws_secret_access_key='adminadmin',
                      config=Config(signature_version='s3v4'))
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(Bucket=bucket_name)

def transformar_a_oro():
    ensure_bucket_exists()
    
    spark = SparkSession.builder \
        .appName("SilverToGoldIoT") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    print("Voy a leer los datos de la capa Plata...")
    silver_path = "s3a://datalake/silver/iot/sensor_aula_01/"
    df = spark.read.parquet(silver_path)
    
    # Agrupo los datos por año, mes, día y hora, y calculo medias, máximos, mínimos y volumen de alertas
    df_gold = df.groupBy("year", "month", "day", "hour").agg(
        avg("temperature").alias("avg_temperature"),
        spark_max("temperature").alias("max_temperature"),
        spark_min("temperature").alias("min_temperature"),
        avg("humidity").alias("avg_humidity"),
        avg("co2").alias("avg_co2"),
        spark_min("battery").alias("min_battery"),
        count("*").alias("num_events"),
        spark_sum(when(col("temperature") > 28.0, 1).otherwise(0)).alias("alert_high_temp"),
        spark_sum(when(col("co2") > 600, 1).otherwise(0)).alias("alert_high_co2")
    )
    
    print("Voy a guardar los resultados agregados en la capa Oro en MinIO...")
    gold_path = "s3a://datalake/gold/iot/sensor_aula_01/hourly_metrics/"
    
    df_gold.write \
        .mode("overwrite") \
        .parquet(gold_path)
        
    print("He finalizado la creacion de la capa Oro correctamente.")
    spark.stop()

if __name__ == "__main__":
    transformar_a_oro()
