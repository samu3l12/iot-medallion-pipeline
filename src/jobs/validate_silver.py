from pyspark.sql import SparkSession

def validar_plata():
    spark = SparkSession.builder \
        .appName("ValidarSilverIoT") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "adminadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
        
    silver_path = "s3a://datalake/silver/iot/sensor_aula_01/"
    
    print(f"Voy a leer la ruta {silver_path} para comprobar que todo esta bien.")
    try:
        df = spark.read.parquet(silver_path)
        print(f"He encontrado {df.count()} registros validos en la capa Plata.")
        df.printSchema()
    except Exception as e:
        print("He tenido un problema al leer la capa Plata.")
        
    spark.stop()

if __name__ == "__main__":
    validar_plata()
