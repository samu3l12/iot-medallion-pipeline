# ==========================================
# JOB: Ingesta Bronze y Particionado HDFS
# PATTERN: BATCH INGESTION & HIVE-STYLE PARTITIONING
# DESCRIPTION: Ingesta los datos crudos inmutables en la capa Bronze (HDFS) via WebHDFS.
# Implementa Hive-style partitioning dinamico calculando el timestamp en runtime 
# para optimizar el Partition Discovery en lecturas futuras.
# ==========================================
import os
from hdfs import InsecureClient
from datetime import datetime

def cargar_en_hdfs():
    # Obtengo la fecha actual para el particionado dinamico
    hoy = datetime.now()
    year = hoy.strftime("%Y")
    month = hoy.strftime("%m")
    day = hoy.strftime("%d")

    # Defino las rutas locales y la conexion al NameNode de Hadoop
    local_file = "/tmp/raw_iot_data.jsonl"
    hdfs_base_dir = f"/datalake/bronze/iot/sensor_aula_01/year={year}/month={month}/day={day}"
    hdfs_dest_file = f"{hdfs_base_dir}/raw_iot_data.jsonl"
    
    # He optado por usar la libreria nativa 'hdfs' para conectarme directamente por API web (WebHDFS)
    # en lugar de depender del comando de consola, asi me aseguro de que funcione desde cualquier contenedor.
    # El NameNode esta en la red de Docker en el puerto 9870
    try:
        client = InsecureClient('http://namenode:9870', user='root')
        
        print("Voy a crear el directorio en HDFS si no existe...")
        client.makedirs(hdfs_base_dir)
        
        print("Ahora voy a subir el fichero original a la capa Bronze...")
        # Subo el archivo local a la ruta HDFS, sobrescribiendo si ya existe de pruebas anteriores
        client.upload(hdfs_dest_file, local_file, overwrite=True)
        
        print("He subido los datos a HDFS con exito.")
    except Exception as e:
        print("He encontrado un problema al subir a HDFS mediante la API web:")
        print(str(e))

if __name__ == "__main__":
    cargar_en_hdfs()
