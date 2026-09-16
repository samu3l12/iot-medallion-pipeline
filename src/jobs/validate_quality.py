# ==========================================
# JOB: Metadata-Driven Data Quality Gate
# PATTERN: DYNAMIC FILTERING & QUARANTINE
# DESCRIPTION: Actua como un Quality Gate entre Bronze y Silver. Parsea reglas externas (JSON)
# y compila dinamicamente un Abstract Syntax Tree (AST) a traves de expr().
# Segrega las tuplas corruptas a una zona de Cuarentena en HDFS.
# ==========================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, expr
import json
import os

def load_rules(sensor_id="sensor_aula_01"):
    # Cargar las reglas desde el archivo JSON
    rules_path = os.path.join(os.path.dirname(__file__), 'quality_rules.json')
    with open(rules_path, 'r') as f:
        rules_doc = json.load(f)
    
    # Unimos todas las reglas con AND para formar la condicion de SQL completa
    rules = rules_doc.get(sensor_id, [])
    if not rules:
        return "1=1" # Si no hay reglas, todo es valido
    return " AND ".join(f"({rule})" for rule in rules)

from datetime import datetime

def ejecutar_calidad():
    # Obtengo la fecha de hoy para leer y escribir en la particion correcta
    hoy = datetime.now()
    year = hoy.strftime("%Y")
    month = hoy.strftime("%m")
    day = hoy.strftime("%d")

    # Inicio la sesion de Spark para la validacion
    spark = SparkSession.builder \
        .appName("ValidacionCalidadIoT") \
        .getOrCreate()
        
    print("He iniciado Spark y voy a leer la capa Bronze...")
    
    bronze_path = f"hdfs://namenode:9000/datalake/bronze/iot/sensor_aula_01/year={year}/month={month}/day={day}/raw_iot_data.jsonl"
    
    try:
        df = spark.read.json(bronze_path)
    except Exception as e:
        print("He tenido un problema leyendo los datos Bronze.")
        spark.stop()
        return

    total_records = df.count()
    
    # Cargo la regla dinamicamente desde el JSON
    validation_condition = load_rules("sensor_aula_01")
    print(f"Aplicando reglas dinámicas: {validation_condition}")
    
    # Aplico la expresion de SQL que he montado
    df_with_flags = df.withColumn("is_valid", expr(validation_condition))
    
    valid_count = df_with_flags.filter(col("is_valid") == True).count()
    invalid_count = total_records - valid_count
    
    # Extraigo los registros invalidos para la zona de cuarentena
    df_invalid = df_with_flags.filter(col("is_valid") == False).drop("is_valid")
    quarantine_path = f"hdfs://namenode:9000/datalake/quarantine/iot/sensor_aula_01/year={year}/month={month}/day={day}/"
    
    print(f"Voy a guardar {invalid_count} registros en cuarentena...")
    df_invalid.write.mode("overwrite").json(quarantine_path)
    
    # Genero un informe resumen en JSON
    reporte = {
        "total_records": total_records,
        "valid_records": valid_count,
        "invalid_records": invalid_count
    }
    
    print("Este es el informe de calidad que he obtenido:")
    print(json.dumps(reporte, indent=2))
    
    # Lo guardo tambien en local para tener evidencia
    with open("/tmp/quality_report.json", "w") as f:
        json.dump(reporte, f, indent=2)
        
    spark.stop()

if __name__ == "__main__":
    ejecutar_calidad()
