# ==========================================
# JOB: Data Generator (IoT Simulation)
# PATTERN: MOCK DATA INGESTION
# DESCRIPTION: Simula el emisor de eventos IoT generando payloads JSON estructurados.
# Inyecta anomalias estocasticas (outliers) para validar el Data Quality Framework.
# ==========================================
import json
import random
import os
from datetime import datetime, timedelta

def generar_datos_iot():
    # He decidido crear 1000 registros para simular un lote de datos
    num_records = 1000
    file_path = "/tmp/raw_iot_data.jsonl"
    
    # Me aseguro de que el directorio temporal exista
    os.makedirs("/tmp", exist_ok=True)
    
    # Uso esta variable de tiempo base
    base_time = datetime.strptime("2026-04-27T00:00:00", "%Y-%m-%dT%H:%M:%S")
    
    with open(file_path, "w", encoding="utf-8") as f:
        for i in range(num_records):
            # Genero datos normales por defecto
            event_id = f"evt_{str(i).zfill(6)}"
            device_id = "sensor_aula_01"
            timestamp = (base_time + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S")
            temperature = round(random.uniform(18.0, 30.0), 1)
            humidity = round(random.uniform(30.0, 70.0), 1)
            co2 = random.randint(400, 800)
            battery = max(0, 100 - (i // 10))
            status = "OK"

            # Introduzco errores controlados aleatoriamente para cumplir con la practica
            error_type = random.random()
            if error_type < 0.02:
                # 2 por ciento de probabilidad: Temperatura fuera de rango
                temperature = 150.0
                status = "ERROR"
            elif error_type < 0.04:
                # 2 por ciento de probabilidad: Humedad imposible
                humidity = -10.0
                status = "ERROR"
            elif error_type < 0.06:
                # 2 por ciento de probabilidad: Fecha vacia
                timestamp = ""
            elif error_type < 0.08:
                # 2 por ciento de probabilidad: Evento duplicado
                if i > 0:
                    event_id = f"evt_{str(i-1).zfill(6)}"
            elif error_type < 0.10:
                # 2 por ciento de probabilidad: Bateria invalida
                battery = 150
            
            record = {
                "event_id": event_id,
                "device_id": device_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "co2": co2,
                "battery": battery,
                "status": status
            }
            
            f.write(json.dumps(record) + "\n")
            
    print(f"He generado correctamente {num_records} registros en {file_path}")

if __name__ == "__main__":
    generar_datos_iot()
