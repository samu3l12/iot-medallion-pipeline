# Memoria del Proyecto: Pipeline de Datos IoT (Arquitectura Medallón)
**Autor:** Samuel Escribano García
## 1. Descripción del caso de uso IoT elegido
Para este proyecto he decidido simular un entorno de "Aula Inteligente". La idea principal es recoger telemetría ambiental de un sensor ubicado en una clase física (que he identificado como`sensor_aula_01`) para monitorizar de forma continua las condiciones en las que están estudiando los alumnos. Por ejemplo, si los niveles de CO2 o la temperatura suben demasiado, sabemos que afecta directamente a la concentración. Por eso me pareció un caso de uso muy práctico y realista para montar un pipeline completo de Big Data.

## 2. Esquema de los datos generados
El script en Python que he preparado simula los eventos del sensor y los vuelca en formato JSON. Cada mensaje de evento que el sensor transmite tiene esta estructura de datos:

-`event_id`: Identificador único del evento (generado como UUID).
-`timestamp`: Fecha y hora exacta de la lectura en formato ISO 8601.
-`sensor_id`: El identificador físico del sensor (en todas mis pruebas será "sensor_aula_01").
-`temperature`: Los grados centígrados detectados en el aula (float).
-`humidity`: El porcentaje de humedad relativa en el ambiente (float).
-`co2`: Las partículas por millón de CO2 (int).
-`battery`: El porcentaje de batería que le queda al sensor (int).

## 3. Explicación de la arquitectura del pipeline
He diseñado el pipeline siguiendo fielmente la **Arquitectura Medallón** (Bronce, Plata, Oro), orquestando paso a paso todo el flujo gracias a Apache Airflow. El proceso funciona exactamente así:

1. **Capa Bronce (Ingesta):** Un job inicial de Python genera los datos y los vuelca tal cual salen del sensor (JSON crudo) directamente al sistema de archivos distribuido Hadoop (HDFS).
2. **Capa Plata (Limpieza):** Aquí entra la potencia de Apache Spark. Un job lee los datos de HDFS y les pasa un filtro de calidad de negocio estricto. Los registros que vienen mal los envía directamente a una zona de "cuarentena" en HDFS para que sean analizados más tarde. Los registros buenos, en cambio, los convierto a formato columnar optimizado (Parquet) y los guardo en MinIO (S3).
3. **Capa Oro (Negocio):** Otro script de Spark coge los datos limpios de la capa Plata y los agrupa, calculando las medias de temperatura por hora. El resultado agregado se guarda de nuevo en MinIO en una carpeta Gold.
4. **Capa de BI (Visualización):** Finalmente, para poder hacer gráficos cómodamente, extraigo la información de la capa Oro y la escribo en una tabla de PostgreSQL. Hecho esto, conecto Apache Superset a Postgres y preparo el cuadro de mandos final.

## 4. Captura de los datos Bronze en HDFS
A continuación se observan los datos crudos JSON aterrizando en el DataLake (capa Bronze) a través del navegador de archivos del NameNode.

![Captura HDFS Bronze](captura_hdfs_bronze.png)

## 5. Captura de los datos Plata y Oro en MinIO
Aquí se demuestran las escrituras distribuidas en formato columnar Parquet, realizadas por Spark, estructuradas en las carpetas correspondientes a la capa Plata (Silver) y Oro (Gold) en el bucket de S3/MinIO.

![Captura MinIO Silver](captura_minio_silver.png)

![Captura MinIO Gold](captura_minio_gold.png)

## 6. Explicación de las reglas de calidad aplicadas
Para garantizar que a nuestro lago de datos no entra información corrupta de los sensores (por fallos eléctricos o desconexiones), he implementado un sistema avanzado de **Calidad de Datos guiada por Metadatos (Metadata-Driven)**.

En lugar de poner "hardcodeadas" las reglas dentro del código Python de Spark (lo que dificultaría el mantenimiento), he extraído toda la lógica de negocio a un archivo externo llamado`quality_rules.json`:

```json
{
  "sensor_aula_01": [
    "event_id IS NOT NULL",
    "timestamp != ''",
    "temperature BETWEEN -20 AND 80",
    "humidity BETWEEN 0 AND 100",
    "co2 > 0",
    "battery BETWEEN 0 AND 100"
  ]
}
```

Tanto el script de validación (`validate_quality.py`) como el de paso a Plata (`bronze_to_silver.py`) leen dinámicamente este JSON y construyen las sentencias SQL utilizando la función`expr()` de PySpark. Cualquier lectura del sensor que no cumpla todas estas condiciones dinámicas, se etiqueta como falsa (`is_valid = False`) y es expulsada a la carpeta de cuarentena en HDFS. Esto permite que analistas de negocio puedan cambiar reglas o añadir nuevos sensores editando únicamente un JSON, sin necesidad de tocar código.

## 7. Informe de calidad generado por el pipeline
En mi última ejecución, en la cual generé 1000 registros para probar, el sistema fue capaz de separar el ruido correctamente, generando este reporte:

```json
{
  "total_records": 1000,
  "valid_records": 929,
  "invalid_records": 71
}
```
Esto certifica que los scripts Spark se ejecutan de manera exitosa y que el pipeline es capaz de proteger las capas superiores de datos basura.

## 8. Captura del DAG de Airflow
Aquí se comprueba que toda la orquestación funciona sin errores (todas las tareas finalizadas en color verde):

![Captura Airflow Graph](captura_airflow_graph.png)

## 9. Captura del dashboard en Superset
Para finalizar el ciclo y cumplir con los requisitos analíticos, he construido un Dashboard completo que contiene **tres visualizaciones**:
1. La evolución de la temperatura media del aula agrupada por hora.
2. El volumen total de eventos procesados por hora.
3. El conteo de alertas de alta temperatura, demostrando el control de calidad.

![Captura Superset](captura_superset_recortada.png)

## 10. Problemas encontrados y soluciones aplicadas
Durante la creación de este pipeline me he enfrentado a dos problemas técnicos que bloqueaban todo y que conseguí diagnosticar y resolver:

- **Error de conectividad y escritura 403 (S3 MinIO):**
  Al principio Spark explotaba y era incapaz de volcar los archivos Parquet en la capa de Plata. Tirando del hilo de los logs, averigüé que era un problema de desalineación de credenciales. Los scripts los tenía configurados con las claves por defecto`minioadmin`, pero investigando el entorno vi que el contenedor Docker pedía el usuario`admin` con contraseña`adminadmin`. Tras actualizar las propiedades de S3A (`fs.s3a.access.key` y`secret.key`) en los 3 scripts de Spark, logré escribir el Parquet correctamente.

- **Caída de Spark por falta de recursos (Exit Code -9 / OOM Killer):**
  Al ejecutar el paso de`transformar_bronze_a_plata`, la tarea fallaba intermitentemente de forma misteriosa devolviendo siempre un`exit code -9`. Me percaté de que el sistema operativo estaba forzando el cierre del proceso Spark por sobreconsumo de memoria RAM al aplicar el códec de compresión Snappy. La solución drástica pero efectiva fue acceder a la configuración de la infraestructura en el`docker-compose.yml`, donde amplié el límite de memoria asignado al servicio`airflow-scheduler` de 512M a 1G, reconstruyendo el contenedor. Problema solucionado de raíz.

## 11. Sistema de Alertas y Operatividad (Extra)
Para asegurar que el pipeline está preparado para un entorno de Producción real, he configurado un **Sistema Proactivo de Alertas en Airflow**. He programado un callback (on_failure_callback) enganchado directamente a los argumentos por defecto del DAG.

De este modo, si cualquier nodo de la arquitectura falla (por ejemplo, si Spark se queda sin memoria o si MinIO rechaza una conexión), Airflow lo detecta instantáneamente, captura el log del error y emite una alerta crítica de fallo dirigida al equipo de Data Engineering. Esto garantiza que ningún fallo pase desapercibido, elevando la fiabilidad del sistema a un nivel completamente profesional.
