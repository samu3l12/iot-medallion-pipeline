# ==========================================
# DAG: IoT Medallion Architecture Pipeline
# PATTERN: ORCHESTRATION & DEPENDENCY MANAGEMENT
# DESCRIPTION: Define el grafo dirigido aciclico (DAG) que orquesta todo el ciclo de vida del dato.
# Implementa dependencias estrictas (>>) y un sistema de alerting proactivo via on_failure_callback
# para notificar fallos criticos en la pipeline al equipo de Data Engineering.
# ==========================================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

import logging

def task_failure_alert(context):
    task_instance = context.get('task_instance')
    task_id = task_instance.task_id
    execution_date = context.get('execution_date')
    log_url = task_instance.log_url
    
    # Simulo el envio de una alerta a Slack o Email en el entorno de pruebas
    alert_message = f"🚨 [ALERTA CRÍTICA] Fallo en la tarea '{task_id}' durante la ejecución del {execution_date}.\nRevisar logs en: {log_url}\nEnviando notificación al equipo de Data Engineering..."
    logging.error(alert_message)
    print(alert_message)

# He definido los argumentos por defecto para mi DAG
default_args = {
    'owner': 'chorg',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_alert,
}

# Creo el DAG que orquestara todo mi pipeline IoT
with DAG(
    'iot_medallion_pipeline',
    default_args=default_args,
    description='Pipeline completo de Big Data para datos IoT con arquitectura Medallon',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['iot', 'medallion', 'bigdata'],
) as dag:

    # 1. Genero los datos de simulacion
    generate_data = BashOperator(
        task_id='generar_datos',
        bash_command='python /opt/airflow/dags/jobs/generate_iot_data.py ',
    )

    # 2. Valido el formato inicial
    validate_raw = BashOperator(
        task_id='validar_raw',
        bash_command='python /opt/airflow/dags/jobs/validate_raw.py ',
    )

    # 3. Cargo en la capa Bronze
    load_bronze = BashOperator(
        task_id='cargar_bronze_hdfs',
        bash_command='python /opt/airflow/dags/jobs/load_bronze_hdfs.py ',
    )

    # 4. Validaciones de calidad y cuarentena
    quality_check = BashOperator(
        task_id='ejecutar_validaciones_calidad',
        bash_command='spark-submit /opt/airflow/dags/jobs/validate_quality.py ',
    )

    # 5. Transformacion Bronze a Plata
    bronze_to_silver = BashOperator(
        task_id='transformar_bronze_a_plata',
        bash_command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/airflow/dags/jobs/bronze_to_silver.py ',
    )

    # 6. Validar capa Plata
    validate_silver = BashOperator(
        task_id='validar_plata',
        bash_command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/airflow/dags/jobs/validate_silver.py ',
    )

    # 7. Generar capa Oro
    silver_to_gold = BashOperator(
        task_id='generar_oro',
        bash_command='spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/airflow/dags/jobs/silver_to_gold.py ',
    )

    # 8. Preparar para Superset
    publish_superset = BashOperator(
        task_id='preparar_superset',
        bash_command='python /opt/airflow/dags/jobs/publish_gold_superset.py ',
    )

    # Defino el orden exacto de ejecucion de mis tareas
    generate_data >> validate_raw >> load_bronze >> quality_check >> bronze_to_silver >> validate_silver >> silver_to_gold >> publish_superset
