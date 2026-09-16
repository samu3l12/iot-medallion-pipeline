# ============================================================
# DAG de ejemplo - Para pruebas desde Jupyter (Ejercicio 2)
# ============================================================
# Este DAG sencillo lo he creado para demostrar cómo lanzar,
# consultar y monitorizar DAGs desde Jupyter mediante la
# API REST y la CLI de Airflow.
# ============================================================

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


def tarea_saludo(**kwargs):
    """Tarea simple que imprime un saludo y devuelve un valor."""
    mensaje = "¡Hola desde el DAG de ejemplo!"
    print(mensaje)
    return {"status": "ok", "mensaje": mensaje}


def tarea_procesamiento(**kwargs):
    """Tarea que simula un procesamiento de datos."""
    import time
    print("Iniciando procesamiento de datos...")
    time.sleep(2)  # Simula un procesamiento
    print("Procesamiento completado correctamente.")
    return {"procesado": True, "registros": 42}


# Definición del DAG
with DAG(
    dag_id='dag_ejemplo',
    default_args=default_args,
    description='DAG de ejemplo para pruebas desde Jupyter',
    schedule_interval=None,  # Solo ejecución manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ejemplo', 'jupyter'],
) as dag:

    # Tarea 1: Saludo con PythonOperator
    t1 = PythonOperator(
        task_id='tarea_saludo',
        python_callable=tarea_saludo,
    )

    # Tarea 2: Imprimir fecha con BashOperator
    t2 = BashOperator(
        task_id='imprimir_fecha',
        bash_command='echo "La fecha actual es: $(date)"',
    )

    # Tarea 3: Procesamiento con PythonOperator
    t3 = PythonOperator(
        task_id='tarea_procesamiento',
        python_callable=tarea_procesamiento,
    )

    # Dependencias: t1 >> [t2, t3]
    t1 >> [t2, t3]
