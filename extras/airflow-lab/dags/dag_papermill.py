# ============================================================
# DAG con Papermill - Ejercicio 3
# ============================================================
# Este DAG ejecuta un notebook Jupyter parametrizable usando
# Papermill. Lo he preparado para demostrar la integración de 
# Airflow con Jupyter como contenido ejecutado (no como cliente).
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
    'retry_delay': timedelta(minutes=2),
}


def ejecutar_notebook(**kwargs):
    """
    Ejecuta un notebook Jupyter parametrizable usando Papermill.
    Los parámetros se pasan desde la configuración del DAG.
    El notebook de salida lo guardo en /opt/airflow/output/.
    """
    import papermill as pm
    from datetime import datetime

    # Parámetros del DAG
    params = kwargs.get('params', {})
    nombre = params.get('nombre', 'Estudiante')
    mensaje = params.get('mensaje', 'Ejecución desde Airflow con Papermill')

    # Generar nombre único para el output
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f'/opt/airflow/output/resultado_{timestamp}.ipynb'

    print(f"Ejecutando notebook con parámetros:")
    print(f"  nombre: {nombre}")
    print(f"  mensaje: {mensaje}")
    print(f"  output: {output_path}")

    # Ejecutar el notebook con Papermill
    pm.execute_notebook(
        input_path='/opt/airflow/notebooks/notebook_parametrizable.ipynb',
        output_path=output_path,
        parameters={
            'nombre': nombre,
            'mensaje': mensaje,
            'fecha_ejecucion': datetime.now().isoformat(),
        },
        kernel_name='python3',
    )

    print(f"Notebook ejecutado correctamente. Output en: {output_path}")
    return {"output_path": output_path, "status": "completado"}


def verificar_resultado(**kwargs):
    """
    Compruebo que el notebook se ha ejecutado correctamente
    leyendo la información del XCom de la tarea anterior.
    """
    ti = kwargs['ti']
    resultado = ti.xcom_pull(task_ids='ejecutar_notebook')

    print(f"Resultado de la ejecución: {resultado}")

    if resultado and resultado.get('status') == 'completado':
        print("✅ El notebook se ejecutó correctamente.")
        print(f"📄 Archivo de salida: {resultado.get('output_path')}")
    else:
        raise ValueError("❌ Error: El notebook no se ejecutó correctamente.")

    return resultado


# Definición del DAG
with DAG(
    dag_id='dag_papermill',
    default_args=default_args,
    description='DAG que ejecuta notebooks Jupyter con Papermill',
    schedule_interval=None,  # Solo ejecución manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['papermill', 'jupyter', 'notebook'],
    params={
        "nombre": "Estudiante",
        "mensaje": "Ejecución de ejemplo desde Airflow",
    },
) as dag:

    # Tarea 1: Preparar el entorno
    preparar = BashOperator(
        task_id='preparar_entorno',
        bash_command='mkdir -p /opt/airflow/output && echo "Directorio de output preparado"',
    )

    # Tarea 2: Ejecutar el notebook con Papermill
    ejecutar = PythonOperator(
        task_id='ejecutar_notebook',
        python_callable=ejecutar_notebook,
    )

    # Tarea 3: Verificar el resultado
    verificar = PythonOperator(
        task_id='verificar_resultado',
        python_callable=verificar_resultado,
    )

    # Dependencias: preparar >> ejecutar >> verificar
    preparar >> ejecutar >> verificar
