from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def tarea_0_func(**kwargs):
    conf = kwargs.get('dag_run').conf if kwargs.get('dag_run') else {}
    descripcion = conf.get('descripcion', 'Sin descripcion')
    commit = conf.get('commit', '0000')
    print(f"Hola! Ejecutando workflow: {descripcion}")
    print(f"Commit recibido: {commit}")
    if commit == "1":
        raise Exception(f"Hoy no desplegamos porque no me gusta el commit {commit}")
    return {"ok": True, "descripcion": descripcion, "commit": commit}

def tarea_2_func(**kwargs):
    ti = kwargs['ti']
    valor_tarea_0 = ti.xcom_pull(task_ids='tarea_0')
    print("Hola")
    print(f"Valor recibido de tarea_0 via XCom: {valor_tarea_0}")
    return {"ok": 2, "datos_recibidos": valor_tarea_0}

with DAG(
    'workflow_video',
    description='Recreacion del workflow del video tutorial de Apache Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['video', 'tutorial', 'workflow'],
    params={
        "descripcion": "Workflow de ejemplo del video tutorial",
        "commit": "0000",
    },
) as dag:
    tarea_0 = PythonOperator(
        task_id='tarea_0',
        python_callable=tarea_0_func,
    )

    print_date = BashOperator(
        task_id='print_date',
        bash_command='echo "La fecha es: $(date)"',
    )

    tarea_2 = PythonOperator(
        task_id='tarea_2',
        python_callable=tarea_2_func,
    )

    tarea_0 >> [print_date, tarea_2]
