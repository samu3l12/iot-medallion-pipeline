FROM apache/airflow:2.10.4-python3.10

USER root

# Instalar dependencias del sistema necesarias
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/airflow/db \
    && chown -R airflow:root /opt/airflow/db

USER airflow

# Copiar e instalar dependencias de Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Instalar el kernel de Jupyter para el entorno de Airflow
RUN python -m ipykernel install --user --name airflow --display-name "Python (Airflow)"
