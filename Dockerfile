# Cambiamos de 2.7.1 (Python 3.8) a 2.10.2-python3.11
FROM apache/airflow:2.10.2-python3.11
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt