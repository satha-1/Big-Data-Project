FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends build-essential \
  && rm -rf /var/lib/apt/lists/*

USER airflow
COPY src/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

