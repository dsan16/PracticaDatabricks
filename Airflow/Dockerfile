FROM apache/airflow:2.6.3-python3.9
USER airflow
RUN pip install --no-cache-dir \
    "pyspark==3.4.1" \
    "kaggle==1.5.12" \
    "dbt-core==1.6.0" \
    "dbt-snowflake==1.6.0"
USER root
