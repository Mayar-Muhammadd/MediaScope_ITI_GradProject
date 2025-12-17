from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {"owner": "airflow"}


with DAG(
    dag_id="spark_hdfs_cleaning",
    start_date=datetime(2025, 12, 6),
    schedule_interval='0 * * * *',  
    default_args=default_args,
    catchup=False,
) as dag:

    
    spark_task = SparkSubmitOperator(
        task_id="run_spark_job",
        application="/opt/spark/apps/read_from_hdfs.py",
        conn_id="spark_default", 
        verbose=True,
        jars="/opt/spark/apps/spark-snowflake_2.12-2.16.0-spark_3.4.jar,/opt/spark/apps/snowflake-jdbc-3.16.1.jar",
        spark_binary="/home/airflow/.local/lib/python3.11/site-packages/pyspark/bin/spark-submit",
    )


    spark_task 