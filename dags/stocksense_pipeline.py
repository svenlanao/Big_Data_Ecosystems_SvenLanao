from datetime import datetime
import os
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_DIR = "/mnt/c/Users/slanao/Desktop/Big_Data_Ecosystems_SvenLanao"

SPARK_ENV = {
    "MINIO_ROOT_USER": os.environ["MINIO_ROOT_USER"],
    "MINIO_ROOT_PASSWORD": os.environ["MINIO_ROOT_PASSWORD"],
    "POSTGRES_DB": os.environ["POSTGRES_DB"],
    "POSTGRES_USER": os.environ["POSTGRES_USER"],
    "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"]
}

def run_spark_script(task_id, script):
    return DockerOperator(
        task_id=task_id,
        image="stocksense-pyspark:1.0",
        command=["python", f"src/spark/{script}"],
        docker_url="unix://var/run/docker.sock",
        network_mode="big_data_ecosystems_svenlanao_default",
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False,
        working_dir="/workspace",
        mounts=[
            Mount(
                source=PROJECT_DIR,
                target="/workspace",
                type="bind"
            )
        ],
        environment=SPARK_ENV
    )

with DAG(
    dag_id="stocksense_pipeline",
    start_date=datetime(2026, 11, 26),
    schedule=None,
    catchup=False,
    tags=["stocksense", "data-pipeline"]
) as dag:

    clean_data = run_spark_script(
        "clean_data",
        "data_cleaning.py"
    )

    sales_analysis = run_spark_script(
        "sales_analysis",
        "sales_analysis.py"
    )

    inventory_analysis = run_spark_script(
        "inventory_analysis",
        "inventory_analysis.py"
    )

    load_postgres = run_spark_script(
        "load_postgres",
        "postgres_load.py"
    )

    clean_data >> sales_analysis >> inventory_analysis >> load_postgres