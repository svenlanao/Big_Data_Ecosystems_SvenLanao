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
    "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
}

NIFI_ENV = {
    "NIFI_USERNAME": os.environ["NIFI_USERNAME"],
    "NIFI_PASSWORD": os.environ["NIFI_PASSWORD"],
}


def run_docker_script(task_id, script, environment):
    return DockerOperator(
        task_id=task_id,
        image="stocksense-pyspark:1.0",
        command=["python", script],
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
                type="bind",
            )
        ],
        environment=environment,
    )


with DAG(
    dag_id="stocksense_pipeline",
    start_date=datetime(2026, 11, 26),
    schedule=None,
    catchup=False,
    tags=["stocksense", "data-pipeline"],
) as dag:

    nifi_ingestion = run_docker_script(
        "nifi_ingestion",
        "src/nifi/nifi_ingestion.py",
        NIFI_ENV,
    )

    prepare_ingestion = run_docker_script(
        "prepare_ingestion",
        "src/spark/prepare_ingestion.py",
        SPARK_ENV,
    )

    clean_data = run_docker_script(
        "clean_data",
        "src/spark/data_cleaning.py",
        SPARK_ENV,
    )

    sales_analysis = run_docker_script(
        "sales_analysis",
        "src/spark/sales_analysis.py",
        SPARK_ENV,
    )

    inventory_analysis = run_docker_script(
        "inventory_analysis",
        "src/spark/inventory_analysis.py",
        SPARK_ENV,
    )

    load_postgres = run_docker_script(
        "load_postgres",
        "src/spark/postgres_load.py",
        SPARK_ENV,
    )

    (
        nifi_ingestion
        >> prepare_ingestion
        >> clean_data
        >> sales_analysis
        >> inventory_analysis
        >> load_postgres
    )