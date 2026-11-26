from datetime import datetime
from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

with DAG(
    dag_id="stocksense_docker_test",
    start_date=datetime(2026, 11, 26),
    schedule=None,
    catchup=False,
    tags=["stocksense", "test"]
) as dag:

        test_pyspark_container = DockerOperator(
        task_id="test_pyspark_container",
        image="stocksense-pyspark:1.0",
        command=[
            "python", "-c",
            "import socket; socket.create_connection(('minio', 9000), timeout=5).close(); print('MinIO connection: OK'); socket.create_connection(('postgres', 5432), timeout=5).close(); print('PostgreSQL connection: OK')"
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="big_data_ecosystems_svenlanao_default",
        api_version="auto",
        auto_remove="success",
        mount_tmp_dir=False
    )