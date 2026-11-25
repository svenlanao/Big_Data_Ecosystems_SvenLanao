FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jdk-headless && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3

RUN pip install --no-cache-dir pyspark==4.2.1

ENV PYSPARK_SUBMIT_ARGS="--packages org.apache.hadoop:hadoop-aws:3.5.0 pyspark-shell"

RUN python -c 'from pyspark.sql import SparkSession; s=SparkSession.builder.appName("S3A_DependencySetup").getOrCreate(); print("S3A dependencies downloaded successfully"); s.stop()'

WORKDIR /workspace

CMD ["bash"]