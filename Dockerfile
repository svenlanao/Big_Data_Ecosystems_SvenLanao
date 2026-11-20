FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends openjdk-21-jdk-headless && apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3

RUN pip install --no-cache-dir pyspark==4.2.1

WORKDIR /workspace

CMD ["bash"]