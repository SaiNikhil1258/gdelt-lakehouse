FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV DAGSTER_HOME=/opt/dagster/dagster_home

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/dagster/app
COPY gdelt_pipeline ./gdelt_pipeline
COPY workspace.yaml ./workspace.yaml

RUN mkdir -p $DAGSTER_HOME
COPY dagster.yaml $DAGSTER_HOME/dagster.yaml

EXPOSE 4000 3000

ENTRYPOINT ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "gdelt_pipeline"]