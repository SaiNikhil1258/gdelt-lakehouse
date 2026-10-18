# dagster/Dockerfile
FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONPATH=/opt/dagster/app/jobs

COPY dagster/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN cd /usr/local/lib/python3.11/site-packages/pyspark/jars && curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.6.1/iceberg-spark-runtime-3.5_2.12-1.6.1.jar && curl -fLO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.6.1/iceberg-aws-bundle-1.6.1.jar && curl -fLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && curl -fLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

WORKDIR /opt/dagster/app
COPY spark/jobs ./jobs
COPY dagster/gdelt_pipeline ./gdelt_pipeline
COPY dagster/workspace.yaml ./workspace.yaml

RUN mkdir -p $DAGSTER_HOME
COPY dagster/dagster.yaml $DAGSTER_HOME/dagster.yaml

EXPOSE 4000 3000

# No ENTRYPOINT here on purpose.
#
# This used to be:
#   ENTRYPOINT ["dagster", "api", "grpc", "-h", "0.0.0.0", "-p", "4000", "-m", "gdelt_pipeline"]
#
# That's what broke `materialize all`. DockerRunLauncher creates ephemeral
# run containers straight from this image via the Docker Engine API,
# bypassing docker-compose entirely -- so your compose-level `entrypoint:`
# overrides never reach those containers. Docker always runs
# ENTRYPOINT + CMD together, and only CMD can be overridden without also
# explicitly overriding ENTRYPOINT. So every run container was actually
# executing:
#   dagster api grpc -h 0.0.0.0 -p 4000 -m gdelt_pipeline  dagster api execute_run {...}
# as one command, and Click rejected the second half as unexpected extra
# arguments -- the run container crashed on start, every time, which is
# why materialize all just sat there stuck.
#
# The grpc command now lives ONLY in docker-compose.yml's `entrypoint:`
# override for the dagster-user-code service. Run containers spawned by
# DockerRunLauncher now get no fixed entrypoint to collide with, so
# "dagster api execute_run ..." runs cleanly on its own.