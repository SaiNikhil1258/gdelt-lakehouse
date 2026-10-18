FROM bitnamilegacy/spark:3.5.3

USER root

RUN pip install --no-cache-dir \
    jupyterlab \
    ipykernel \
    py4j==0.10.9.7

WORKDIR /home/spark/notebooks

USER 1001