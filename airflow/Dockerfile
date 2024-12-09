FROM apache/airflow:slim-latest
USER root
RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
    msopenjdk-11 \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*
USER 50000
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
