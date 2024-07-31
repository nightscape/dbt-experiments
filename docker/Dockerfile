# this image gets published to GHCR for production use
ARG py_version=3.11.2

FROM python:$py_version-slim-bullseye as base

RUN apt-get update \
    && apt-get dist-upgrade -y \
    && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    curl \
    gcc \
    git \
    libpq-dev \
    libsasl2-dev \
    make \
    openjdk-11-jdk \
    openssh-client \
    procps \
    python-dev-is-python2 \
    software-properties-common \
    unixodbc-dev \
    && apt-get clean \
    && rm -rf \
    /var/lib/apt/lists/* \
    /tmp/* \
    /var/tmp/*

ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

RUN python -m pip install --upgrade "pip>=24.0" "setuptools>=69.2.0" "wheel>=0.43.0" --no-cache-dir

FROM rust:bullseye as builder
RUN cargo install --locked watchexec-cli


FROM base as dbt-spark

ARG commit_ref=main
ARG extras=all

HEALTHCHECK CMD dbt --version || exit 1

WORKDIR /usr/app/dbt/
COPY --from=builder /usr/local/cargo/bin/watchexec /usr/local/bin/watchexec
COPY pyproject.toml requirements.lock ./
RUN PYTHONDONTWRITEBYTECODE=1 python -m pip install --no-cache-dir -r requirements.lock

COPY . .
CMD ["/usr/local/bin/watchexec", "-r", "-e", "py,sql,yml,ini", "-i", "target/**", "clear ; pytest -s -l --tb=long test_dbt_spark.py"]
