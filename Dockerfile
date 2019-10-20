## Dockerfile
FROM python:2.7.15-stretch

RUN apt-get update && apt-get install -y -q --fix-missing --no-install-recommends\
    vim \
    less \
    curl \
    build-essential \
    git \
    netcat \
    libaio1 \
    libaio-dev \
    unzip \
    gnupg \
    dos2unix \
    alien \
    wget \
    net-tools \
    procps \
    dnsutils \
    telnet \
    libsasl2-2 \
    libsasl2-dev \
    python-dev \
    gcc  \
    supervisor


ARG AIRFLOW_BASE_ARG=/app/airflow
ENV AIRFLOW_HOME /app/airflow
ENV SLUGIFY_USES_TEXT_UNIDECODE yes
ENV ARCH x86_64

ENV PIP_DEFAULT_TIMEOUT 100

RUN apt-get install -y postgresql postgresql-contrib

RUN apt-get install -y supervisor

RUN service supervisor restart

COPY scripts/requirements.txt /app/requirements.txt

RUN pip install pip --upgrade

RUN pip install -r /app/requirements.txt

ADD . /app

COPY supervisord.conf /app/airflow

WORKDIR /app/airflow

RUN apt-get clean

RUN chmod 777 -R /app/airflow/

RUN chmod 777 -R /var/log/supervisor/

ENV AIRFLOW__CORE__FERNET_KEY=6sHzQYEX2m5yvtyF6B5_sXzMhtL7b-xIiHUhnykP9VM=

EXPOSE 8080

USER postgres

RUN service postgresql start &&\
    psql -c "CREATE USER airflow WITH PASSWORD 'airflow';" &&\
    psql -c 'create database airflow;' &&\
    psql -c 'grant all privileges on database airflow to airflow;'

RUN service postgresql start &&\
    airflow initdb

USER root

CMD supervisord -c supervisord.conf