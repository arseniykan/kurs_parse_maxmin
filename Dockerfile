FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y \
    python3-pip \
    && pip install requests beautifulsoup4

USER airflow