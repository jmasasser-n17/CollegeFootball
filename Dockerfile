FROM apache/airflow:2.9.1-python3.11

USER root

# Install Chromium and Chromedriver for arm64
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-driver \
    wget \
    gnupg2

USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ dags/
COPY utils.py .
