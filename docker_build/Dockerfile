# FROM apache/airflow
FROM apache/airflow:latest

# Install custom python libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

ENV PYTHONPATH=${PYTHONPATH}:/datafuel_libraries