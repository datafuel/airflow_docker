# Airflow v2 on Docker Compose

<p align="center">
  <a target="_blank" rel="noopener noreferrer">
    <img width="75%" src="https://upload.wikimedia.org/wikipedia/commons/d/de/AirflowLogo.png" alt="Docker+Compose+Airflow" />
  </a>
</p>

# About
Official Apache Airflow local docker compose setup

[Airflow Official documentation for docker compose deployment](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#docker-compose-yaml)


# Quickstart
1. Clone repo `git clone https://github.com/datafuel/airflow_docker.git`
2. Run `cd airflow_docker`
3. Run `echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" > .env` **(only on first use)**
4. Run `cd docker_build` and run `docker compose build`
5. Run `cd ..` and run `docker-compose up airflow-init` **(only on first use)**
6. Run `docker compose up` then access airflow on http://localhost:8080

# Default Credentials
- **Airflow username** : airflow
- **Airflow password** : airflow



