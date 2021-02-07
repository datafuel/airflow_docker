airflow db init

echo "Initiating database with $AIRFLOW_ADMIN_FIRST_NAME $AIRFLOW_ADMIN_LAST_NAME credentials"
airflow users create \
    --username $AIRFLOW_ADMIN_USERNAME \
    --password $AIRFLOW_ADMIN_PASSWORD \
    --firstname $AIRFLOW_ADMIN_FIRST_NAME \
    --lastname $AIRFLOW_ADMIN_LAST_NAME \
    --role Admin \
    --email $AIRFLOW_ADMIN_EMAIL

airflow connections add 'trino' \
    --conn-uri 'presto://amir@trino-coordinator:8080/hive'

# pip install /datafuel/dist/datafuel-0.0.0-py3-none-any.whl
pip install -r /requirements/requirements.txt

airflow webserver --port 8080