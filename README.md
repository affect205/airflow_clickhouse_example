# airflow_clickhouse_example
Working example project of apache airflow and clickhouse from docker compose

#### Start services:
docker-compose -f docker-compose.airflow.yaml up -d

#### clickhouse web ui client:
http://localhost:8123/play

#### airflow web ui:
http://localhost:8080/home

#### airflow user:
airflow

#### airflow password:
airflow

#### Stop services:
docker-compose -f docker-compose.airflow.yaml down
