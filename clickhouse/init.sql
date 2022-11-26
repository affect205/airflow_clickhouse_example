CREATE DATABASE IF NOT EXISTS airflow_db;
USE airflow_db;
CREATE TABLE IF NOT EXISTS exchangerate_latest_raw(
    run_id      text,
    data        text
)
ENGINE = MergeTree()
PRIMARY KEY run_id
ORDER BY run_id;
CREATE TABLE IF NOT EXISTS exchangerate_latest_typed(
    run_id          text,
    event_date      DateTime,
    source_symbol   varchar(3),
    target_symbol   varchar(3),
    rate            Float64
)
ENGINE = MergeTree()
PRIMARY KEY run_id
ORDER BY run_id;