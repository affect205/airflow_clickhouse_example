import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from datetime import timedelta
import json
from pprint import pprint
import requests
import clickhouse_connect


def load_rate_btc_to_usd_latest():
    url = 'https://api.exchangerate.host/latest?base=BTC&source=crypto&symbols=USD'
    resp = requests.get(url)
    return resp.json()


def save_raw_data_to_clickhouse(ds, **context):
    print("Task execution context:")
    pprint(context)
    print(ds)

    try:
        rate_data = load_rate_btc_to_usd_latest()
    except Exception as ex:
        print("Can't load data from https://api.exchangerate.host:\n{0}".format(str(ex)))
        raise ex

    try:
        client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    except Exception as ex:
        print("Can't connect to clickhouse:\n{0}".format(str(ex)))
        raise ex

    query = "INSERT INTO airflow_db.exchangerate_latest_raw(run_id, data) VALUES ('{0}', '{1}')"\
        .format(context['run_id'], str(rate_data).replace("'", "\""))

    print("Execute query: {0}".format(query))
    try:
        client.command(query)
    except Exception as ex:
        print("Can't store data to clickhouse:\n{0}".format(str(ex)))
        raise ex

    return True


def save_typed_data_to_clickhouse(ds, **context):
    print("Task execution context:")
    pprint(context)
    print(ds)

    try:
        client = clickhouse_connect.get_client(host='clickhouse', port=8123)
    except Exception as ex:
        print("Can't connect to clickhouse:\n{0}".format(str(ex)))
        raise ex

    fetch_query = "SELECT data FROM airflow_db.exchangerate_latest_raw WHERE run_id='{}'".format(context['run_id'])

    print("Execute query: {0}".format(fetch_query))
    try:
        result = client.query(fetch_query)
    except Exception as ex:
        print("Can't fetch data from clickhouse:\n{0}".format(str(ex)))
        raise ex

    if not result.result_set or not result.result_set[0][0]:
        error_msg = "Error! Record with run_id='{0}' not found in airflow_db.exchangerate_latest_raw".format(context['run_id'])
        print(error_msg)
        raise Exception(error_msg)

    print("Fetch result:\n{0}".format(result.result_set[0][0]))

    raw_data = json \
        .loads(result.result_set[0][0]
               .replace("True", "true")
               .replace("False", "false"))

    event_dt = datetime.strptime(context['ts_nodash'], '%Y%m%dT%H%M%S')

    insert_query = """INSERT INTO airflow_db.exchangerate_latest_typed(run_id, event_date, source_symbol, target_symbol, rate) 
    VALUES ('{0}', toDateTime('{1}'), 'BTC', 'USD', {2})""".format(
        context['run_id'],
        event_dt.strftime("%Y-%m-%d %H:%M:%S"),
        raw_data['rates']['USD']
    )

    print("Execute {0}".format(insert_query))
    try:
        client.command(insert_query)
    except Exception as ex:
        print("Can't insert data to clickhouse:\n{0}".format(str(ex)))
        raise ex

    return True


dag_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

btc_to_usd_dag = DAG(
    dag_id = "btc_to_usd_rate_3_hours",
    default_args = dag_args,
    max_active_runs = 2,
    schedule_interval = '0 */3 * * *',
    # schedule_interval=None,
    # schedule_interval = '*/5 * * * *',
    dagrun_timeout = timedelta(minutes=1),
    description = 'use case of python operator in airflow',
    start_date = airflow.utils.dates.days_ago(0)
)


btc_to_usd_raw_task = PythonOperator(
    task_id='btc_to_usd_raw_task',
    python_callable=save_raw_data_to_clickhouse,
    dag=btc_to_usd_dag,
    task_concurrency=2,
    provide_context=True
)
btc_to_usd_typed_task = PythonOperator(
    task_id='btc_to_usd_typed_task',
    python_callable=save_typed_data_to_clickhouse,
    dag=btc_to_usd_dag,
    task_concurrency=2,
    provide_context=True
)


btc_to_usd_raw_task >> btc_to_usd_typed_task

if __name__ == "__main__":
    btc_to_usd_dag.cli()
