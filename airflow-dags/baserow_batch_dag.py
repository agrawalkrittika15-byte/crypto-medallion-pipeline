from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

import os
BASEROW_TOKEN = os.environ.get('BASEROW_TOKEN', 'your_token_here')
CLICKHOUSE_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', 'changeme')
BASEROW_TABLE_ID = 208

def load_baserow_coins():
    import requests
    from clickhouse_connect import get_client
    client = get_client(host='clickhouse-server', port=8123, username='default', password=CLICKHOUSE_PASSWORD)
    url = f'http://baserow/api/database/rows/table/{BASEROW_TABLE_ID}/?user_field_names=true'
    headers = {
        'Authorization': f'Token {BASEROW_TOKEN}',
        'Host': 'localhost:8081',
    }
    resp = requests.get(url, headers=headers)
    resp.raise_for_status()
    data = resp.json()
    rows = []
    for r in data['results']:
        category = r['Category']['value'] if r.get('Category') else None
        tier = r['market_cap_tier']['value'] if r.get('market_cap_tier') else None
        rows.append((r['id'], r['Name'], r['Symbol'], category, tier))
    client.command('TRUNCATE TABLE crypto_pipeline.bronze_baserow_coins')
    client.insert('crypto_pipeline.bronze_baserow_coins', rows, column_names=['row_id','name','symbol','category','market_cap_tier'])

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'baserow_batch_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='load_baserow_coins',
        python_callable=load_baserow_coins,
    )