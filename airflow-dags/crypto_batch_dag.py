from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import csv
import os

CLICKHOUSE_PASSWORD = os.environ['CLICKHOUSE_PASSWORD']


def to_float(val):
    return float(val) if val not in ('', None) else None


def to_pydate(val):
    if val in ('', None):
        return None
    return datetime.strptime(val, '%Y-%m-%d').date()


def load_crypto_snapshot():
    from clickhouse_connect import get_client

    client = get_client(
        host='clickhouse-server',
        port=8123,
        username='default',
        password=CLICKHOUSE_PASSWORD,
    )

    rows = []
    with open('/opt/airflow/dags/data/crypto_daily_history.csv', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append((
                to_pydate(r['date']),
                r['coin_id'],
                r['symbol'],
                r['name'],
                to_float(r['market_cap_rank']),
                to_float(r['current_price_usd']),
                to_float(r['market_cap_usd']),
                to_float(r['fully_diluted_valuation']),
                to_float(r['total_volume_usd']),
                to_float(r['high_24h']),
                to_float(r['low_24h']),
                to_float(r['price_change_24h']),
                to_float(r['price_change_pct_1h']),
                to_float(r['price_change_pct_24h']),
                to_float(r['price_change_pct_7d']),
                to_float(r['price_change_pct_30d']),
                to_float(r['circulating_supply']),
                to_float(r['total_supply']),
                to_float(r['max_supply']),
                to_float(r['ath']),
                to_float(r['ath_change_pct']),
                to_pydate(r['ath_date']),
                to_float(r['atl']),
                to_float(r['atl_change_pct']),
                to_pydate(r['atl_date']),
            ))

    columns = [
        'date', 'coin_id', 'symbol', 'name', 'market_cap_rank', 'current_price_usd',
        'market_cap_usd', 'fully_diluted_valuation', 'total_volume_usd', 'high_24h', 'low_24h',
        'price_change_24h', 'price_change_pct_1h', 'price_change_pct_24h', 'price_change_pct_7d',
        'price_change_pct_30d', 'circulating_supply', 'total_supply', 'max_supply', 'ath',
        'ath_change_pct', 'ath_date', 'atl', 'atl_change_pct', 'atl_date',
    ]

    client.command('TRUNCATE TABLE crypto_pipeline.bronze_crypto_snapshot')
    client.insert('crypto_pipeline.bronze_crypto_snapshot', rows, column_names=columns)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    'crypto_batch_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    load_task = PythonOperator(
        task_id='load_crypto_snapshot',
        python_callable=load_crypto_snapshot,
    )
