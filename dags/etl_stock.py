from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests, pandas as pd
import psycopg2
from sqlalchemy import create_engine

def fetch_data():
    # url = "https://www.alphavantage.co/query"
    # params = {
    #     "function": "TIME_SERIES_DAILY",
    #     "symbol": "AAPL",
    #     "outputsize": "full",
    #     "datatype": "csv",
    #     "apikey": "demo"
    # }
    # response = requests.get(url, params=params)
    response = requests.get("https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=IBM&apikey=demo&datatype=csv")
    with open("/opt/airflow/dags/aapl.csv", "w") as f:
        f.write(response.text)

def load_to_postgres():
    df = pd.read_csv("/opt/airflow/dags/aapl.csv")
    engine = create_engine("postgresql+psycopg2://postgres:Mika2u7w@postgres:5432/stockdata")
    df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
    df.to_sql("daily_stock", engine, if_exists="replace", index=False)

def transfer_to_clickhouse():
    from clickhouse_driver import Client
    import pandas as pd

    df = pd.read_csv("/opt/airflow/dags/aapl.csv")
    df.columns = [c.lower().strip().replace(' ', '_') for c in df.columns]
    df['timestamp'] = pd.to_datetime(df['timestamp']).dt.date

    client = Client(
    host='clickhouse',
    user='etl_user',
    password='Mika2u7w',
    port=9000
    )


    client.execute('CREATE DATABASE IF NOT EXISTS stock')

    client.execute('DROP TABLE IF EXISTS stock.daily_stock')
    client.execute('''
        CREATE TABLE stock.daily_stock (
            timestamp Date,
            open Float64,
            high Float64,
            low Float64,
            close Float64,
            volume UInt64
        ) ENGINE = MergeTree()
        ORDER BY timestamp
    ''')

    data = [tuple(row) for _, row in df.iterrows()]
    client.execute(
        'INSERT INTO stock.daily_stock (timestamp, open, high, low, close, volume) VALUES',
        data
    )

default_args = {
    'start_date': datetime(2025, 5, 19),
    'catchup': False
}

with DAG("etl_stock_pipeline", schedule_interval="@daily", default_args=default_args) as dag:
    fetch = PythonOperator(task_id="fetch_data", python_callable=fetch_data)
    load_pg = PythonOperator(task_id="load_postgres", python_callable=load_to_postgres)
    load_ch = PythonOperator(task_id="load_clickhouse", python_callable=transfer_to_clickhouse)

    fetch >> load_pg >> load_ch
