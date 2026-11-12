from datetime import datetime
import requests
from pymongo import MongoClient

try:
    from airflow.sdk import DAG  # Airflow 3
except ImportError:
    from airflow import DAG  # Airflow 2

try:
    from airflow.providers.standard.operators.python import PythonOperator  # Airflow 3
except ImportError:
    from airflow.operators.python import PythonOperator  # Airflow 2

API_URL = "https://api.frankfurter.dev/v2/rates"
MONGO_URI = "mongodb://admin:admin@mongo:27017/"


# TASK 1: Extract
def extract_rates(ti):
    r = requests.get(
        API_URL,
        params={"base": "EUR", "quotes": "USD,GBP,JPY"},
        timeout=30,
    )
    r.raise_for_status()
    ti.xcom_push(key="raw_rates", value=r.json())  # named XCom


# TASK 2: Transform
def transform_rates(ti):
    raw = ti.xcom_pull(task_ids="extract_rates", key="raw_rates")
    now = datetime.now().isoformat(timespec="seconds")
    docs = []
    if isinstance(raw, list):  # v2 format: list of {date, base, quote, rate}
        for x in raw:
            docs.append({
                "base_currency": x["base"],
                "target_currency": x["quote"],
                "exchange_rate": x["rate"],
                "rate_date": x["date"],
                "source": "Frankfurter API",
                "retrieved_at": now,
            })
    else:  # fallback: {base, date, rates: {USD: ...}}
        for cur, rate in raw["rates"].items():
            docs.append({
                "base_currency": raw["base"],
                "target_currency": cur,
                "exchange_rate": rate,
                "rate_date": raw["date"],
                "source": "Frankfurter API",
                "retrieved_at": now,
            })
    ti.xcom_push(key="clean_rates", value=docs)  # named XCom


# TASK 3: Load
def load_to_mongodb(ti):
    docs = ti.xcom_pull(task_ids="transform_rates", key="clean_rates")
    col = MongoClient(MONGO_URI)["currency_db"]["exchange_rates"]
    for d in docs:
        col.update_one(  # upsert: no duplicates on reruns
            {
                "base_currency": d["base_currency"],
                "target_currency": d["target_currency"],
                "rate_date": d["rate_date"],
            },
            {"$set": d},
            upsert=True,
        )


with DAG(
    dag_id="currency_etl",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    t1 = PythonOperator(task_id="extract_rates", python_callable=extract_rates)
    t2 = PythonOperator(task_id="transform_rates", python_callable=transform_rates)
    t3 = PythonOperator(task_id="load_to_mongodb", python_callable=load_to_mongodb)

    t1 >> t2 >> t3