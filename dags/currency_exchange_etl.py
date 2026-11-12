"""Daily ETL: Frankfurter API  ->  transform  ->  MongoDB.

Three tasks, connected in order:  extract_exchange_rates >> transform_exchange_rates >> load_to_mongodb

Data moves between the tasks through NAMED XComs (an explicit key on every push and pull):
    extract   pushes  key="raw_rates"          (the API answer)
    transform pulls   key="raw_rates"  and pushes  key="transformed_rates"
    load      pulls   key="transformed_rates"
"""
from __future__ import annotations

import logging
import os
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from include.currency_etl import fetch_rates, load_documents, transform_rates

log = logging.getLogger(__name__)

BASE_CURRENCY = "EUR"
QUOTE_CURRENCIES = "USD,GBP,JPY"

XCOM_RAW = "raw_rates"
XCOM_TRANSFORMED = "transformed_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily EUR exchange rates from the Frankfurter API into MongoDB",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,  # the API gives the latest rates, so replaying past days would only repeat today's data
    default_args={"owner": "23L0682", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():
    @task(task_id="extract_exchange_rates")
    def extract(ti=None) -> None:
        raw_rates = fetch_rates(BASE_CURRENCY, QUOTE_CURRENCIES)
        log.info("Extracted %d rates: %s", len(raw_rates), raw_rates)
        ti.xcom_push(key=XCOM_RAW, value=raw_rates)

    @task(task_id="transform_exchange_rates")
    def transform(ti=None) -> None:
        raw_rates = ti.xcom_pull(task_ids="extract_exchange_rates", key=XCOM_RAW)
        documents = transform_rates(raw_rates)
        log.info("Transformed into %d documents: %s", len(documents), documents)
        ti.xcom_push(key=XCOM_TRANSFORMED, value=documents)

    @task(task_id="load_to_mongodb")
    def load(ti=None) -> None:
        documents = ti.xcom_pull(task_ids="transform_exchange_rates", key=XCOM_TRANSFORMED)
        summary = load_documents(
            documents,
            mongo_uri=os.environ["MONGO_URI"],  # set in .env, never written in code
            db_name=os.environ.get("MONGO_DB", "currency_db"),
            collection_name=os.environ.get("MONGO_COLLECTION", "exchange_rates"),
        )
        log.info("MongoDB load finished: %s", summary)

    extract() >> transform() >> load()


currency_exchange_etl()
