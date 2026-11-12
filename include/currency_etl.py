"""Plain Python helpers for the currency ETL (no Airflow imports, so they are easy to unit-test).

The DAG in dags/currency_exchange_etl.py only wires these functions into three Airflow tasks.
"""
from __future__ import annotations

from datetime import datetime, timezone

import requests

API_URL = "https://api.frankfurter.dev/v2/rates"
SOURCE_NAME = "Frankfurter API"
REQUIRED_FIELDS = ("base", "quote", "rate", "date")


def fetch_rates(base: str, quotes: str, timeout: int = 30) -> list[dict]:
    """Extract: ask the Frankfurter API for the latest rates of `base` against `quotes` ("USD,GBP,JPY").

    The v2 endpoint answers with a JSON list, one object per currency:
        [{"date": "2026-11-12", "base": "EUR", "quote": "USD", "rate": 1.1123}, ...]
    """
    response = requests.get(API_URL, params={"base": base, "quotes": quotes}, timeout=timeout)
    response.raise_for_status()  # a 4xx/5xx answer fails the task, so Airflow can retry it
    payload = response.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError(f"Unexpected API answer (expected a non-empty list): {payload!r}")
    return payload


def transform_rates(raw_rates: list[dict], retrieved_at: datetime | None = None) -> list[dict]:
    """Transform: turn each API item into the document that is stored in MongoDB."""
    retrieved_at = retrieved_at or datetime.now(timezone.utc)
    stamp = retrieved_at.strftime("%Y-%m-%dT%H:%M:%S")  # e.g. 2026-11-05T07:15:00
    documents = []
    for item in raw_rates:
        missing = [field for field in REQUIRED_FIELDS if item.get(field) is None]
        if missing:
            raise ValueError(f"API item is missing {missing}: {item!r}")
        documents.append(
            {
                "base_currency": item["base"],
                "target_currency": item["quote"],
                "exchange_rate": float(item["rate"]),
                "rate_date": item["date"],
                "source": SOURCE_NAME,
                "retrieved_at": stamp,
            }
        )
    return documents


def load_documents(documents: list[dict], mongo_uri: str, db_name: str, collection_name: str) -> dict:
    """Load: upsert the documents into MongoDB.

    One document per (base, target, rate_date). Running the DAG twice on the same day updates the existing
    document instead of creating a duplicate, so the load is idempotent.
    """
    from pymongo import ASCENDING, MongoClient, UpdateOne

    if not documents:
        raise ValueError("Nothing to load: the transformed list is empty")
    with MongoClient(mongo_uri, serverSelectionTimeoutMS=10_000) as client:
        collection = client[db_name][collection_name]
        collection.create_index(
            [("base_currency", ASCENDING), ("target_currency", ASCENDING), ("rate_date", ASCENDING)],
            unique=True,
            name="uniq_pair_per_day",
        )
        operations = [
            UpdateOne(
                {k: doc[k] for k in ("base_currency", "target_currency", "rate_date")},
                {"$set": doc},
                upsert=True,
            )
            for doc in documents
        ]
        result = collection.bulk_write(operations)
        return {
            "inserted": result.upserted_count,
            "updated": result.modified_count,
            "total_in_collection": collection.count_documents({}),
        }
