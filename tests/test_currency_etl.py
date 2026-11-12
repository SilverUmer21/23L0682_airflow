"""Unit tests for the transform step (no Airflow, no network, no database needed).

Run with:  astro dev pytest tests/test_currency_etl.py
"""
from datetime import datetime, timezone

import pytest

from include.currency_etl import transform_rates

RAW = [
    {"date": "2026-11-12", "base": "EUR", "quote": "USD", "rate": 1.1123},
    {"date": "2026-11-12", "base": "EUR", "quote": "JPY", "rate": 177.3},
]


def test_transform_has_the_required_fields_and_values():
    docs = transform_rates(RAW, retrieved_at=datetime(2026, 11, 12, 7, 15, tzinfo=timezone.utc))
    assert docs[0] == {
        "base_currency": "EUR",
        "target_currency": "USD",
        "exchange_rate": 1.1123,
        "rate_date": "2026-11-12",
        "source": "Frankfurter API",
        "retrieved_at": "2026-11-12T07:15:00",
    }
    assert [d["target_currency"] for d in docs] == ["USD", "JPY"]


def test_transform_rejects_an_item_with_a_missing_field():
    with pytest.raises(ValueError):
        transform_rates([{"date": "2026-11-12", "base": "EUR", "quote": "USD"}])
