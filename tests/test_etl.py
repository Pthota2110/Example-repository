"""Unit tests for ETL transform functions (no Spark/Glue required)."""

import pytest
import pandas as pd
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# Pure-Python equivalents of Glue transforms for unit testing

EXCHANGE_RATES = {
    "USD": 1.0, "EUR": 1.08, "GBP": 1.27, "JPY": 0.0067,
    "CAD": 0.74, "AUD": 0.65, "CHF": 1.12, "CNY": 0.14,
}

MCC_CATEGORY_MAP = {
    "5411": "Groceries", "5812": "Restaurants", "5541": "Gas Stations",
}

VALID_TYPES = {"DEBIT", "CREDIT", "TRANSFER", "REFUND"}
VALID_STATUSES = {"PENDING", "SETTLED", "FLAGGED", "REVERSED", "FAILED"}


def normalize_currency_pd(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["amount_usd"] = df.apply(
        lambda r: round(r["amount"] * EXCHANGE_RATES.get(r["currency"], 1.0), 2), axis=1
    )
    df["exchange_rate"] = df["currency"].map(EXCHANGE_RATES)
    return df


def validate_rows(df: pd.DataFrame):
    valid = df[
        df["transaction_id"].notna() &
        df["account_id"].notna() &
        df["amount"].notna() &
        df["transaction_type"].isin(VALID_TYPES) &
        df["status"].isin(VALID_STATUSES)
    ]
    quarantine = df[~df.index.isin(valid.index)]
    return valid, quarantine


def deduplicate_pd(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("updated_at", ascending=False).drop_duplicates("transaction_id")


class TestNormalizeCurrency:
    def test_usd_unchanged(self):
        df = pd.DataFrame([{"amount": 100.0, "currency": "USD"}])
        result = normalize_currency_pd(df)
        assert result.iloc[0]["amount_usd"] == 100.0
        assert result.iloc[0]["exchange_rate"] == 1.0

    def test_eur_conversion(self):
        df = pd.DataFrame([{"amount": 100.0, "currency": "EUR"}])
        result = normalize_currency_pd(df)
        assert result.iloc[0]["amount_usd"] == pytest.approx(108.0, rel=1e-4)

    def test_gbp_conversion(self):
        df = pd.DataFrame([{"amount": 50.0, "currency": "GBP"}])
        result = normalize_currency_pd(df)
        assert result.iloc[0]["amount_usd"] == pytest.approx(63.5, rel=1e-4)

    def test_unknown_currency_defaults_to_one(self):
        df = pd.DataFrame([{"amount": 100.0, "currency": "XYZ"}])
        result = normalize_currency_pd(df)
        assert result.iloc[0]["amount_usd"] == 100.0

    def test_multiple_currencies(self):
        df = pd.DataFrame([
            {"amount": 200.0, "currency": "USD"},
            {"amount": 100.0, "currency": "EUR"},
            {"amount": 80.0,  "currency": "GBP"},
        ])
        result = normalize_currency_pd(df)
        assert len(result) == 3
        assert result.iloc[1]["amount_usd"] == pytest.approx(108.0, rel=1e-4)


class TestValidateRows:
    def _base_row(self, **overrides) -> dict:
        row = {
            "transaction_id": "abc-123",
            "account_id": "a" * 64,
            "amount": 50.0,
            "transaction_type": "DEBIT",
            "status": "SETTLED",
        }
        row.update(overrides)
        return row

    def test_valid_row_passes(self):
        df = pd.DataFrame([self._base_row()])
        valid, quarantine = validate_rows(df)
        assert len(valid) == 1
        assert len(quarantine) == 0

    def test_null_transaction_id_quarantined(self):
        df = pd.DataFrame([self._base_row(transaction_id=None)])
        valid, quarantine = validate_rows(df)
        assert len(valid) == 0
        assert len(quarantine) == 1

    def test_invalid_transaction_type_quarantined(self):
        df = pd.DataFrame([self._base_row(transaction_type="WIRE")])
        valid, quarantine = validate_rows(df)
        assert len(valid) == 0
        assert len(quarantine) == 1

    def test_invalid_status_quarantined(self):
        df = pd.DataFrame([self._base_row(status="UNKNOWN")])
        valid, quarantine = validate_rows(df)
        assert len(valid) == 0
        assert len(quarantine) == 1

    def test_mixed_rows(self):
        rows = [
            self._base_row(transaction_id="good-1"),
            self._base_row(transaction_id=None),
            self._base_row(transaction_type="BAD"),
            self._base_row(transaction_id="good-2"),
        ]
        df = pd.DataFrame(rows)
        valid, quarantine = validate_rows(df)
        assert len(valid) == 2
        assert len(quarantine) == 2


class TestDeduplicate:
    def test_keeps_latest_updated_at(self):
        df = pd.DataFrame([
            {"transaction_id": "t1", "updated_at": "2026-01-01T10:00:00", "amount": 100},
            {"transaction_id": "t1", "updated_at": "2026-01-01T12:00:00", "amount": 200},
        ])
        result = deduplicate_pd(df)
        assert len(result) == 1
        assert result.iloc[0]["amount"] == 200

    def test_no_duplicates_unchanged(self):
        df = pd.DataFrame([
            {"transaction_id": "t1", "updated_at": "2026-01-01", "amount": 100},
            {"transaction_id": "t2", "updated_at": "2026-01-01", "amount": 200},
        ])
        result = deduplicate_pd(df)
        assert len(result) == 2
