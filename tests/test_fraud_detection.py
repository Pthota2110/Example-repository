"""Unit tests for the fraud rule engine."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fraud_detection"))

from fraud_rules import FraudEvaluation, FraudRuleEngine

DEFAULT_THRESHOLDS = {
    "high_amount_usd": 10_000,
    "velocity_per_hour": 10,
    "flag_score": 0.4,
    "block_score": 0.8,
    "high_risk_countries": ["NG", "RO", "KP", "IR"],
}

BASE_TXN = {
    "transaction_id": "test-txn-001",
    "account_id": "a" * 64,
    "amount_usd": 50.0,
    "transaction_type": "DEBIT",
    "transaction_hour": 14,  # 2pm — normal
    "is_international": False,
    "country_code": "US",
    "merchant_id": "MERCHANT_0001",
    "device_id": "abc123",
}


def make_engine(overrides: dict | None = None) -> FraudRuleEngine:
    thresholds = {**DEFAULT_THRESHOLDS, **(overrides or {})}
    return FraudRuleEngine(thresholds)


class TestHighAmountRule:
    def test_below_threshold_no_signal(self):
        engine = make_engine()
        result = engine.evaluate({**BASE_TXN, "amount_usd": 5_000})
        triggered = [s for s in result.signals if s.rule_name == "HIGH_AMOUNT" and s.triggered]
        assert not triggered

    def test_above_threshold_triggers(self):
        engine = make_engine()
        result = engine.evaluate({**BASE_TXN, "amount_usd": 15_000})
        triggered = [s for s in result.signals if s.rule_name == "HIGH_AMOUNT" and s.triggered]
        assert len(triggered) == 1
        assert triggered[0].score == pytest.approx(0.4)

    def test_custom_threshold_respected(self):
        engine = make_engine({"high_amount_usd": 1_000})
        result = engine.evaluate({**BASE_TXN, "amount_usd": 1_500})
        triggered = [s for s in result.signals if s.rule_name == "HIGH_AMOUNT" and s.triggered]
        assert len(triggered) == 1


class TestUnusualHourRule:
    @pytest.mark.parametrize("hour", [1, 2, 3, 4])
    def test_high_risk_hours_trigger(self, hour):
        engine = make_engine()
        result = engine.evaluate({**BASE_TXN, "transaction_hour": hour})
        triggered = [s for s in result.signals if s.rule_name == "UNUSUAL_HOUR" and s.triggered]
        assert len(triggered) == 1

    @pytest.mark.parametrize("hour", [0, 5, 12, 23])
    def test_normal_hours_no_signal(self, hour):
        engine = make_engine()
        result = engine.evaluate({**BASE_TXN, "transaction_hour": hour})
        triggered = [s for s in result.signals if s.rule_name == "UNUSUAL_HOUR" and s.triggered]
        assert not triggered


class TestInternationalRule:
    def test_high_risk_country_high_score(self):
        engine = make_engine()
        result = engine.evaluate({**BASE_TXN, "is_international": True, "country_code": "NG"})
        sig = next(s for s in result.signals if s.rule_name == "INTERNATIONAL")
        assert sig.triggered
        assert sig.score == pytest.approx(0.5)

    def test_low_risk_international_lower_score(self):
        engine = make_engine()
        result = engine.evaluate({**BASE_TXN, "is_international": True, "country_code": "GB"})
        sig = next(s for s in result.signals if s.rule_name == "INTERNATIONAL")
        assert sig.triggered
        assert sig.score == pytest.approx(0.15)

    def test_domestic_no_signal(self):
        engine = make_engine()
        result = engine.evaluate({**BASE_TXN, "is_international": False, "country_code": "US"})
        sig = next(s for s in result.signals if s.rule_name == "INTERNATIONAL")
        assert not sig.triggered


class TestVelocityRule:
    def test_within_limit_no_signal(self):
        engine = make_engine()
        result = engine.evaluate(BASE_TXN, recent_count=5)
        sig = next(s for s in result.signals if s.rule_name == "VELOCITY")
        assert not sig.triggered

    def test_exceeds_limit_triggers(self):
        engine = make_engine()
        result = engine.evaluate(BASE_TXN, recent_count=15)
        sig = next(s for s in result.signals if s.rule_name == "VELOCITY")
        assert sig.triggered
        assert sig.score == pytest.approx(0.6)


class TestOverallEvaluation:
    def test_clean_transaction_allowed(self):
        engine = make_engine()
        result = engine.evaluate(BASE_TXN)
        assert result.action == "ALLOW"
        assert not result.is_fraud

    def test_high_risk_transaction_flagged(self):
        engine = make_engine()
        txn = {
            **BASE_TXN,
            "amount_usd": 15_000,
            "is_international": True,
            "country_code": "NG",
        }
        result = engine.evaluate(txn)
        assert result.action in ("FLAG", "BLOCK")
        assert result.is_fraud
        assert result.total_score >= 0.4

    def test_multiple_rules_cumulative_score(self):
        engine = make_engine()
        txn = {
            **BASE_TXN,
            "amount_usd": 15_000,  # +0.4
            "transaction_hour": 2,  # +0.2
            "is_international": True,
            "country_code": "NG",  # +0.5
        }
        result = engine.evaluate(txn, recent_count=15)  # +0.6
        # Score is capped at 1.0
        assert result.total_score == pytest.approx(1.0)
        assert result.action == "BLOCK"

    def test_to_dict_structure(self):
        engine = make_engine()
        result = engine.evaluate(BASE_TXN)
        d = result.to_dict()
        assert "transaction_id" in d
        assert "total_score" in d
        assert "action" in d
        assert "signals" in d
        assert isinstance(d["signals"], list)
