"""
Configurable fraud rule engine for real-time transaction scoring.

Rules are data-driven — thresholds live in DynamoDB so they can be updated
without a Lambda redeployment. Each rule returns a (triggered: bool, reason: str) tuple.
"""

import logging
from dataclasses import dataclass, field
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class FraudSignal:
    rule_name: str
    triggered: bool
    score: float  # 0.0–1.0 contribution to total risk score
    reason: str


@dataclass
class FraudEvaluation:
    transaction_id: str
    total_score: float
    signals: list[FraudSignal] = field(default_factory=list)
    is_fraud: bool = False
    action: str = "ALLOW"  # ALLOW | FLAG | BLOCK

    def to_dict(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "total_score": round(self.total_score, 4),
            "is_fraud": self.is_fraud,
            "action": self.action,
            "signals": [
                {
                    "rule": s.rule_name,
                    "triggered": s.triggered,
                    "score": s.score,
                    "reason": s.reason,
                }
                for s in self.signals
            ],
        }


class FraudRuleEngine:
    def __init__(self, thresholds: dict[str, Any]):
        self.thresholds = thresholds

    def _rule_high_amount(self, txn: dict) -> FraudSignal:
        limit = self.thresholds.get("high_amount_usd", 10_000)
        triggered = float(txn.get("amount_usd", 0)) > limit
        return FraudSignal(
            rule_name="HIGH_AMOUNT",
            triggered=triggered,
            score=0.4 if triggered else 0.0,
            reason=(f"Amount ${txn.get('amount_usd')} exceeds ${limit}" if triggered else ""),
        )

    def _rule_unusual_hour(self, txn: dict) -> FraudSignal:
        hour = int(txn.get("transaction_hour", 12))
        # Transactions between 01:00–04:00 UTC are statistically higher-risk
        triggered = 1 <= hour <= 4
        return FraudSignal(
            rule_name="UNUSUAL_HOUR",
            triggered=triggered,
            score=0.2 if triggered else 0.0,
            reason=(f"Transaction at {hour:02d}:00 UTC (high-risk window)" if triggered else ""),
        )

    def _rule_international(self, txn: dict) -> FraudSignal:
        triggered = bool(txn.get("is_international", False))
        high_risk_countries = set(self.thresholds.get("high_risk_countries", ["NG", "RO", "KP", "IR"]))
        is_high_risk = txn.get("country_code") in high_risk_countries
        score = 0.5 if is_high_risk else (0.15 if triggered else 0.0)
        reason = (
            f"Transaction from high-risk country {txn.get('country_code')}"
            if is_high_risk
            else ("International transaction" if triggered else "")
        )
        return FraudSignal(
            rule_name="INTERNATIONAL",
            triggered=triggered,
            score=score,
            reason=reason,
        )

    def _rule_velocity(self, txn: dict, recent_count: int) -> FraudSignal:
        limit = self.thresholds.get("velocity_per_hour", 10)
        triggered = recent_count > limit
        return FraudSignal(
            rule_name="VELOCITY",
            triggered=triggered,
            score=0.6 if triggered else 0.0,
            reason=(f"{recent_count} transactions in 1 hour (limit: {limit})" if triggered else ""),
        )

    def _rule_new_merchant(self, txn: dict, is_new_merchant: bool) -> FraudSignal:
        triggered = is_new_merchant and float(txn.get("amount_usd", 0)) > 500
        return FraudSignal(
            rule_name="NEW_MERCHANT_HIGH_VALUE",
            triggered=triggered,
            score=0.35 if triggered else 0.0,
            reason=("Large transaction at previously unseen merchant" if triggered else ""),
        )

    def _rule_card_not_present(self, txn: dict) -> FraudSignal:
        triggered = txn.get("device_id") is None and txn.get("transaction_type") == "DEBIT"
        return FraudSignal(
            rule_name="CARD_NOT_PRESENT",
            triggered=triggered,
            score=0.25 if triggered else 0.0,
            reason="Debit with no device_id (card-not-present)" if triggered else "",
        )

    def evaluate(
        self,
        txn: dict,
        recent_count: int = 0,
        is_new_merchant: bool = False,
    ) -> FraudEvaluation:
        signals = [
            self._rule_high_amount(txn),
            self._rule_unusual_hour(txn),
            self._rule_international(txn),
            self._rule_velocity(txn, recent_count),
            self._rule_new_merchant(txn, is_new_merchant),
            self._rule_card_not_present(txn),
        ]

        total_score = min(sum(s.score for s in signals if s.triggered), 1.0)
        block_threshold = self.thresholds.get("block_score", 0.8)
        flag_threshold = self.thresholds.get("flag_score", 0.4)

        is_fraud = total_score >= flag_threshold
        if total_score >= block_threshold:
            action = "BLOCK"
        elif total_score >= flag_threshold:
            action = "FLAG"
        else:
            action = "ALLOW"

        return FraudEvaluation(
            transaction_id=txn["transaction_id"],
            total_score=total_score,
            signals=signals,
            is_fraud=is_fraud,
            action=action,
        )
