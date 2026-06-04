"""
Persist fraud alerts to DynamoDB and send SNS notifications for high-severity cases.
"""

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

if TYPE_CHECKING:
    from fraud_rules import FraudEvaluation

logger = logging.getLogger(__name__)


class AlertManager:
    def __init__(self, table_name: str, sns_topic_arn: str, region: str = "us-east-1"):
        self._dynamodb = boto3.resource("dynamodb", region_name=region)
        self._sns = boto3.client("sns", region_name=region)
        self._table = self._dynamodb.Table(table_name)
        self._sns_topic_arn = sns_topic_arn

    def save_alert(self, evaluation: "FraudEvaluation", transaction: dict) -> None:
        item = {
            "transaction_id": evaluation.transaction_id,
            "detected_at": datetime.now(timezone.utc).isoformat(),
            "account_id": transaction.get("account_id"),
            "amount_usd": str(transaction.get("amount_usd", 0)),
            "action": evaluation.action,
            "risk_score": str(round(evaluation.total_score, 4)),
            "signals": json.dumps(
                [{"rule": s.rule_name, "reason": s.reason} for s in evaluation.signals if s.triggered]
            ),
            "ttl": int(datetime.now(timezone.utc).timestamp()) + (90 * 86400),  # 90-day TTL
        }
        try:
            self._table.put_item(Item=item)
            logger.info(
                "Saved fraud alert: transaction_id=%s action=%s score=%.2f",
                evaluation.transaction_id,
                evaluation.action,
                evaluation.total_score,
            )
        except ClientError as e:
            logger.error("DynamoDB put_item failed: %s", e.response["Error"]["Message"])
            raise

    def send_notification(self, evaluation: "FraudEvaluation", transaction: dict) -> None:
        if evaluation.action not in ("FLAG", "BLOCK"):
            return

        subject = f"[{evaluation.action}] Fraud Alert — {evaluation.transaction_id}"
        triggered_rules = [s.rule_name for s in evaluation.signals if s.triggered]
        message = {
            "transaction_id": evaluation.transaction_id,
            "account_id": transaction.get("account_id"),
            "action": evaluation.action,
            "risk_score": round(evaluation.total_score, 4),
            "amount_usd": transaction.get("amount_usd"),
            "triggered_rules": triggered_rules,
            "detected_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            self._sns.publish(
                TopicArn=self._sns_topic_arn,
                Subject=subject,
                Message=json.dumps(message, indent=2),
                MessageAttributes={
                    "action": {"DataType": "String", "StringValue": evaluation.action},
                    "risk_score": {
                        "DataType": "Number",
                        "StringValue": str(evaluation.total_score),
                    },
                },
            )
            logger.info("SNS notification sent for %s", evaluation.transaction_id)
        except ClientError as e:
            logger.error("SNS publish failed: %s", e.response["Error"]["Message"])

    def process(self, evaluation: "FraudEvaluation", transaction: dict) -> None:
        if evaluation.is_fraud:
            self.save_alert(evaluation, transaction)
            self.send_notification(evaluation, transaction)
