"""
Kinesis Data Streams consumer for real-time fraud detection.

Retrieves per-account velocity from DynamoDB, loads rule thresholds from
Parameter Store, and delegates scoring to FraudRuleEngine.
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.exceptions import ClientError

from fraud_rules import FraudRuleEngine
from alert_manager import AlertManager

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_ssm_client = boto3.client("ssm", region_name=os.environ.get("AWS_REGION", "us-east-1"))
_dynamodb = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1"))

VELOCITY_TABLE = os.environ["VELOCITY_TABLE"]
FRAUD_ALERT_TABLE = os.environ["FRAUD_ALERT_TABLE"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
THRESHOLDS_PARAM = os.environ.get("THRESHOLDS_PARAM", "/financial-platform/fraud/thresholds")

_cached_thresholds: dict | None = None


def load_thresholds() -> dict:
    global _cached_thresholds
    if _cached_thresholds is None:
        try:
            response = _ssm_client.get_parameter(Name=THRESHOLDS_PARAM, WithDecryption=False)
            _cached_thresholds = json.loads(response["Parameter"]["Value"])
        except ClientError:
            logger.warning("Could not load thresholds from SSM; using defaults")
            _cached_thresholds = {
                "high_amount_usd": 10000,
                "velocity_per_hour": 10,
                "flag_score": 0.4,
                "block_score": 0.8,
                "high_risk_countries": ["NG", "RO", "KP", "IR"],
            }
    return _cached_thresholds


def get_velocity_count(account_id: str) -> int:
    """Count transactions for this account in the last 60 minutes via DynamoDB."""
    table = _dynamodb.Table(VELOCITY_TABLE)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
    try:
        response = table.query(
            KeyConditionExpression="account_id = :aid AND created_at > :cutoff",
            ExpressionAttributeValues={":aid": account_id, ":cutoff": cutoff},
            Select="COUNT",
        )
        return response.get("Count", 0)
    except ClientError as e:
        logger.error("Velocity lookup failed: %s", e.response["Error"]["Message"])
        return 0


def check_new_merchant(account_id: str, merchant_id: str) -> bool:
    """Return True if this account has never transacted at this merchant."""
    table = _dynamodb.Table(VELOCITY_TABLE)
    try:
        response = table.get_item(
            Key={"account_id": account_id, "merchant_id": merchant_id}
        )
        return "Item" not in response
    except ClientError:
        return False


def record_transaction(account_id: str, merchant_id: str | None, txn_id: str) -> None:
    """Write a lightweight record for velocity and merchant-history lookups."""
    table = _dynamodb.Table(VELOCITY_TABLE)
    item = {
        "account_id": account_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "transaction_id": txn_id,
        "ttl": int(datetime.now(timezone.utc).timestamp()) + 3600,
    }
    if merchant_id:
        item["merchant_id"] = merchant_id
    try:
        table.put_item(Item=item)
    except ClientError as e:
        logger.error("Velocity record write failed: %s", e.response["Error"]["Message"])


def process_transaction(txn: dict, engine: FraudRuleEngine, alert_mgr: AlertManager) -> dict:
    account_id = txn.get("account_id", "")
    merchant_id = txn.get("merchant_id")

    velocity = get_velocity_count(account_id)
    is_new_merchant = check_new_merchant(account_id, merchant_id) if merchant_id else False

    evaluation = engine.evaluate(txn, recent_count=velocity, is_new_merchant=is_new_merchant)
    alert_mgr.process(evaluation, txn)
    record_transaction(account_id, merchant_id, txn["transaction_id"])

    logger.info(
        "transaction_id=%s action=%s score=%.2f",
        txn["transaction_id"], evaluation.action, evaluation.total_score
    )
    return evaluation.to_dict()


def process_stream_batch(records: list[dict]) -> list[dict]:
    thresholds = load_thresholds()
    engine = FraudRuleEngine(thresholds)
    alert_mgr = AlertManager(FRAUD_ALERT_TABLE, SNS_TOPIC_ARN)

    results = []
    for record in records:
        try:
            txn = json.loads(record["data"])
            result = process_transaction(txn, engine, alert_mgr)
            results.append(result)
        except (json.JSONDecodeError, KeyError) as e:
            logger.error("Malformed record skipped: %s", e)
    return results
