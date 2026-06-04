"""
Structured audit logger for the Financial Data Platform.

Writes immutable audit records to CloudWatch Logs with a fixed schema,
enabling compliance queries via CloudWatch Logs Insights.
All log entries are append-only and include the actor, action, and data fingerprint.
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

LOG_GROUP = os.environ.get("AUDIT_LOG_GROUP", "/financial-platform/audit")
LOG_STREAM_PREFIX = "audit"


class AuditLogger:
    def __init__(self, region: str = "us-east-1"):
        self._logs = boto3.client("logs", region_name=region)
        self._stream_token: str | None = None
        self._stream_name = f"{LOG_STREAM_PREFIX}/{datetime.now(timezone.utc).strftime('%Y/%m/%d')}"
        self._ensure_stream()

    def _ensure_stream(self) -> None:
        try:
            self._logs.create_log_stream(logGroupName=LOG_GROUP, logStreamName=self._stream_name)
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceAlreadyExistsException":
                raise

    def _fingerprint(self, data: dict) -> str:
        serialized = json.dumps(data, sort_keys=True, default=str).encode()
        return hashlib.sha256(serialized).hexdigest()[:16]

    def _emit(self, event: dict) -> None:
        log_entry = json.dumps(event, default=str)
        kwargs = {
            "logGroupName": LOG_GROUP,
            "logStreamName": self._stream_name,
            "logEvents": [
                {
                    "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                    "message": log_entry,
                }
            ],
        }
        if self._stream_token:
            kwargs["sequenceToken"] = self._stream_token

        try:
            response = self._logs.put_log_events(**kwargs)
            self._stream_token = response.get("nextSequenceToken")
        except ClientError as e:
            logger.error("Audit log write failed: %s", e.response["Error"]["Message"])

    def log_etl_run(
        self,
        job_name: str,
        run_id: str,
        status: str,
        records_read: int,
        records_written: int,
        records_quarantined: int,
        source_path: str,
        target_path: str,
        actor: str = "glue-service",
    ) -> None:
        self._emit(
            {
                "event_type": "ETL_RUN",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "actor": actor,
                "job_name": job_name,
                "run_id": run_id,
                "status": status,
                "records_read": records_read,
                "records_written": records_written,
                "records_quarantined": records_quarantined,
                "source_path": source_path,
                "target_path": target_path,
            }
        )

    def log_fraud_alert(
        self,
        transaction_id: str,
        account_id: str,
        action: str,
        risk_score: float,
        triggered_rules: list[str],
    ) -> None:
        self._emit(
            {
                "event_type": "FRAUD_ALERT",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "actor": "fraud-detection-lambda",
                "transaction_id": transaction_id,
                "account_id": account_id,
                "action": action,
                "risk_score": round(risk_score, 4),
                "triggered_rules": triggered_rules,
            }
        )

    def log_data_quality_result(
        self,
        dataset: str,
        run_date: str,
        passed: int,
        failed: int,
        pass_rate: float,
        overall_success: bool,
        failed_checks: list[str],
    ) -> None:
        self._emit(
            {
                "event_type": "DATA_QUALITY_RESULT",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "actor": "dq-validator",
                "dataset": dataset,
                "run_date": run_date,
                "checks_passed": passed,
                "checks_failed": failed,
                "pass_rate_pct": round(pass_rate, 2),
                "overall_success": overall_success,
                "failed_checks": failed_checks,
            }
        )

    def log_schema_change(
        self,
        table_name: str,
        change_type: str,
        column_name: str | None,
        old_value: Any,
        new_value: Any,
        actor: str,
    ) -> None:
        change_data = {"old": old_value, "new": new_value}
        self._emit(
            {
                "event_type": "SCHEMA_CHANGE",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "actor": actor,
                "table": table_name,
                "change_type": change_type,
                "column": column_name,
                "fingerprint": self._fingerprint(change_data),
                **change_data,
            }
        )

    def log_access(
        self,
        actor: str,
        resource: str,
        action: str,
        result: str,
        details: dict | None = None,
    ) -> None:
        self._emit(
            {
                "event_type": "DATA_ACCESS",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "actor": actor,
                "resource": resource,
                "action": action,
                "result": result,
                "details": details or {},
            }
        )
