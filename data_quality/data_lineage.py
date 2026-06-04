"""
Data lineage tracking for the Financial Data Platform.

Records the provenance of each data asset — which job produced it, from which
inputs, at what time — and stores the lineage graph in DynamoDB.
Queryable via the AWS Glue Data Catalog and surfaced in the audit dashboard.
"""

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum
from typing import Any

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

LINEAGE_TABLE = os.environ.get("LINEAGE_TABLE", "financial-platform-lineage")


class AssetType(str, Enum):
    S3_PATH = "S3_PATH"
    REDSHIFT_TABLE = "REDSHIFT_TABLE"
    GLUE_JOB = "GLUE_JOB"
    LAMBDA = "LAMBDA"
    KINESIS_STREAM = "KINESIS_STREAM"
    QUICKSIGHT_DATASET = "QUICKSIGHT_DATASET"


class LineageTracker:
    def __init__(self, region: str = "us-east-1"):
        self._dynamo = boto3.resource("dynamodb", region_name=region)
        self._table = self._dynamo.Table(LINEAGE_TABLE)

    def record_run(
        self,
        job_name: str,
        job_type: AssetType,
        inputs: list[dict[str, str]],
        outputs: list[dict[str, str]],
        run_id: str,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Record a single pipeline run with its inputs and outputs.

        Each input/output dict should have: {"type": AssetType, "uri": "..."}.
        Returns the lineage_id for this run.
        """
        lineage_id = f"{job_name}#{run_id}"
        item = {
            "lineage_id": lineage_id,
            "job_name": job_name,
            "job_type": job_type.value,
            "run_id": run_id,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "inputs": json.dumps(inputs),
            "outputs": json.dumps(outputs),
            "metadata": json.dumps(metadata or {}),
            "ttl": int(datetime.now(timezone.utc).timestamp()) + (365 * 86400),  # 1-year retention
        }
        try:
            self._table.put_item(Item=item)
            logger.info("Lineage recorded: lineage_id=%s", lineage_id)
        except ClientError as e:
            logger.error("Lineage write failed: %s", e.response["Error"]["Message"])
        return lineage_id

    def get_lineage(self, lineage_id: str) -> dict | None:
        try:
            response = self._table.get_item(Key={"lineage_id": lineage_id})
            item = response.get("Item")
            if item:
                item["inputs"] = json.loads(item["inputs"])
                item["outputs"] = json.loads(item["outputs"])
                item["metadata"] = json.loads(item["metadata"])
            return item
        except ClientError as e:
            logger.error("Lineage read failed: %s", e.response["Error"]["Message"])
            return None

    def get_upstream(self, output_uri: str) -> list[dict]:
        """Find all runs that produced the given output URI (reverse lineage)."""
        try:
            response = self._table.scan(
                FilterExpression="contains(outputs, :uri)",
                ExpressionAttributeValues={":uri": output_uri},
            )
            return response.get("Items", [])
        except ClientError as e:
            logger.error("Lineage upstream query failed: %s", e.response["Error"]["Message"])
            return []


# Pre-built lineage helpers for each pipeline stage


def record_ingest_lineage(tracker: LineageTracker, run_id: str, source_path: str, bronze_path: str) -> str:
    return tracker.record_run(
        job_name="financial-ingest-job",
        job_type=AssetType.GLUE_JOB,
        inputs=[{"type": AssetType.S3_PATH.value, "uri": source_path}],
        outputs=[{"type": AssetType.S3_PATH.value, "uri": bronze_path}],
        run_id=run_id,
        metadata={"layer": "raw→bronze", "format": "csv→parquet"},
    )


def record_transform_lineage(tracker: LineageTracker, run_id: str, bronze_path: str, silver_path: str) -> str:
    return tracker.record_run(
        job_name="financial-transform-job",
        job_type=AssetType.GLUE_JOB,
        inputs=[{"type": AssetType.S3_PATH.value, "uri": bronze_path}],
        outputs=[{"type": AssetType.S3_PATH.value, "uri": silver_path}],
        run_id=run_id,
        metadata={
            "layer": "bronze→silver",
            "transformations": ["currency_normalize", "dedup", "enrich_category"],
        },
    )


def record_load_lineage(tracker: LineageTracker, run_id: str, silver_path: str, redshift_table: str) -> str:
    return tracker.record_run(
        job_name="financial-load-job",
        job_type=AssetType.GLUE_JOB,
        inputs=[{"type": AssetType.S3_PATH.value, "uri": silver_path}],
        outputs=[
            {
                "type": AssetType.REDSHIFT_TABLE.value,
                "uri": f"redshift://{redshift_table}",
            }
        ],
        run_id=run_id,
        metadata={"layer": "silver→gold", "load_strategy": "upsert"},
    )


def record_fraud_lineage(tracker: LineageTracker, run_id: str, kinesis_stream: str, alert_table: str) -> str:
    return tracker.record_run(
        job_name="fraud-detection-lambda",
        job_type=AssetType.LAMBDA,
        inputs=[
            {
                "type": AssetType.KINESIS_STREAM.value,
                "uri": f"kinesis://{kinesis_stream}",
            }
        ],
        outputs=[{"type": AssetType.REDSHIFT_TABLE.value, "uri": f"dynamodb://{alert_table}"}],
        run_id=run_id,
        metadata={"processing_model": "real-time", "engine": "rule-based"},
    )
