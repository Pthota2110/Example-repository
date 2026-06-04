"""
Data quality validation using Great Expectations.

Loads an expectations suite from JSON, runs it against a Pandas/Spark DataFrame,
and emits pass/fail results to CloudWatch as custom metrics.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import boto3
import great_expectations as ge
from great_expectations.core.batch import RuntimeBatchRequest
import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CLOUDWATCH_NAMESPACE = "FinancialPlatform/DataQuality"


class DataQualityValidator:
    def __init__(
        self,
        expectations_path: str,
        cloudwatch_region: str = "us-east-1",
        emit_metrics: bool = True,
    ):
        self._expectations = self._load_expectations(expectations_path)
        self._cloudwatch = boto3.client("cloudwatch", region_name=cloudwatch_region) if emit_metrics else None
        self._emit_metrics = emit_metrics
        self._context = ge.get_context()

    @staticmethod
    def _load_expectations(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    def validate(self, df: pd.DataFrame, dataset_name: str, run_date: str | None = None) -> dict[str, Any]:
        # Create a validator from pandas DataFrame using the current API
        validator = self._context.sources.pandas_default.read_dataframe(df)
        
        results = {
            "dataset": dataset_name,
            "run_date": run_date or datetime.now(timezone.utc).date().isoformat(),
            "checks": [],
        }
        passed = 0
        failed = 0

        for expectation in self._expectations.get("expectations", []):
            exp_type = expectation["expectation_type"]
            kwargs = expectation.get("kwargs", {})

            try:
                # Call the expectation method on the validator
                result = getattr(validator, exp_type)(**kwargs)
                success = result.success
                check = {
                    "expectation": exp_type,
                    "column": kwargs.get("column"),
                    "success": success,
                    "result": result.result if hasattr(result, 'result') else {},
                }
                results["checks"].append(check)
                if success:
                    passed += 1
                else:
                    failed += 1
                    logger.warning(
                        "FAIL %s on column=%s kwargs=%s",
                        exp_type,
                        kwargs.get("column"),
                        kwargs,
                    )
            except Exception as e:
                logger.error("Error running %s: %s", exp_type, e)
                results["checks"].append({"expectation": exp_type, "success": False, "error": str(e)})
                failed += 1

        results["summary"] = {
            "total": passed + failed,
            "passed": passed,
            "failed": failed,
            "pass_rate": round(passed / max(passed + failed, 1) * 100, 2),
        }
        results["overall_success"] = failed == 0

        logger.info(
            "Validation complete: dataset=%s passed=%d failed=%d pass_rate=%.1f%%",
            dataset_name,
            passed,
            failed,
            results["summary"]["pass_rate"],
        )

        if self._emit_metrics:
            self._publish_metrics(dataset_name, results["summary"])

        return results

    def _publish_metrics(self, dataset_name: str, summary: dict) -> None:
        metrics = [
            {"MetricName": "PassedChecks", "Value": summary["passed"], "Unit": "Count"},
            {"MetricName": "FailedChecks", "Value": summary["failed"], "Unit": "Count"},
            {
                "MetricName": "PassRatePct",
                "Value": summary["pass_rate"],
                "Unit": "Percent",
            },
        ]
        dimensions = [{"Name": "Dataset", "Value": dataset_name}]
        metric_data = [{**m, "Dimensions": dimensions, "Timestamp": datetime.now(timezone.utc)} for m in metrics]
        try:
            self._cloudwatch.put_metric_data(
                Namespace=CLOUDWATCH_NAMESPACE,
                MetricData=metric_data,
            )
        except Exception as e:
            logger.error("CloudWatch metric publish failed: %s", e)


def run_validation_from_s3(
    s3_path: str,
    expectations_path: str,
    dataset_name: str,
    output_bucket: str,
) -> dict:
    """Load a Parquet file from S3, validate it, and write the results JSON back to S3."""
    import io

    import boto3

    s3 = boto3.client("s3")
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))

    validator = DataQualityValidator(expectations_path)
    results = validator.validate(df, dataset_name)

    output_key = f"dq-results/{dataset_name}/{datetime.now(timezone.utc).date()}/results.json"
    s3.put_object(
        Bucket=output_bucket,
        Key=output_key,
        Body=json.dumps(results, indent=2).encode(),
        ContentType="application/json",
    )
    logger.info("DQ results written to s3://%s/%s", output_bucket, output_key)
    return results


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Run data quality checks on a Parquet file")
    parser.add_argument("--input", required=True, help="S3 or local path to Parquet file")
    parser.add_argument("--config", default="data_quality/great_expectations/expectations.json")
    parser.add_argument("--dataset", default="transactions")
    parser.add_argument("--no-metrics", action="store_true", help="Skip CloudWatch metrics")
    args = parser.parse_args()

    if args.input.startswith("s3://"):
        import io

        import boto3 as b3

        s3c = b3.client("s3")
        bucket, key = args.input.replace("s3://", "").split("/", 1)
        df = pd.read_parquet(io.BytesIO(s3c.get_object(Bucket=bucket, Key=key)["Body"].read()))
    else:
        df = pd.read_parquet(args.input)

    v = DataQualityValidator(args.config, emit_metrics=not args.no_metrics)
    result = v.validate(df, args.dataset)
    print(json.dumps(result["summary"], indent=2))
    exit(0 if result["overall_success"] else 1)
