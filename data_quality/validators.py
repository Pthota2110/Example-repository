"""
Data quality validation — pure-pandas expectation runner.

Loads an expectations suite from JSON, runs it against a Pandas DataFrame,
and optionally emits pass/fail metrics to CloudWatch.

boto3 is lazy-imported only when emit_metrics=True so unit tests run
without AWS credentials or the boto3 package installed.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

CLOUDWATCH_NAMESPACE = "FinancialPlatform/DataQuality"


# ---------------------------------------------------------------------------
# Pure-pandas expectation implementations
# ---------------------------------------------------------------------------


def _check(exp_type: str, kwargs: dict, df: pd.DataFrame) -> dict[str, Any]:
    """Dispatch an expectation type to a pure-pandas implementation."""
    col = kwargs.get("column")
    series = df[col] if col and col in df.columns else None

    if exp_type == "expect_table_row_count_to_be_between":
        n = len(df)
        lo, hi = kwargs.get("min_value", 0), kwargs.get("max_value", float("inf"))
        return {"success": lo <= n <= hi, "result": {"observed_value": n}}

    if exp_type == "expect_column_to_exist":
        return {"success": col in df.columns, "result": {}}

    if exp_type == "expect_column_values_to_not_be_null":
        if series is None:
            return {"success": False, "result": {"error": f"column '{col}' missing"}}
        null_pct = series.isna().mean()
        mostly = kwargs.get("mostly", 1.0)
        return {"success": (1 - null_pct) >= mostly, "result": {"null_percent": round(null_pct * 100, 2)}}

    if exp_type == "expect_column_values_to_be_unique":
        if series is None:
            return {"success": False, "result": {"error": f"column '{col}' missing"}}
        dup_count = series.duplicated().sum()
        return {"success": int(dup_count) == 0, "result": {"duplicate_count": int(dup_count)}}

    if exp_type == "expect_column_values_to_be_in_set":
        if series is None:
            return {"success": False, "result": {"error": f"column '{col}' missing"}}
        value_set = set(kwargs.get("value_set", []))
        mostly = kwargs.get("mostly", 1.0)
        valid_pct = series.dropna().isin(value_set).mean()
        bad = series.dropna()[~series.dropna().isin(value_set)].unique().tolist()
        return {"success": valid_pct >= mostly, "result": {"unexpected_values": bad[:5]}}

    if exp_type == "expect_column_values_to_be_between":
        if series is None:
            return {"success": False, "result": {"error": f"column '{col}' missing"}}
        lo = kwargs.get("min_value", float("-inf"))
        hi = kwargs.get("max_value", float("inf"))
        mostly = kwargs.get("mostly", 1.0)
        numeric = pd.to_numeric(series, errors="coerce")
        valid_pct = ((numeric >= lo) & (numeric <= hi)).mean()
        return {"success": valid_pct >= mostly, "result": {"pass_rate": round(float(valid_pct), 4)}}

    if exp_type == "expect_column_values_to_match_regex":
        if series is None:
            return {"success": False, "result": {"error": f"column '{col}' missing"}}
        mostly = kwargs.get("mostly", 1.0)
        pattern = kwargs.get("regex", "")
        valid_pct = series.dropna().astype(str).str.match(pattern).mean()
        return {"success": float(valid_pct) >= mostly, "result": {"pass_rate": round(float(valid_pct), 4)}}

    if exp_type == "expect_column_proportion_of_unique_values_to_be_between":
        if series is None:
            return {"success": False, "result": {"error": f"column '{col}' missing"}}
        prop = series.nunique() / max(len(series), 1)
        lo = kwargs.get("min_value", 0.0)
        hi = kwargs.get("max_value", 1.0)
        return {"success": lo <= prop <= hi, "result": {"observed_proportion": round(prop, 6)}}

    # Unknown expectation type — skip with a warning
    logger.warning("Unknown expectation type '%s' — skipped", exp_type)
    return {"success": True, "result": {"skipped": True}}


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


class DataQualityValidator:
    def __init__(
        self,
        expectations_path: str,
        cloudwatch_region: str = "us-east-1",
        emit_metrics: bool = True,
    ):
        self._expectations = self._load_expectations(expectations_path)
        self._cloudwatch_region = cloudwatch_region
        self._emit_metrics = emit_metrics

    @staticmethod
    def _load_expectations(path: str) -> dict:
        with open(path) as f:
            return json.load(f)

    def validate(self, df: pd.DataFrame, dataset_name: str, run_date: str | None = None) -> dict[str, Any]:
        results: dict[str, Any] = {
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
                outcome = _check(exp_type, kwargs, df)
                success = outcome["success"]
                results["checks"].append(
                    {
                        "expectation": exp_type,
                        "column": kwargs.get("column"),
                        "success": success,
                        "result": outcome.get("result", {}),
                    }
                )
                if success:
                    passed += 1
                else:
                    failed += 1
                    logger.warning("FAIL %s column=%s", exp_type, kwargs.get("column"))
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
        import boto3  # lazy import — only needed when emit_metrics=True

        cloudwatch = boto3.client("cloudwatch", region_name=self._cloudwatch_region)
        dimensions = [{"Name": "Dataset", "Value": dataset_name}]
        metric_data = [
            {
                "MetricName": name,
                "Value": value,
                "Unit": unit,
                "Dimensions": dimensions,
                "Timestamp": datetime.now(timezone.utc),
            }
            for name, value, unit in [
                ("PassedChecks", summary["passed"], "Count"),
                ("FailedChecks", summary["failed"], "Count"),
                ("PassRatePct", summary["pass_rate"], "Percent"),
            ]
        ]
        try:
            cloudwatch.put_metric_data(Namespace=CLOUDWATCH_NAMESPACE, MetricData=metric_data)
        except Exception as e:
            logger.error("CloudWatch metric publish failed: %s", e)


def run_validation_from_s3(s3_path: str, expectations_path: str, dataset_name: str, output_bucket: str) -> dict:
    """Load a Parquet file from S3, validate it, and write results JSON back to S3."""
    import io

    import boto3

    s3 = boto3.client("s3")
    bucket, key = s3_path.replace("s3://", "").split("/", 1)
    df = pd.read_parquet(io.BytesIO(s3.get_object(Bucket=bucket, Key=key)["Body"].read()))

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
