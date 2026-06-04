"""Unit tests for data_quality/data_lineage.py."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from botocore.exceptions import ClientError  # noqa: E402


def _client_error(code="InternalError"):
    return ClientError({"Error": {"Code": code, "Message": "err"}}, "op")


def _make_tracker():
    mock_table = MagicMock()
    mock_dynamo = MagicMock()
    mock_dynamo.Table.return_value = mock_table
    with patch("data_quality.data_lineage.boto3.resource", return_value=mock_dynamo):
        from data_quality.data_lineage import LineageTracker

        tracker = LineageTracker()
    tracker._table = mock_table
    return tracker, mock_table


class TestAssetType:
    def test_all_values_are_strings(self):
        from data_quality.data_lineage import AssetType

        for member in AssetType:
            assert isinstance(member.value, str)

    def test_expected_members_exist(self):
        from data_quality.data_lineage import AssetType

        assert AssetType.S3_PATH.value == "S3_PATH"
        assert AssetType.GLUE_JOB.value == "GLUE_JOB"
        assert AssetType.LAMBDA.value == "LAMBDA"
        assert AssetType.KINESIS_STREAM.value == "KINESIS_STREAM"
        assert AssetType.REDSHIFT_TABLE.value == "REDSHIFT_TABLE"


class TestLineageTrackerRecordRun:
    def test_returns_correct_lineage_id(self):
        tracker, _ = _make_tracker()
        from data_quality.data_lineage import AssetType

        lid = tracker.record_run(
            job_name="ingest-job",
            job_type=AssetType.GLUE_JOB,
            inputs=[{"type": "S3_PATH", "uri": "s3://raw/"}],
            outputs=[{"type": "S3_PATH", "uri": "s3://bronze/"}],
            run_id="run-001",
        )
        assert lid == "ingest-job#run-001"

    def test_puts_item_to_dynamo(self):
        tracker, mock_table = _make_tracker()
        from data_quality.data_lineage import AssetType

        tracker.record_run(
            job_name="transform-job",
            job_type=AssetType.GLUE_JOB,
            inputs=[],
            outputs=[],
            run_id="run-002",
            metadata={"layer": "bronze→silver"},
        )
        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["job_name"] == "transform-job"
        assert item["run_id"] == "run-002"
        assert json.loads(item["metadata"])["layer"] == "bronze→silver"
        assert "ttl" in item

    def test_handles_put_item_error_gracefully(self):
        tracker, mock_table = _make_tracker()
        from data_quality.data_lineage import AssetType

        mock_table.put_item.side_effect = _client_error()
        lid = tracker.record_run(
            job_name="job",
            job_type=AssetType.GLUE_JOB,
            inputs=[],
            outputs=[],
            run_id="run-err",
        )
        assert lid == "job#run-err"  # still returns the id

    def test_metadata_defaults_to_empty_dict(self):
        tracker, mock_table = _make_tracker()
        from data_quality.data_lineage import AssetType

        tracker.record_run("j", AssetType.LAMBDA, [], [], "r1")
        item = mock_table.put_item.call_args[1]["Item"]
        assert json.loads(item["metadata"]) == {}


class TestLineageTrackerGetLineage:
    def test_returns_parsed_item(self):
        tracker, mock_table = _make_tracker()
        raw_item = {
            "lineage_id": "job#run-1",
            "inputs": json.dumps([{"uri": "s3://raw/"}]),
            "outputs": json.dumps([{"uri": "s3://bronze/"}]),
            "metadata": json.dumps({"layer": "raw→bronze"}),
        }
        mock_table.get_item.return_value = {"Item": raw_item}

        result = tracker.get_lineage("job#run-1")

        assert result["inputs"] == [{"uri": "s3://raw/"}]
        assert result["outputs"] == [{"uri": "s3://bronze/"}]
        assert result["metadata"]["layer"] == "raw→bronze"

    def test_returns_none_when_item_missing(self):
        tracker, mock_table = _make_tracker()
        mock_table.get_item.return_value = {}

        assert tracker.get_lineage("job#missing") is None

    def test_returns_none_on_client_error(self):
        tracker, mock_table = _make_tracker()
        mock_table.get_item.side_effect = _client_error()

        assert tracker.get_lineage("job#run-1") is None


class TestLineageTrackerGetUpstream:
    def test_returns_items_from_scan(self):
        tracker, mock_table = _make_tracker()
        mock_table.scan.return_value = {"Items": [{"lineage_id": "job#run-1"}]}

        items = tracker.get_upstream("s3://bronze/data/")

        assert len(items) == 1
        assert items[0]["lineage_id"] == "job#run-1"

    def test_returns_empty_on_client_error(self):
        tracker, mock_table = _make_tracker()
        mock_table.scan.side_effect = _client_error()

        assert tracker.get_upstream("s3://bronze/") == []

    def test_returns_empty_when_no_matches(self):
        tracker, mock_table = _make_tracker()
        mock_table.scan.return_value = {"Items": []}

        assert tracker.get_upstream("s3://nonexistent/") == []


class TestLineageHelpers:
    def test_record_ingest_lineage(self):
        from data_quality.data_lineage import record_ingest_lineage

        tracker = MagicMock()
        tracker.record_run.return_value = "financial-ingest-job#run-1"

        lid = record_ingest_lineage(tracker, "run-1", "s3://raw/", "s3://bronze/")

        assert lid == "financial-ingest-job#run-1"
        call_kwargs = tracker.record_run.call_args[1]
        assert call_kwargs["job_name"] == "financial-ingest-job"
        assert call_kwargs["inputs"][0]["uri"] == "s3://raw/"
        assert call_kwargs["outputs"][0]["uri"] == "s3://bronze/"

    def test_record_transform_lineage(self):
        from data_quality.data_lineage import record_transform_lineage

        tracker = MagicMock()
        tracker.record_run.return_value = "financial-transform-job#run-1"

        record_transform_lineage(tracker, "run-1", "s3://bronze/", "s3://silver/")

        call_kwargs = tracker.record_run.call_args[1]
        assert call_kwargs["job_name"] == "financial-transform-job"
        assert "currency_normalize" in call_kwargs["metadata"]["transformations"]

    def test_record_load_lineage(self):
        from data_quality.data_lineage import record_load_lineage

        tracker = MagicMock()
        tracker.record_run.return_value = "financial-load-job#run-1"

        record_load_lineage(tracker, "run-1", "s3://silver/", "fact_transactions")

        call_kwargs = tracker.record_run.call_args[1]
        assert call_kwargs["outputs"][0]["uri"] == "redshift://fact_transactions"
        assert call_kwargs["metadata"]["load_strategy"] == "upsert"

    def test_record_fraud_lineage(self):
        from data_quality.data_lineage import record_fraud_lineage

        tracker = MagicMock()
        tracker.record_run.return_value = "fraud-detection-lambda#run-1"

        record_fraud_lineage(tracker, "run-1", "transactions-stream", "fraud-alerts")

        call_kwargs = tracker.record_run.call_args[1]
        assert "kinesis://transactions-stream" in call_kwargs["inputs"][0]["uri"]
        assert call_kwargs["metadata"]["engine"] == "rule-based"
