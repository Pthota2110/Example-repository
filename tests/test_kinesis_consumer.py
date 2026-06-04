"""Unit tests for fraud_detection/kinesis_consumer.py."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

# Must set env vars before the module is imported (module-level reads)
os.environ["VELOCITY_TABLE"] = "test-velocity"
os.environ["FRAUD_ALERT_TABLE"] = "test-alerts"
os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789:test"
os.environ.setdefault("AWS_REGION", "us-east-1")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fraud_detection"))

from botocore.exceptions import ClientError  # noqa: E402


def _client_error(code="InternalError"):
    return ClientError({"Error": {"Code": code, "Message": "err"}}, "op")


BASE_TXN = {
    "transaction_id": "txn-123",
    "account_id": "acc-abc",
    "amount_usd": 50.0,
    "transaction_type": "DEBIT",
    "transaction_hour": 14,
    "is_international": False,
    "country_code": "US",
    "merchant_id": "M001",
    "device_id": "dev1",
}


class TestLoadThresholds:
    def setup_method(self):
        import kinesis_consumer

        kinesis_consumer._cached_thresholds = None  # reset cache each test

    def test_loads_from_ssm(self):
        import kinesis_consumer

        thresholds = {"high_amount_usd": 5000, "flag_score": 0.3}
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": json.dumps(thresholds)}}
        kinesis_consumer._ssm_client = mock_ssm

        result = kinesis_consumer.load_thresholds()

        assert result["high_amount_usd"] == 5000
        mock_ssm.get_parameter.assert_called_once()

    def test_falls_back_to_defaults_on_ssm_error(self):
        import kinesis_consumer

        mock_ssm = MagicMock()
        mock_ssm.get_parameter.side_effect = _client_error()
        kinesis_consumer._ssm_client = mock_ssm

        result = kinesis_consumer.load_thresholds()

        assert "high_amount_usd" in result
        assert result["velocity_per_hour"] == 10

    def test_caches_result(self):
        import kinesis_consumer

        thresholds = {"high_amount_usd": 9999}
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {"Parameter": {"Value": json.dumps(thresholds)}}
        kinesis_consumer._ssm_client = mock_ssm

        kinesis_consumer.load_thresholds()
        kinesis_consumer.load_thresholds()

        assert mock_ssm.get_parameter.call_count == 1


class TestGetVelocityCount:
    def test_returns_count_from_dynamo(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_table.query.return_value = {"Count": 7}
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        count = kinesis_consumer.get_velocity_count("acc-1")

        assert count == 7

    def test_returns_zero_on_client_error(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_table.query.side_effect = _client_error()
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        count = kinesis_consumer.get_velocity_count("acc-1")

        assert count == 0

    def test_returns_zero_when_count_missing(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_table.query.return_value = {}
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        assert kinesis_consumer.get_velocity_count("acc-1") == 0


class TestCheckNewMerchant:
    def test_returns_true_when_no_item(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_table.get_item.return_value = {}
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        assert kinesis_consumer.check_new_merchant("acc-1", "M001") is True

    def test_returns_false_when_item_exists(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_table.get_item.return_value = {"Item": {"account_id": "acc-1"}}
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        assert kinesis_consumer.check_new_merchant("acc-1", "M001") is False

    def test_returns_false_on_client_error(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_table.get_item.side_effect = _client_error()
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        assert kinesis_consumer.check_new_merchant("acc-1", "M001") is False


class TestRecordTransaction:
    def test_writes_record_with_merchant(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        kinesis_consumer.record_transaction("acc-1", "M001", "txn-1")

        mock_table.put_item.assert_called_once()
        item = mock_table.put_item.call_args[1]["Item"]
        assert item["merchant_id"] == "M001"
        assert item["account_id"] == "acc-1"
        assert "ttl" in item

    def test_writes_record_without_merchant(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        kinesis_consumer.record_transaction("acc-1", None, "txn-1")

        item = mock_table.put_item.call_args[1]["Item"]
        assert "merchant_id" not in item

    def test_logs_error_on_client_error(self):
        import kinesis_consumer

        mock_table = MagicMock()
        mock_table.put_item.side_effect = _client_error()
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        kinesis_consumer.record_transaction("acc-1", None, "txn-1")
        # should not raise


class TestProcessStreamBatch:
    def setup_method(self):
        import kinesis_consumer

        kinesis_consumer._cached_thresholds = None

    def test_processes_valid_records(self):
        import kinesis_consumer

        kinesis_consumer._cached_thresholds = {
            "high_amount_usd": 10000,
            "velocity_per_hour": 10,
            "flag_score": 0.4,
            "block_score": 0.8,
            "high_risk_countries": [],
        }

        mock_table = MagicMock()
        mock_table.query.return_value = {"Count": 0}
        mock_table.get_item.return_value = {}
        mock_table.put_item.return_value = {}
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        mock_boto3_resource = MagicMock()
        mock_boto3_resource.Table.return_value = mock_table
        mock_boto3_client = MagicMock()

        with (
            patch("alert_manager.boto3.resource", return_value=mock_boto3_resource),
            patch("alert_manager.boto3.client", return_value=mock_boto3_client),
        ):
            records = [{"data": json.dumps(BASE_TXN)}]
            results = kinesis_consumer.process_stream_batch(records)

        assert len(results) == 1
        assert "action" in results[0]

    def test_skips_malformed_json(self):
        import kinesis_consumer

        kinesis_consumer._cached_thresholds = {
            "high_amount_usd": 10000,
            "velocity_per_hour": 10,
            "flag_score": 0.4,
            "block_score": 0.8,
            "high_risk_countries": [],
        }

        mock_table = MagicMock()
        mock_dynamo = MagicMock()
        mock_dynamo.Table.return_value = mock_table
        kinesis_consumer._dynamodb = mock_dynamo

        with (
            patch("alert_manager.boto3.resource", return_value=MagicMock()),
            patch("alert_manager.boto3.client", return_value=MagicMock()),
        ):
            results = kinesis_consumer.process_stream_batch([{"data": "not-json"}])

        assert results == []
