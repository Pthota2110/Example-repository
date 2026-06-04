"""Unit tests for fraud_detection/lambda_handler.py."""

import base64
import json
import os
import sys
from unittest.mock import patch

# Set required env vars before any import of the fraud_detection modules
os.environ.setdefault("VELOCITY_TABLE", "test-velocity")
os.environ.setdefault("FRAUD_ALERT_TABLE", "test-alerts")
os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fraud_detection"))  # noqa: E402


def _kinesis_record(payload: dict, seq: str = "seq-001") -> dict:
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    return {"kinesis": {"sequenceNumber": seq, "data": encoded}}


def _bad_record(seq: str = "seq-bad") -> dict:
    return {"kinesis": {"sequenceNumber": seq, "data": "!!!not-base64!!!"}}


class TestLambdaHandler:
    @patch("lambda_handler.process_stream_batch")
    def test_empty_event_returns_no_failures(self, mock_batch):
        from lambda_handler import handler

        mock_batch.return_value = []
        result = handler({"Records": []}, None)
        assert result == {"batchItemFailures": []}
        mock_batch.assert_called_once_with([])

    @patch("lambda_handler.process_stream_batch")
    def test_valid_records_decoded_and_processed(self, mock_batch):
        from lambda_handler import handler

        mock_batch.return_value = [{"action": "ALLOW", "total_score": 0.1}]
        txn = {"transaction_id": "t1", "account_id": "acc1", "amount_usd": 50}
        event = {"Records": [_kinesis_record(txn, "seq-1")]}

        result = handler(event, None)

        assert result == {"batchItemFailures": []}
        called_records = mock_batch.call_args[0][0]
        assert len(called_records) == 1
        assert json.loads(called_records[0]["data"])["transaction_id"] == "t1"

    @patch("lambda_handler.process_stream_batch")
    def test_invalid_base64_goes_to_failures(self, mock_batch):
        from lambda_handler import handler

        mock_batch.return_value = []
        event = {"Records": [_bad_record("seq-999")]}

        result = handler(event, None)

        assert {"itemIdentifier": "seq-999"} in result["batchItemFailures"]
        # bad record is not passed to batch processor
        called_records = mock_batch.call_args[0][0]
        assert called_records == []

    @patch("lambda_handler.process_stream_batch")
    def test_batch_exception_adds_all_to_failures(self, mock_batch):
        from lambda_handler import handler

        mock_batch.side_effect = RuntimeError("DynamoDB down")
        txn = {"transaction_id": "t1"}
        event = {"Records": [_kinesis_record(txn, "seq-1"), _kinesis_record(txn, "seq-2")]}

        result = handler(event, None)

        ids = [f["itemIdentifier"] for f in result["batchItemFailures"]]
        assert "seq-1" in ids
        assert "seq-2" in ids

    @patch("lambda_handler.process_stream_batch")
    def test_mixed_valid_and_invalid_records(self, mock_batch):
        from lambda_handler import handler

        mock_batch.return_value = [{"action": "ALLOW"}]
        txn = {"transaction_id": "t1"}
        event = {"Records": [_kinesis_record(txn, "seq-ok"), _bad_record("seq-bad")]}

        result = handler(event, None)

        ids = [f["itemIdentifier"] for f in result["batchItemFailures"]]
        assert "seq-bad" in ids
        assert "seq-ok" not in ids

    @patch("lambda_handler.process_stream_batch")
    def test_blocked_and_flagged_counted_in_logs(self, mock_batch, caplog):
        import logging

        from lambda_handler import handler

        mock_batch.return_value = [
            {"action": "BLOCK"},
            {"action": "FLAG"},
            {"action": "ALLOW"},
        ]
        event = {"Records": [_kinesis_record({"transaction_id": f"t{i}"}, f"seq-{i}") for i in range(3)]}

        with caplog.at_level(logging.INFO, logger="lambda_handler"):
            handler(event, None)

        assert any("blocked=1" in m for m in caplog.messages)
        assert any("flagged=1" in m for m in caplog.messages)

    @patch("lambda_handler.process_stream_batch")
    def test_missing_records_key_handled(self, mock_batch):
        from lambda_handler import handler

        mock_batch.return_value = []
        result = handler({}, None)
        assert result == {"batchItemFailures": []}
