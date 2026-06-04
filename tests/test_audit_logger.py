"""Unit tests for data_quality/audit_logger.py."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from botocore.exceptions import ClientError  # noqa: E402


def _client_error(code="ResourceAlreadyExistsException"):
    return ClientError({"Error": {"Code": code, "Message": "err"}}, "op")


def _make_logger():
    """Create an AuditLogger with a fully mocked CloudWatch client."""
    mock_logs = MagicMock()
    mock_logs.put_log_events.return_value = {"nextSequenceToken": "tok-001"}
    with patch("data_quality.audit_logger.boto3.client", return_value=mock_logs):
        from data_quality.audit_logger import AuditLogger

        logger = AuditLogger()
    logger._logs = mock_logs
    return logger, mock_logs


class TestAuditLoggerInit:
    def test_creates_log_stream_on_init(self):
        mock_logs = MagicMock()
        with patch("data_quality.audit_logger.boto3.client", return_value=mock_logs):
            from data_quality.audit_logger import AuditLogger

            AuditLogger()
        mock_logs.create_log_stream.assert_called_once()

    def test_ignores_already_exists_error(self):
        mock_logs = MagicMock()
        mock_logs.create_log_stream.side_effect = _client_error("ResourceAlreadyExistsException")
        with patch("data_quality.audit_logger.boto3.client", return_value=mock_logs):
            from data_quality.audit_logger import AuditLogger

            AuditLogger()  # should not raise

    def test_raises_other_client_errors(self):
        mock_logs = MagicMock()
        mock_logs.create_log_stream.side_effect = _client_error("AccessDeniedException")
        with patch("data_quality.audit_logger.boto3.client", return_value=mock_logs):
            from data_quality.audit_logger import AuditLogger

            with pytest.raises(ClientError):
                AuditLogger()


class TestFingerprint:
    def test_returns_16_char_hex(self):
        audit_logger, _ = _make_logger()
        fp = audit_logger._fingerprint({"key": "value"})
        assert len(fp) == 16
        assert all(c in "0123456789abcdef" for c in fp)

    def test_deterministic_for_same_input(self):
        audit_logger, _ = _make_logger()
        fp1 = audit_logger._fingerprint({"a": 1, "b": 2})
        fp2 = audit_logger._fingerprint({"b": 2, "a": 1})
        assert fp1 == fp2  # sort_keys=True makes it order-independent

    def test_different_for_different_input(self):
        audit_logger, _ = _make_logger()
        assert audit_logger._fingerprint({"a": 1}) != audit_logger._fingerprint({"a": 2})


class TestEmit:
    def test_emit_calls_put_log_events(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger._emit({"event_type": "TEST", "value": 42})
        mock_logs.put_log_events.assert_called_once()

    def test_emit_stores_sequence_token(self):
        audit_logger, mock_logs = _make_logger()
        mock_logs.put_log_events.return_value = {"nextSequenceToken": "tok-xyz"}
        audit_logger._emit({"event_type": "TEST"})
        assert audit_logger._stream_token == "tok-xyz"

    def test_emit_includes_sequence_token_when_set(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger._stream_token = "tok-prev"
        audit_logger._emit({"event_type": "TEST"})
        call_kwargs = mock_logs.put_log_events.call_args[1]
        assert call_kwargs["sequenceToken"] == "tok-prev"

    def test_emit_skips_sequence_token_when_none(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger._stream_token = None
        audit_logger._emit({"event_type": "TEST"})
        call_kwargs = mock_logs.put_log_events.call_args[1]
        assert "sequenceToken" not in call_kwargs

    def test_emit_logs_error_on_client_error(self):
        audit_logger, mock_logs = _make_logger()
        mock_logs.put_log_events.side_effect = _client_error("ThrottlingException")
        audit_logger._emit({"event_type": "TEST"})  # should not raise

    def test_emitted_message_is_valid_json(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger._emit({"event_type": "TEST", "data": {"nested": True}})
        log_events = mock_logs.put_log_events.call_args[1]["logEvents"]
        assert len(log_events) == 1
        parsed = json.loads(log_events[0]["message"])
        assert parsed["event_type"] == "TEST"


class TestLogMethods:
    def test_log_etl_run_emits_correct_event_type(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger.log_etl_run(
            job_name="ingest-job",
            run_id="run-001",
            status="SUCCEEDED",
            records_read=1000,
            records_written=980,
            records_quarantined=20,
            source_path="s3://raw/",
            target_path="s3://bronze/",
        )
        msg = json.loads(mock_logs.put_log_events.call_args[1]["logEvents"][0]["message"])
        assert msg["event_type"] == "ETL_RUN"
        assert msg["job_name"] == "ingest-job"
        assert msg["records_read"] == 1000
        assert msg["actor"] == "glue-service"

    def test_log_fraud_alert_emits_correct_fields(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger.log_fraud_alert(
            transaction_id="txn-1",
            account_id="acc-1",
            action="BLOCK",
            risk_score=0.95,
            triggered_rules=["HIGH_AMOUNT", "VELOCITY"],
        )
        msg = json.loads(mock_logs.put_log_events.call_args[1]["logEvents"][0]["message"])
        assert msg["event_type"] == "FRAUD_ALERT"
        assert msg["action"] == "BLOCK"
        assert msg["risk_score"] == 0.95
        assert "HIGH_AMOUNT" in msg["triggered_rules"]

    def test_log_data_quality_result(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger.log_data_quality_result(
            dataset="transactions",
            run_date="2026-06-04",
            passed=18,
            failed=2,
            pass_rate=90.0,
            overall_success=False,
            failed_checks=["expect_column_values_to_be_unique"],
        )
        msg = json.loads(mock_logs.put_log_events.call_args[1]["logEvents"][0]["message"])
        assert msg["event_type"] == "DATA_QUALITY_RESULT"
        assert msg["checks_passed"] == 18
        assert msg["overall_success"] is False

    def test_log_schema_change_includes_fingerprint(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger.log_schema_change(
            table_name="fact_transactions",
            change_type="ADD_COLUMN",
            column_name="new_col",
            old_value=None,
            new_value="VARCHAR(100)",
            actor="data-engineer",
        )
        msg = json.loads(mock_logs.put_log_events.call_args[1]["logEvents"][0]["message"])
        assert msg["event_type"] == "SCHEMA_CHANGE"
        assert "fingerprint" in msg
        assert len(msg["fingerprint"]) == 16

    def test_log_access_emits_data_access(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger.log_access(
            actor="analyst@company.com",
            resource="redshift://financial_dw/fact_transactions",
            action="SELECT",
            result="ALLOWED",
            details={"rows_returned": 500},
        )
        msg = json.loads(mock_logs.put_log_events.call_args[1]["logEvents"][0]["message"])
        assert msg["event_type"] == "DATA_ACCESS"
        assert msg["actor"] == "analyst@company.com"
        assert msg["details"]["rows_returned"] == 500

    def test_log_access_defaults_details_to_empty_dict(self):
        audit_logger, mock_logs = _make_logger()
        audit_logger.log_access(
            actor="svc",
            resource="s3://bucket/key",
            action="GET",
            result="ALLOWED",
        )
        msg = json.loads(mock_logs.put_log_events.call_args[1]["logEvents"][0]["message"])
        assert msg["details"] == {}
