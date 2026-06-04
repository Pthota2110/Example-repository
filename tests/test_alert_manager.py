"""Unit tests for fraud_detection/alert_manager.py."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("VELOCITY_TABLE", "test-velocity")
os.environ.setdefault("FRAUD_ALERT_TABLE", "test-alerts")
os.environ.setdefault("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:123456789:test")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "fraud_detection"))

from botocore.exceptions import ClientError  # noqa: E402


def _client_error(code: str = "InternalError") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "test error"}}, "op")


def _make_evaluation(
    txn_id="txn-001",
    action="FLAG",
    score=0.6,
    is_fraud=True,
):
    from fraud_rules import FraudRuleEngine

    engine = FraudRuleEngine(
        {
            "high_amount_usd": 10000,
            "velocity_per_hour": 10,
            "flag_score": 0.4,
            "block_score": 0.8,
            "high_risk_countries": [],
        }
    )
    txn = {
        "transaction_id": txn_id,
        "account_id": "acc-abc",
        "amount_usd": 15000.0,
        "transaction_type": "DEBIT",
        "transaction_hour": 14,
        "is_international": False,
        "country_code": "US",
        "merchant_id": "M001",
        "device_id": "dev1",
    }
    ev = engine.evaluate(txn)
    ev.action = action
    ev.is_fraud = is_fraud
    ev.total_score = score
    return ev


class TestAlertManager:
    def _make_manager(self, mock_dynamo, mock_sns):
        from alert_manager import AlertManager

        with (
            patch("alert_manager.boto3.resource", return_value=mock_dynamo),
            patch("alert_manager.boto3.client", return_value=mock_sns),
        ):
            mgr = AlertManager("test-table", "arn:aws:sns:us-east-1:123:topic")
        mgr._table = mock_dynamo.Table.return_value
        mgr._sns = mock_sns
        return mgr

    def test_save_alert_calls_put_item(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation()
        txn = {"account_id": "acc-1", "amount_usd": 15000}

        mgr.save_alert(ev, txn)

        mgr._table.put_item.assert_called_once()
        item = mgr._table.put_item.call_args[1]["Item"]
        assert item["transaction_id"] == ev.transaction_id
        assert item["action"] == ev.action
        assert "ttl" in item

    def test_save_alert_raises_on_client_error(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)
        mgr._table.put_item.side_effect = _client_error()

        with pytest.raises(ClientError):
            mgr.save_alert(_make_evaluation(), {"account_id": "a", "amount_usd": 100})

    def test_send_notification_skipped_for_allow(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation(action="ALLOW", is_fraud=False)
        mgr.send_notification(ev, {})

        mock_sns.publish.assert_not_called()

    def test_send_notification_called_for_flag(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation(action="FLAG")
        mgr.send_notification(ev, {"account_id": "acc1", "amount_usd": 500})

        mock_sns.publish.assert_called_once()
        call_kwargs = mock_sns.publish.call_args[1]
        assert "FLAG" in call_kwargs["Subject"]
        assert call_kwargs["TopicArn"] == mgr._sns_topic_arn

    def test_send_notification_called_for_block(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation(action="BLOCK")
        mgr.send_notification(ev, {"account_id": "acc1", "amount_usd": 20000})

        mock_sns.publish.assert_called_once()
        body = json.loads(mock_sns.publish.call_args[1]["Message"])
        assert body["action"] == "BLOCK"

    def test_send_notification_sns_error_does_not_raise(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mock_sns.publish.side_effect = _client_error()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation(action="FLAG")
        mgr.send_notification(ev, {"account_id": "a", "amount_usd": 100})
        # should not raise

    def test_process_fraud_calls_save_and_notify(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation(action="FLAG", is_fraud=True)
        txn = {"account_id": "acc1", "amount_usd": 500}

        mgr.process(ev, txn)

        mgr._table.put_item.assert_called_once()
        mock_sns.publish.assert_called_once()

    def test_process_no_fraud_skips_save_and_notify(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation(action="ALLOW", is_fraud=False)
        mgr.process(ev, {"account_id": "acc1", "amount_usd": 10})

        mgr._table.put_item.assert_not_called()
        mock_sns.publish.assert_not_called()

    def test_save_alert_signals_serialised_correctly(self):
        mock_dynamo = MagicMock()
        mock_sns = MagicMock()
        mgr = self._make_manager(mock_dynamo, mock_sns)

        ev = _make_evaluation(action="BLOCK", score=0.9)
        mgr.save_alert(ev, {"account_id": "a", "amount_usd": 20000})

        item = mgr._table.put_item.call_args[1]["Item"]
        signals = json.loads(item["signals"])
        assert isinstance(signals, list)
