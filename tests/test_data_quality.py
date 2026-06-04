"""Unit tests for the DataQualityValidator (no CloudWatch required)."""

import json
import os
import sys
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


MINIMAL_EXPECTATIONS = {
    "expectation_suite_name": "test_suite",
    "expectations": [
        {
            "expectation_type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "transaction_id"},
        },
        {
            "expectation_type": "expect_column_values_to_be_in_set",
            "kwargs": {
                "column": "status",
                "value_set": ["PENDING", "SETTLED", "FLAGGED"],
            },
        },
        {
            "expectation_type": "expect_column_values_to_be_between",
            "kwargs": {"column": "amount", "min_value": 0.01, "max_value": 1_000_000},
        },
        {
            "expectation_type": "expect_column_values_to_be_unique",
            "kwargs": {"column": "transaction_id"},
        },
    ],
}


@pytest.fixture
def expectations_file(tmp_path):
    path = tmp_path / "expectations.json"
    path.write_text(json.dumps(MINIMAL_EXPECTATIONS))
    return str(path)


@pytest.fixture
def valid_df():
    return pd.DataFrame(
        [
            {"transaction_id": "t1", "status": "SETTLED", "amount": 100.0},
            {"transaction_id": "t2", "status": "PENDING", "amount": 50.0},
            {"transaction_id": "t3", "status": "FLAGGED", "amount": 9999.0},
        ]
    )


@pytest.fixture
def invalid_df():
    return pd.DataFrame(
        [
            {"transaction_id": None, "status": "SETTLED", "amount": 100.0},
            {"transaction_id": "t2", "status": "UNKNOWN", "amount": 50.0},
            {
                "transaction_id": "t2",
                "status": "SETTLED",
                "amount": -5.0,
            },  # duplicate + negative
        ]
    )


class TestDataQualityValidator:
    @patch("boto3.client")
    @patch("great_expectations.get_context")
    def test_valid_data_all_pass(
        self, mock_context, mock_boto, expectations_file, valid_df
    ):
        from data_quality.validators import DataQualityValidator

        # Mock the Great Expectations context and validator
        mock_validator = MagicMock()
        mock_context_instance = MagicMock()
        mock_context.return_value = mock_context_instance
        mock_context_instance.sources.pandas_default.read_dataframe.return_value = (
            mock_validator
        )

        # Mock expectation results
        mock_result1 = MagicMock()
        mock_result1.success = True
        mock_result1.result = {}

        mock_validator.expect_column_values_to_not_be_null.return_value = mock_result1
        mock_validator.expect_column_values_to_be_in_set.return_value = mock_result1
        mock_validator.expect_column_values_to_be_between.return_value = mock_result1
        mock_validator.expect_column_values_to_be_unique.return_value = mock_result1

        v = DataQualityValidator(expectations_file, emit_metrics=False)
        result = v.validate(valid_df, "test_dataset")
        assert result["overall_success"] is True
        assert result["summary"]["failed"] == 0

    @patch("boto3.client")
    @patch("great_expectations.get_context")
    def test_invalid_data_some_fail(
        self, mock_context, mock_boto, expectations_file, invalid_df
    ):
        from data_quality.validators import DataQualityValidator

        # Mock the Great Expectations context and validator
        mock_validator = MagicMock()
        mock_context_instance = MagicMock()
        mock_context.return_value = mock_context_instance
        mock_context_instance.sources.pandas_default.read_dataframe.return_value = (
            mock_validator
        )

        # Mock mixed results
        mock_fail = MagicMock()
        mock_fail.success = False
        mock_fail.result = {}

        mock_validator.expect_column_values_to_not_be_null.return_value = mock_fail
        mock_validator.expect_column_values_to_be_in_set.return_value = mock_fail
        mock_validator.expect_column_values_to_be_between.return_value = mock_fail
        mock_validator.expect_column_values_to_be_unique.return_value = mock_fail

        v = DataQualityValidator(expectations_file, emit_metrics=False)
        result = v.validate(invalid_df, "test_dataset")
        assert result["overall_success"] is False
        assert result["summary"]["failed"] > 0

    @patch("boto3.client")
    @patch("great_expectations.get_context")
    def test_result_structure(
        self, mock_context, mock_boto, expectations_file, valid_df
    ):
        from data_quality.validators import DataQualityValidator

        # Mock the Great Expectations context and validator
        mock_validator = MagicMock()
        mock_context_instance = MagicMock()
        mock_context.return_value = mock_context_instance
        mock_context_instance.sources.pandas_default.read_dataframe.return_value = (
            mock_validator
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.result = {}

        mock_validator.expect_column_values_to_not_be_null.return_value = mock_result
        mock_validator.expect_column_values_to_be_in_set.return_value = mock_result
        mock_validator.expect_column_values_to_be_between.return_value = mock_result
        mock_validator.expect_column_values_to_be_unique.return_value = mock_result

        v = DataQualityValidator(expectations_file, emit_metrics=False)
        result = v.validate(valid_df, "test_dataset")
        assert "dataset" in result
        assert "summary" in result
        assert "checks" in result
        assert "pass_rate" in result["summary"]

    @patch("boto3.client")
    @patch("great_expectations.get_context")
    def test_pass_rate_calculation(
        self, mock_context, mock_boto, expectations_file, valid_df
    ):
        from data_quality.validators import DataQualityValidator

        # Mock the Great Expectations context and validator
        mock_validator = MagicMock()
        mock_context_instance = MagicMock()
        mock_context.return_value = mock_context_instance
        mock_context_instance.sources.pandas_default.read_dataframe.return_value = (
            mock_validator
        )

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.result = {}

        mock_validator.expect_column_values_to_not_be_null.return_value = mock_result
        mock_validator.expect_column_values_to_be_in_set.return_value = mock_result
        mock_validator.expect_column_values_to_be_between.return_value = mock_result
        mock_validator.expect_column_values_to_be_unique.return_value = mock_result

        v = DataQualityValidator(expectations_file, emit_metrics=False)
        result = v.validate(valid_df, "test_dataset")
        total = result["summary"]["total"]
        passed = result["summary"]["passed"]
        assert result["summary"]["pass_rate"] == pytest.approx(
            passed / total * 100, rel=1e-4
        )

    @patch("boto3.client")
    @patch("great_expectations.get_context")
    def test_missing_column_counted_as_failure(
        self, mock_context, mock_boto, expectations_file
    ):
        from data_quality.validators import DataQualityValidator

        df = pd.DataFrame([{"transaction_id": "t1"}])  # missing 'status' and 'amount'

        # Mock the Great Expectations context and validator
        mock_validator = MagicMock()
        mock_context_instance = MagicMock()
        mock_context.return_value = mock_context_instance
        mock_context_instance.sources.pandas_default.read_dataframe.return_value = (
            mock_validator
        )

        mock_fail = MagicMock()
        mock_fail.success = False
        mock_fail.result = {}

        mock_validator.expect_column_values_to_not_be_null.return_value = mock_fail
        mock_validator.expect_column_values_to_be_in_set.return_value = mock_fail
        mock_validator.expect_column_values_to_be_between.return_value = mock_fail
        mock_validator.expect_column_values_to_be_unique.return_value = mock_fail

        v = DataQualityValidator(expectations_file, emit_metrics=False)
        result = v.validate(df, "test_dataset")
        assert result["summary"]["failed"] > 0
