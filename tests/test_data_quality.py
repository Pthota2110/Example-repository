"""Unit tests for the DataQualityValidator (no AWS credentials required)."""

import json
import os
import sys

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
            "kwargs": {"column": "status", "value_set": ["PENDING", "SETTLED", "FLAGGED"]},
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
            {"transaction_id": "t2", "status": "SETTLED", "amount": -5.0},  # duplicate + negative
        ]
    )


class TestDataQualityValidator:
    def _make_validator(self, expectations_file):
        from data_quality.validators import DataQualityValidator

        return DataQualityValidator(expectations_file, emit_metrics=False)

    def test_valid_data_all_pass(self, expectations_file, valid_df):
        v = self._make_validator(expectations_file)
        result = v.validate(valid_df, "test_dataset")
        assert result["overall_success"] is True
        assert result["summary"]["failed"] == 0

    def test_invalid_data_some_fail(self, expectations_file, invalid_df):
        v = self._make_validator(expectations_file)
        result = v.validate(invalid_df, "test_dataset")
        assert result["overall_success"] is False
        assert result["summary"]["failed"] > 0

    def test_result_structure(self, expectations_file, valid_df):
        v = self._make_validator(expectations_file)
        result = v.validate(valid_df, "test_dataset")
        assert "dataset" in result
        assert "summary" in result
        assert "checks" in result
        assert "pass_rate" in result["summary"]

    def test_pass_rate_calculation(self, expectations_file, valid_df):
        v = self._make_validator(expectations_file)
        result = v.validate(valid_df, "test_dataset")
        total = result["summary"]["total"]
        passed = result["summary"]["passed"]
        assert result["summary"]["pass_rate"] == pytest.approx(passed / total * 100, rel=1e-4)

    def test_missing_column_counted_as_failure(self, expectations_file):
        from data_quality.validators import DataQualityValidator

        df = pd.DataFrame([{"transaction_id": "t1"}])  # missing 'status' and 'amount'
        v = DataQualityValidator(expectations_file, emit_metrics=False)
        result = v.validate(df, "test_dataset")
        assert result["summary"]["failed"] > 0
