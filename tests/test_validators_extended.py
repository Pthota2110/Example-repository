"""Extended tests for data_quality/validators.py to cover uncovered branches."""

import json
import os
import sys

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_quality.validators import DataQualityValidator, _check  # noqa: E402


class TestCheckDispatch:
    """Tests for the _check() pure-pandas dispatcher — covers all branches."""

    def _df(self, **cols):
        return pd.DataFrame([cols])

    # expect_table_row_count_to_be_between
    def test_row_count_within_bounds(self):
        df = pd.DataFrame([{}, {}, {}])
        r = _check("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": 10}, df)
        assert r["success"]
        assert r["result"]["observed_value"] == 3

    def test_row_count_below_min(self):
        df = pd.DataFrame([{}])
        r = _check("expect_table_row_count_to_be_between", {"min_value": 5, "max_value": 10}, df)
        assert not r["success"]

    def test_row_count_above_max(self):
        df = pd.DataFrame([{} for _ in range(20)])
        r = _check("expect_table_row_count_to_be_between", {"min_value": 1, "max_value": 10}, df)
        assert not r["success"]

    # expect_column_to_exist
    def test_column_exists(self):
        df = self._df(amount=100)
        r = _check("expect_column_to_exist", {"column": "amount"}, df)
        assert r["success"]

    def test_column_does_not_exist(self):
        df = self._df(amount=100)
        r = _check("expect_column_to_exist", {"column": "missing_col"}, df)
        assert not r["success"]

    # expect_column_values_to_not_be_null
    def test_null_check_passes_when_no_nulls(self):
        df = pd.DataFrame({"val": [1, 2, 3]})
        r = _check("expect_column_values_to_not_be_null", {"column": "val"}, df)
        assert r["success"]
        assert r["result"]["null_percent"] == 0.0

    def test_null_check_fails_when_all_null(self):
        df = pd.DataFrame({"val": [None, None]})
        r = _check("expect_column_values_to_not_be_null", {"column": "val"}, df)
        assert not r["success"]

    def test_null_check_passes_with_mostly_threshold(self):
        df = pd.DataFrame({"val": [1, 2, None, 4, 5]})
        r = _check("expect_column_values_to_not_be_null", {"column": "val", "mostly": 0.7}, df)
        assert r["success"]  # 80% non-null >= 70% threshold

    def test_null_check_missing_column(self):
        df = self._df(other=1)
        r = _check("expect_column_values_to_not_be_null", {"column": "missing"}, df)
        assert not r["success"]

    # expect_column_values_to_be_unique
    def test_unique_passes(self):
        df = pd.DataFrame({"id": ["a", "b", "c"]})
        r = _check("expect_column_values_to_be_unique", {"column": "id"}, df)
        assert r["success"]
        assert r["result"]["duplicate_count"] == 0

    def test_unique_fails_with_duplicates(self):
        df = pd.DataFrame({"id": ["a", "a", "b"]})
        r = _check("expect_column_values_to_be_unique", {"column": "id"}, df)
        assert not r["success"]
        assert r["result"]["duplicate_count"] == 1

    def test_unique_missing_column(self):
        df = self._df(other=1)
        r = _check("expect_column_values_to_be_unique", {"column": "missing"}, df)
        assert not r["success"]

    # expect_column_values_to_be_in_set
    def test_in_set_passes(self):
        df = pd.DataFrame({"status": ["SETTLED", "PENDING", "FLAGGED"]})
        r = _check(
            "expect_column_values_to_be_in_set",
            {"column": "status", "value_set": ["SETTLED", "PENDING", "FLAGGED", "REVERSED"]},
            df,
        )
        assert r["success"]

    def test_in_set_fails_with_invalid_values(self):
        df = pd.DataFrame({"status": ["SETTLED", "UNKNOWN"]})
        r = _check(
            "expect_column_values_to_be_in_set",
            {"column": "status", "value_set": ["SETTLED", "PENDING"]},
            df,
        )
        assert not r["success"]
        assert "UNKNOWN" in r["result"]["unexpected_values"]

    def test_in_set_missing_column(self):
        df = self._df(other=1)
        r = _check("expect_column_values_to_be_in_set", {"column": "missing", "value_set": ["A"]}, df)
        assert not r["success"]

    # expect_column_values_to_be_between
    def test_between_passes(self):
        df = pd.DataFrame({"amount": [10.0, 50.0, 100.0]})
        r = _check("expect_column_values_to_be_between", {"column": "amount", "min_value": 0, "max_value": 200}, df)
        assert r["success"]

    def test_between_fails_with_out_of_range(self):
        df = pd.DataFrame({"amount": [10.0, -5.0, 100.0]})
        r = _check("expect_column_values_to_be_between", {"column": "amount", "min_value": 0, "max_value": 200}, df)
        assert not r["success"]

    def test_between_mostly_threshold(self):
        df = pd.DataFrame({"amount": [10.0, 20.0, 30.0, -999.0]})
        r = _check(
            "expect_column_values_to_be_between",
            {"column": "amount", "min_value": 0, "max_value": 100, "mostly": 0.7},
            df,
        )
        assert r["success"]  # 75% in range >= 70%

    def test_between_missing_column(self):
        df = self._df(other=1)
        r = _check("expect_column_values_to_be_between", {"column": "missing", "min_value": 0, "max_value": 10}, df)
        assert not r["success"]

    # expect_column_values_to_match_regex
    def test_regex_passes(self):
        df = pd.DataFrame({"currency": ["USD", "EUR", "GBP"]})
        r = _check("expect_column_values_to_match_regex", {"column": "currency", "regex": "^[A-Z]{3}$"}, df)
        assert r["success"]

    def test_regex_fails(self):
        df = pd.DataFrame({"currency": ["USD", "usd", "123"]})
        r = _check("expect_column_values_to_match_regex", {"column": "currency", "regex": "^[A-Z]{3}$"}, df)
        assert not r["success"]

    def test_regex_missing_column(self):
        df = self._df(other=1)
        r = _check("expect_column_values_to_match_regex", {"column": "missing", "regex": ".*"}, df)
        assert not r["success"]

    # expect_column_proportion_of_unique_values_to_be_between
    def test_proportion_unique_passes(self):
        df = pd.DataFrame({"account_id": [f"acc-{i}" for i in range(100)]})
        r = _check(
            "expect_column_proportion_of_unique_values_to_be_between",
            {"column": "account_id", "min_value": 0.5, "max_value": 1.0},
            df,
        )
        assert r["success"]

    def test_proportion_unique_fails(self):
        df = pd.DataFrame({"account_id": ["same"] * 100})
        r = _check(
            "expect_column_proportion_of_unique_values_to_be_between",
            {"column": "account_id", "min_value": 0.5, "max_value": 1.0},
            df,
        )
        assert not r["success"]

    def test_proportion_unique_missing_column(self):
        df = self._df(other=1)
        r = _check(
            "expect_column_proportion_of_unique_values_to_be_between",
            {"column": "missing", "min_value": 0, "max_value": 1},
            df,
        )
        assert not r["success"]

    # unknown expectation type
    def test_unknown_expectation_type_returns_success(self):
        df = self._df(val=1)
        r = _check("expect_something_unknown", {}, df)
        assert r["success"]
        assert r["result"].get("skipped")


class TestValidatorRunDate:
    def test_custom_run_date_stored_in_result(self, tmp_path):
        expectations = {"expectations": [{"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}}]}
        path = tmp_path / "exp.json"
        path.write_text(json.dumps(expectations))

        v = DataQualityValidator(str(path), emit_metrics=False)
        df = pd.DataFrame([{"id": 1}])
        result = v.validate(df, "ds", run_date="2026-01-01")

        assert result["run_date"] == "2026-01-01"

    def test_run_date_defaults_to_today(self, tmp_path):
        import datetime

        expectations = {"expectations": []}
        path = tmp_path / "exp.json"
        path.write_text(json.dumps(expectations))

        v = DataQualityValidator(str(path), emit_metrics=False)
        result = v.validate(pd.DataFrame([{}]), "ds")

        assert result["run_date"] == datetime.date.today().isoformat()


class TestValidatorExceptionHandling:
    def test_exception_in_check_counted_as_failure(self, tmp_path):
        expectations = {
            "expectations": [
                {
                    "expectation_type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "nonexistent"},
                }
            ]
        }
        path = tmp_path / "exp.json"
        path.write_text(json.dumps(expectations))

        v = DataQualityValidator(str(path), emit_metrics=False)
        result = v.validate(pd.DataFrame([{"other": 1}]), "ds")

        assert result["summary"]["failed"] == 1
        assert not result["overall_success"]

    def test_overall_success_true_when_all_pass(self, tmp_path):
        expectations = {
            "expectations": [
                {"expectation_type": "expect_column_to_exist", "kwargs": {"column": "id"}},
                {"expectation_type": "expect_column_values_to_not_be_null", "kwargs": {"column": "id"}},
            ]
        }
        path = tmp_path / "exp.json"
        path.write_text(json.dumps(expectations))

        v = DataQualityValidator(str(path), emit_metrics=False)
        result = v.validate(pd.DataFrame([{"id": "abc"}]), "ds")

        assert result["overall_success"]
        assert result["summary"]["passed"] == 2
        assert result["summary"]["failed"] == 0
