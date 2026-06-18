"""
ingestion/silver/dq_validator.py
Rule-based data quality engine for the Silver layer.
"""
import json
import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, isnull

logger = logging.getLogger(__name__)

class DQValidator:

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def validate(self, df: DataFrame, dq_rules_json: str) -> dict:
        if not dq_rules_json:
            return {"passed": True, "checks": [], "note": "No DQ rules configured"}
        rules = json.loads(dq_rules_json)
        checks = []
        if "null_cols" in rules:
            checks += self._check_nulls(df, rules["null_cols"])
        if "min_rows" in rules:
            checks.append(self._check_row_count(df, rules["min_rows"]))
        if "value_ranges" in rules:
            checks += self._check_ranges(df, rules["value_ranges"])
        all_passed = all(c["passed"] for c in checks)
        return {"passed": all_passed, "total_checks": len(checks), "checks": checks}

    def _check_nulls(self, df, columns):
        checks = []
        for c in columns:
            null_count = df.filter(isnull(col(c))).count()
            checks.append({
                "check": f"null_check:{c}",
                "passed": null_count == 0,
                "detail": f"{null_count} nulls in '{c}'"
            })
        return checks

    def _check_row_count(self, df, min_rows):
        row_count = df.count()
        return {
            "check": "min_row_count",
            "passed": row_count >= min_rows,
            "detail": f"{row_count} rows found, minimum: {min_rows}"
        }

    def _check_ranges(self, df, ranges):
        checks = []
        for col_name, bounds in ranges.items():
            violations = df.filter(
                (col(col_name) < bounds["min"]) | (col(col_name) > bounds["max"])
            ).count()
            checks.append({
                "check": f"range_check:{col_name}",
                "passed": violations == 0,
                "detail": f"{violations} values outside range in '{col_name}'"
            })
        return checks
