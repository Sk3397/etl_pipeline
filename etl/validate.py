"""
validate.py - Data quality validation checks
"""
import pandas as pd
from dataclasses import dataclass, field
from typing import List
import logging

logger = logging.getLogger(__name__)

@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    message: str
    severity: str = "ERROR"

@dataclass
class ValidationReport:
    results: List[ValidationResult] = field(default_factory=list)

    @property
    def passed(self):
        return all(r.passed for r in self.results if r.severity == "ERROR")

    @property
    def errors(self):
        return [r for r in self.results if not r.passed and r.severity == "ERROR"]

    @property
    def warnings(self):
        return [r for r in self.results if not r.passed and r.severity == "WARNING"]

    def summary(self):
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        return f"Validation: {passed}/{total} passed | {len(self.errors)} errors | {len(self.warnings)} warnings"

def check_not_empty(df):
    ok = len(df) > 0
    return ValidationResult("not_empty", ok, f"{len(df):,} rows" if ok else "DataFrame is empty")

def check_required_columns(df):
    required = {"city", "country", "recorded_at", "temperature_c", "humidity_pct"}
    missing = required - set(df.columns)
    ok = len(missing) == 0
    return ValidationResult("required_columns", ok,
        "All required columns present" if ok else f"Missing: {missing}")

def check_no_null_keys(df):
    nc = df["city"].isnull().sum()
    nt = df["recorded_at"].isnull().sum()
    ok = nc == 0 and nt == 0
    return ValidationResult("no_null_keys", ok,
        "No nulls in key columns" if ok else f"Nulls — city: {nc}, recorded_at: {nt}")

def check_unique_city_time(df):
    dupes = df.duplicated(subset=["city", "recorded_at"]).sum()
    ok = dupes == 0
    return ValidationResult("unique_city_time", ok,
        "No duplicate (city, recorded_at)" if ok else f"{dupes:,} duplicates", severity="WARNING")

def check_temperature_range(df):
    out = ((df["temperature_c"] < -80) | (df["temperature_c"] > 60)).sum()
    ok = out == 0
    return ValidationResult("temperature_range", ok,
        "Temperatures in valid range" if ok else f"{out:,} out of range")

def check_humidity_range(df):
    out = ((df["humidity_pct"] < 0) | (df["humidity_pct"] > 100)).sum()
    ok = out == 0
    return ValidationResult("humidity_range", ok,
        "Humidity in 0-100%" if ok else f"{out:,} out of range")

def check_city_coverage(df, min_cities=40):
    n = df["city"].nunique()
    ok = n >= min_cities
    return ValidationResult("city_coverage", ok,
        f"{n} cities loaded" if ok else f"Only {n} cities (expected >= {min_cities})", severity="WARNING")

def check_row_count(df, min_rows=10000):
    ok = len(df) >= min_rows
    return ValidationResult("row_count", ok,
        f"{len(df):,} rows" if ok else f"Low count: {len(df):,}", severity="WARNING")

def validate(df: pd.DataFrame) -> ValidationReport:
    report = ValidationReport()
    checks = [
        check_not_empty, check_required_columns, check_no_null_keys,
        check_unique_city_time, check_temperature_range, check_humidity_range,
        check_city_coverage, check_row_count,
    ]
    for fn in checks:
        try:
            result = fn(df)
            report.results.append(result)
            icon = "✓" if result.passed else ("✗" if result.severity == "ERROR" else "⚠")
            logger.info(f"  [{icon}] {result.check_name}: {result.message}")
        except Exception as e:
            report.results.append(ValidationResult(fn.__name__, False, f"Crashed: {e}"))
    logger.info(report.summary())
    if not report.passed:
        msgs = "\n".join(f"  - {e.check_name}: {e.message}" for e in report.errors)
        raise ValueError(f"Validation FAILED:\n{msgs}")
    return report
