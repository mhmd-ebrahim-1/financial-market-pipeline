from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from utils.helpers import ensure_dir, setup_logger


class DataQualityValidator:
    def __init__(self, config: Dict[str, Any], paths: Dict[str, Path]):
        self.config = config
        self.paths = paths
        self.logger = setup_logger(self.__class__.__name__)
        self.validation_cfg = config.get("validation", {})
        report_path = self.paths.get("validation_report")
        if report_path is None:
            report_path = self.paths["curated"] / "validation_report.json"
        self.report_path = Path(report_path)

    def _load_tables(self) -> Dict[str, pd.DataFrame]:
        table_files = {
            "fact_market_trades": self.paths["warehouse"] / "Fact_Market_Trades.csv",
            "dim_stocks": self.paths["warehouse"] / "Dim_Stocks.csv",
            "dim_date": self.paths["warehouse"] / "Dim_Date.csv",
            "curated": self.paths["curated"] / "market_data_curated.csv",
        }

        missing_files = [str(path) for path in table_files.values() if not path.exists()]
        if missing_files:
            raise FileNotFoundError(
                "Validation cannot start. Missing files: " + ", ".join(missing_files)
            )

        return {name: pd.read_csv(path) for name, path in table_files.items()}

    def _null_ratio_check(self, df: pd.DataFrame, columns: List[str], table_name: str) -> List[Dict[str, Any]]:
        max_null_ratio = float(self.validation_cfg.get("max_null_ratio", 0.01))
        issues: List[Dict[str, Any]] = []

        for col in columns:
            if col not in df.columns:
                issues.append(
                    {
                        "check": "column_exists",
                        "table": table_name,
                        "column": col,
                        "status": "error",
                        "details": "Required column missing",
                    }
                )
                continue

            null_ratio = float(df[col].isna().mean())
            status = "pass" if null_ratio <= max_null_ratio else "error"
            issues.append(
                {
                    "check": "null_ratio",
                    "table": table_name,
                    "column": col,
                    "status": status,
                    "details": f"null_ratio={null_ratio:.6f}, threshold={max_null_ratio:.6f}",
                }
            )

        return issues

    def _uniqueness_check(self, df: pd.DataFrame, columns: List[str], table_name: str) -> Dict[str, Any]:
        for col in columns:
            if col not in df.columns:
                return {
                    "check": "primary_key_uniqueness",
                    "table": table_name,
                    "columns": columns,
                    "status": "error",
                    "details": f"Missing key column: {col}",
                }

        duplicates = int(df.duplicated(subset=columns).sum())
        status = "pass" if duplicates == 0 else "error"
        return {
            "check": "primary_key_uniqueness",
            "table": table_name,
            "columns": columns,
            "status": status,
            "details": f"duplicate_rows={duplicates}",
        }

    def _foreign_key_check(self, fact: pd.DataFrame, dim: pd.DataFrame, fact_key: str, dim_key: str, name: str) -> Dict[str, Any]:
        if fact_key not in fact.columns or dim_key not in dim.columns:
            return {
                "check": "foreign_key_integrity",
                "relationship": name,
                "status": "error",
                "details": f"Missing columns fact.{fact_key} or dim.{dim_key}",
            }

        invalid = int((~fact[fact_key].isin(dim[dim_key])).sum())
        status = "pass" if invalid == 0 else "error"
        return {
            "check": "foreign_key_integrity",
            "relationship": name,
            "status": status,
            "details": f"orphan_rows={invalid}",
        }

    def _domain_checks(self, fact: pd.DataFrame) -> List[Dict[str, Any]]:
        checks: List[Dict[str, Any]] = []

        if "rsi" in fact.columns:
            out_of_range = int(((fact["rsi"] < 0) | (fact["rsi"] > 100)).sum())
            checks.append(
                {
                    "check": "rsi_range",
                    "table": "fact_market_trades",
                    "status": "pass" if out_of_range == 0 else "error",
                    "details": f"out_of_range_rows={out_of_range}",
                }
            )

        if "closeprice" in fact.columns:
            non_positive = int((fact["closeprice"] <= 0).sum())
            checks.append(
                {
                    "check": "closeprice_positive",
                    "table": "fact_market_trades",
                    "status": "pass" if non_positive == 0 else "error",
                    "details": f"non_positive_rows={non_positive}",
                }
            )

        if "volume" in fact.columns:
            negative = int((fact["volume"] < 0).sum())
            checks.append(
                {
                    "check": "volume_non_negative",
                    "table": "fact_market_trades",
                    "status": "pass" if negative == 0 else "error",
                    "details": f"negative_rows={negative}",
                }
            )

        return checks

    def _curated_grain_check(self, curated: pd.DataFrame) -> Dict[str, Any]:
        expected = ["symbol", "date"]
        for col in expected:
            if col not in curated.columns:
                return {
                    "check": "curated_grain_uniqueness",
                    "table": "curated",
                    "status": "error",
                    "details": f"Missing column: {col}",
                }

        duplicates = int(curated.duplicated(subset=expected).sum())
        status = "pass" if duplicates == 0 else "error"
        return {
            "check": "curated_grain_uniqueness",
            "table": "curated",
            "status": status,
            "details": f"duplicate_symbol_date_rows={duplicates}",
        }

    def _minimum_rows_check(self, fact: pd.DataFrame) -> Dict[str, Any]:
        min_rows = int(self.validation_cfg.get("min_expected_fact_rows", 10))
        fact_rows = len(fact)
        status = "pass" if fact_rows >= min_rows else "error"
        return {
            "check": "minimum_fact_rows",
            "table": "fact_market_trades",
            "status": status,
            "details": f"fact_rows={fact_rows}, minimum_required={min_rows}",
        }

    def _build_report(self, tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        fact = tables["fact_market_trades"]
        dim_stocks = tables["dim_stocks"]
        dim_date = tables["dim_date"]
        curated = tables["curated"]

        checks: List[Dict[str, Any]] = []
        checks.extend(
            self._null_ratio_check(
                fact,
                ["tradeid", "tickerid", "dateid", "openprice", "closeprice", "volume", "ma_7", "rsi"],
                "fact_market_trades",
            )
        )
        checks.append(self._uniqueness_check(fact, ["tradeid"], "fact_market_trades"))
        checks.append(self._uniqueness_check(dim_stocks, ["tickerid"], "dim_stocks"))
        checks.append(self._uniqueness_check(dim_date, ["dateid"], "dim_date"))
        checks.append(
            self._foreign_key_check(
                fact,
                dim_stocks,
                "tickerid",
                "tickerid",
                "fact_market_trades -> dim_stocks",
            )
        )
        checks.append(
            self._foreign_key_check(
                fact,
                dim_date,
                "dateid",
                "dateid",
                "fact_market_trades -> dim_date",
            )
        )
        checks.extend(self._domain_checks(fact))
        checks.append(self._curated_grain_check(curated))
        checks.append(self._minimum_rows_check(fact))

        errors = [c for c in checks if c["status"] == "error"]
        warnings = [c for c in checks if c["status"] == "warning"]

        return {
            "summary": {
                "total_checks": len(checks),
                "error_count": len(errors),
                "warning_count": len(warnings),
                "status": "failed" if errors else "passed",
            },
            "checks": checks,
        }

    def _write_report(self, report: Dict[str, Any]) -> None:
        ensure_dir(self.report_path.parent)
        with self.report_path.open("w", encoding="utf-8") as file:
            json.dump(report, file, indent=2)

    def run(self) -> Dict[str, Any]:
        if not bool(self.validation_cfg.get("enabled", True)):
            skipped = {
                "summary": {
                    "status": "skipped",
                    "reason": "Validation disabled in config",
                    "total_checks": 0,
                    "error_count": 0,
                    "warning_count": 0,
                },
                "checks": [],
            }
            self._write_report(skipped)
            return skipped

        tables = self._load_tables()
        report = self._build_report(tables)
        self._write_report(report)

        self.logger.info("Validation summary: %s", report["summary"])

        if report["summary"]["status"] == "failed" and bool(
            self.validation_cfg.get("fail_on_error", True)
        ):
            raise ValueError(
                "Data quality checks failed. Inspect report: " + str(self.report_path)
            )

        return report
