from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict

from utils.helpers import ensure_dir, setup_logger


class WarehouseLoader:
    def __init__(self, config: Dict[str, Any], paths: Dict[str, Path]):
        self.config = config
        self.paths = paths
        self.logger = setup_logger(self.__class__.__name__)
        self.wh_config = config["warehouse"]

    def _table_file_map(self) -> Dict[str, Path]:
        return {
            "DIM_STOCKS": self.paths["warehouse"] / "Dim_Stocks.csv",
            "DIM_DATE": self.paths["warehouse"] / "Dim_Date.csv",
            "FACT_MARKET_TRADES": self.paths["warehouse"] / "Fact_Market_Trades.csv",
        }

    def _build_ddl_sql(self) -> str:
        database = self.wh_config["database"]
        schema = self.wh_config["schema"]
        warehouse_name = self.wh_config["warehouse_name"]
        role = self.wh_config["role"]

        return f"""
CREATE DATABASE IF NOT EXISTS {database};
USE DATABASE {database};
USE ROLE {role};
USE WAREHOUSE {warehouse_name};
CREATE SCHEMA IF NOT EXISTS {schema};
USE SCHEMA {schema};

CREATE OR REPLACE TABLE DIM_STOCKS (
    TICKERID INTEGER,
    SYMBOL STRING,
    COMPANYNAME STRING
);

CREATE OR REPLACE TABLE DIM_DATE (
    DATEID INTEGER,
    FULLDATE DATE,
    YEAR INTEGER,
    MONTH INTEGER,
    DAY INTEGER
);

CREATE OR REPLACE TABLE FACT_MARKET_TRADES (
    TRADEID INTEGER,
    TICKERID INTEGER,
    DATEID INTEGER,
    OPENPRICE FLOAT,
    CLOSEPRICE FLOAT,
    VOLUME FLOAT,
    MA_7 FLOAT,
    RSI FLOAT
);
""".strip()

    def _build_copy_sql(self) -> str:
        lines = [
            "CREATE OR REPLACE FILE FORMAT MARKET_CSV_FORMAT TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"';",
            "CREATE OR REPLACE STAGE MARKET_STAGE FILE_FORMAT = MARKET_CSV_FORMAT;",
        ]

        for table, file_path in self._table_file_map().items():
            absolute = file_path.resolve().as_posix()
            file_name = file_path.name
            lines.append(f"PUT 'file://{absolute}' @MARKET_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")
            lines.append(
                f"COPY INTO {table} FROM @MARKET_STAGE/{file_name}.gz FILE_FORMAT=(FORMAT_NAME=MARKET_CSV_FORMAT) ON_ERROR='CONTINUE';"
            )

        return "\n".join(lines)

    def _write_sql_artifacts(self) -> Dict[str, Path]:
        project_root = self.paths["warehouse"].parents[1]
        sql_dir = ensure_dir(project_root / "loading" / "sql")
        ddl_path = sql_dir / "ddl.sql"
        copy_path = sql_dir / "copy_into.sql"

        ddl_path.write_text(self._build_ddl_sql(), encoding="utf-8")
        copy_path.write_text(self._build_copy_sql(), encoding="utf-8")

        return {"ddl": ddl_path, "copy": copy_path}

    def _simulate_load(self) -> Dict[str, Any]:
        table_files = self._table_file_map()
        missing = [name for name, file in table_files.items() if not file.exists()]
        if missing:
            raise FileNotFoundError(
                f"Missing warehouse table files for load simulation: {', '.join(missing)}"
            )

        sql_files = self._write_sql_artifacts()
        self.logger.info("Simulation mode enabled. SQL files generated at: %s", sql_files)

        return {
            "mode": "simulate",
            "tables_ready": list(table_files.keys()),
            "sql_files": {k: str(v) for k, v in sql_files.items()},
        }

    def _execute_snowflake(self) -> Dict[str, Any]:
        try:
            import snowflake.connector
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "snowflake-connector-python is required for Snowflake mode."
            ) from exc

        user = os.getenv(self.wh_config["user_env_var"])
        password = os.getenv(self.wh_config["password_env_var"])
        account = os.getenv(self.wh_config["account_env_var"])

        if not all([user, password, account]):
            raise EnvironmentError(
                "Snowflake credentials missing. Set env vars configured in config.yaml."
            )

        table_files = self._table_file_map()
        sql_files = self._write_sql_artifacts()

        conn = snowflake.connector.connect(
            user=user,
            password=password,
            account=account,
            warehouse=self.wh_config["warehouse_name"],
            database=self.wh_config["database"],
            schema=self.wh_config["schema"],
            role=self.wh_config["role"],
        )

        try:
            with conn.cursor() as cur:
                for statement in self._build_ddl_sql().split(";"):
                    stmt = statement.strip()
                    if stmt:
                        cur.execute(stmt)

                cur.execute(
                    "CREATE OR REPLACE FILE FORMAT MARKET_CSV_FORMAT TYPE='CSV' SKIP_HEADER=1 FIELD_OPTIONALLY_ENCLOSED_BY='\"'"
                )
                cur.execute("CREATE OR REPLACE STAGE MARKET_STAGE FILE_FORMAT = MARKET_CSV_FORMAT")

                for table, file_path in table_files.items():
                    absolute = file_path.resolve().as_posix()
                    file_name = file_path.name
                    cur.execute(
                        f"PUT 'file://{absolute}' @MARKET_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                    )
                    cur.execute(
                        f"COPY INTO {table} FROM @MARKET_STAGE/{file_name}.gz FILE_FORMAT=(FORMAT_NAME=MARKET_CSV_FORMAT) ON_ERROR='CONTINUE'"
                    )
        finally:
            conn.close()

        return {
            "mode": "snowflake",
            "tables_loaded": list(table_files.keys()),
            "sql_files": {k: str(v) for k, v in sql_files.items()},
        }

    def run(self) -> Dict[str, Any]:
        mode = self.wh_config.get("mode", "simulate").lower()
        if mode == "snowflake":
            return self._execute_snowflake()
        return self._simulate_load()
