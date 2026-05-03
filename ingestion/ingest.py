from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd
import yfinance as yf

from utils.helpers import ensure_dir, read_json, setup_logger, write_json


class MarketDataIngestor:
    def __init__(self, config: Dict[str, Any], paths: Dict[str, Path]):
        self.config = config
        self.paths = paths
        self.logger = setup_logger(self.__class__.__name__)
        self.webhdfs_base = "http://namenode:9870/webhdfs/v1"
        self.state = read_json(
            paths["state_file"],
            default={"last_ingested": {}},
        )

    def _get_start_date(self, ticker: str) -> str:
        last_ingested = self.state.get("last_ingested", {}).get(ticker)
        if last_ingested:
            start = datetime.fromisoformat(last_ingested).date() + timedelta(days=1)
            return start.isoformat()
        return self.config["market"]["default_start_date"]

    def _download_ticker_data(self, ticker: str) -> pd.DataFrame:
        start_date = self._get_start_date(ticker)
        end_date = (datetime.now(timezone.utc).date() + timedelta(days=1)).isoformat()
        interval = self.config["market"]["interval"]

        if pd.Timestamp(start_date).date() >= pd.Timestamp(end_date).date():
            self.logger.info(
                "No eligible ingestion window for %s (start=%s, end=%s).",
                ticker,
                start_date,
                end_date,
            )
            return pd.DataFrame()

        self.logger.info(
            "Downloading data for %s from %s to %s (%s)",
            ticker,
            start_date,
            end_date,
            interval,
        )

        data = yf.download(
            tickers=ticker,
            start=start_date,
            end=end_date,
            interval=interval,
            auto_adjust=False,
            progress=False,
        )

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = [col[0] if isinstance(col, tuple) else col for col in data.columns]

        if data.empty:
            self.logger.info("No new rows found for %s", ticker)
            return data

        data = data.reset_index()
        data["symbol"] = ticker
        data["ingested_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        if "Date" in data.columns:
            data["Date"] = pd.to_datetime(data["Date"]).dt.date

        return data

    def _write_partition(self, df_partition: pd.DataFrame, ticker: str, trade_date: str) -> int:
        out_dir = ensure_dir(self.paths["raw"] / f"symbol={ticker}" / f"date={trade_date}")
        out_file = out_dir / "market_data.csv"

        if out_file.exists():
            existing = pd.read_csv(out_file)
            merged = pd.concat([existing, df_partition], ignore_index=True)
            merged = merged.drop_duplicates(subset=["Date", "symbol"], keep="last")
        else:
            merged = df_partition

        merged.to_csv(out_file, index=False)
        self._upload_to_hdfs(out_file, ticker, trade_date)
        return len(df_partition)

    def _upload_to_hdfs(self, local_file: Path, ticker: str, trade_date: str) -> None:
        hdfs_dir = f"/data/raw/symbol={ticker}/date={trade_date}"
        hdfs_file = f"{hdfs_dir}/market_data.csv"
        container_path = Path("/opt/project") / local_file.relative_to(self.paths["raw"].parents[1])

        self._webhdfs_mkdirs(hdfs_dir)
        self._webhdfs_put(container_path, hdfs_file)

    def _webhdfs_mkdirs(self, hdfs_dir: str) -> None:
        params = {"op": "MKDIRS"}
        url = f"{self.webhdfs_base}{hdfs_dir}?{urlencode(params)}"
        req = Request(url, method="PUT")
        with urlopen(req) as _resp:
            return

    def _webhdfs_put(self, local_path: Path, hdfs_path: str) -> None:
        params = {"op": "CREATE", "overwrite": "true"}
        init_url = f"{self.webhdfs_base}{hdfs_path}?{urlencode(params)}"
        init_req = Request(init_url, method="PUT")

        try:
            init_resp = urlopen(init_req)
            redirect_url = init_resp.getheader("Location")
        except HTTPError as exc:
            if exc.code != 307:
                raise
            redirect_url = exc.headers.get("Location")

        if not redirect_url:
            raise RuntimeError("WebHDFS did not provide a redirect URL for upload.")

        with local_path.open("rb") as handle:
            data = handle.read()

        upload_req = Request(redirect_url, method="PUT", data=data)
        with urlopen(upload_req) as _resp:
            return

    def _save_partitioned(self, data: pd.DataFrame, ticker: str) -> int:
        total_rows = 0
        data = data.copy()
        data["Date"] = pd.to_datetime(data["Date"]).dt.date.astype(str)

        for trade_date, partition in data.groupby("Date"):
            total_rows += self._write_partition(partition, ticker, trade_date)

        return total_rows

    def _update_state(self, ticker: str, data: pd.DataFrame) -> None:
        if data.empty:
            return

        max_date = pd.to_datetime(data["Date"]).max().date().isoformat()
        self.state.setdefault("last_ingested", {})[ticker] = max_date
        write_json(self.paths["state_file"], self.state)

    def run(self) -> Dict[str, int]:
        results: Dict[str, int] = {}

        for ticker in self.config["market"]["tickers"]:
            try:
                data = self._download_ticker_data(ticker)
                if data.empty:
                    results[ticker] = 0
                    continue

                rows_written = self._save_partitioned(data, ticker)
                self._update_state(ticker, data)
                results[ticker] = rows_written
                self.logger.info("Wrote %s raw rows for %s", rows_written, ticker)
            except Exception as exc:  # noqa: BLE001
                self.logger.exception("Ingestion failed for %s: %s", ticker, exc)
                results[ticker] = -1

        return results
