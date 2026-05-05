import yfinance as yf
import pandas as pd
import requests
import time
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

SYMBOLS = ["AAPL", "MSFT", "BTC-USD"]
HDFS_HOST = "http://localhost:9870"
HDFS_USER = "root"
HDFS_BASE_PATH = "/data/realtime"
HDFS_DATANODE_HOST = "0c37aa3b0859"
HDFS_DATANODE_REPLACE = "localhost"
INTERVAL_SECONDS = 15 * 60


def fetch_realtime(symbol: str) -> dict:
    ticker = yf.Ticker(symbol)
    info = ticker.fast_info
    return {
        "symbol": symbol,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "price": round(float(info.last_price), 4),
        "open": round(float(info.open), 4),
        "high": round(float(info.day_high), 4),
        "low": round(float(info.day_low), 4),
        "volume": int(info.last_volume),
    }


def upload_to_hdfs(df: pd.DataFrame, symbol: str):
    now = datetime.now(timezone.utc)
    hdfs_path = (
        f"{HDFS_BASE_PATH}/symbol={symbol}/"
        f"date={now.strftime('%Y-%m-%d')}/"
        f"{now.strftime('%H%M%S')}.csv"
    )

    csv_data = df.to_csv(index=False).encode("utf-8")

    create_url = (
        f"{HDFS_HOST}/webhdfs/v1{hdfs_path}"
        f"?op=CREATE&user.name={HDFS_USER}"
        f"&overwrite=true&createparent=true"
    )
    r1 = requests.put(create_url, allow_redirects=False)
    if r1.status_code != 307:
        raise Exception(f"HDFS CREATE failed: {r1.status_code} {r1.text}")

    write_url = r1.headers["Location"].replace(
        HDFS_DATANODE_HOST, HDFS_DATANODE_REPLACE
    )

    r2 = requests.put(
        write_url,
        data=csv_data,
        headers={"Content-Type": "application/octet-stream"}
    )
    if r2.status_code != 201:
        raise Exception(f"HDFS WRITE failed: {r2.status_code} {r2.text}")

    logging.info(f"Uploaded {symbol} to {hdfs_path}")


def run():
    logging.info("Starting real-time ingestion every 15 minutes...")
    while True:
        for symbol in SYMBOLS:
            try:
                data = fetch_realtime(symbol)
                df = pd.DataFrame([data])
                upload_to_hdfs(df, symbol)
                logging.info(
                    f"{symbol} | price={data['price']} | volume={data['volume']}"
                )
            except Exception as e:
                logging.error(f"{symbol} failed: {e}")

        logging.info("Sleeping 15 minutes...")
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    run()