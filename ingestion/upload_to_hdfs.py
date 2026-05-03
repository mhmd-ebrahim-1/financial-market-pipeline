"""
upload_to_hdfs.py
=================
بيمشي على الـ raw data المحلية (partitioned بـ symbol/date)
وبيرفعها على HDFS مع الحفاظ على نفس الـ structure.

Usage:
    python upload_to_hdfs.py
"""

from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
PROJECT_ROOT = Path(r"D:\Downloads\big data")
LOCAL_RAW = PROJECT_ROOT / "data" / "raw"
HDFS_RAW = "/data/raw"
WEBHDFS_BASE = "http://localhost:9870/webhdfs/v1"
WEBHDFS_USER = "hdfs"

# ─────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────
def webhdfs_mkdirs(path: str) -> None:
    params = {"op": "MKDIRS", "user.name": WEBHDFS_USER}
    url = f"{WEBHDFS_BASE}{path}?{urlencode(params)}"
    req = Request(url, method="PUT")
    with urlopen(req) as _resp:
        return


def webhdfs_put(local: Path, hdfs_path: str) -> None:
    params = {"op": "CREATE", "overwrite": "true", "user.name": WEBHDFS_USER}
    init_url = f"{WEBHDFS_BASE}{hdfs_path}?{urlencode(params)}"
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

    parsed = urlparse(redirect_url)
    if parsed.hostname and parsed.hostname not in {"localhost", "127.0.0.1"}:
        # Datanode hostname from containers is not resolvable on host.
        netloc = f"localhost:{parsed.port or 9864}"
        redirect_url = urlunparse(parsed._replace(netloc=netloc))

    with local.open("rb") as handle:
        data = handle.read()

    upload_req = Request(redirect_url, method="PUT", data=data)
    with urlopen(upload_req) as _resp:
        return


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
def main():
    print("=" * 50)
    print("  Uploading Raw Data to HDFS")
    print("=" * 50)

    # عمل الـ root directory
    webhdfs_mkdirs(HDFS_RAW)

    total_files = 0
    total_symbols = 0

    # المشي على كل symbol
    for symbol_dir in sorted(LOCAL_RAW.iterdir()):
        if not symbol_dir.is_dir() or not symbol_dir.name.startswith("symbol="):
            continue

        symbol = symbol_dir.name  # e.g. symbol=AAPL
        hdfs_symbol_path = f"{HDFS_RAW}/{symbol}"
        webhdfs_mkdirs(hdfs_symbol_path)
        total_symbols += 1

        print(f"\n📂 {symbol}")

        # المشي على كل date
        for date_dir in sorted(symbol_dir.iterdir()):
            if not date_dir.is_dir() or not date_dir.name.startswith("date="):
                continue

            date = date_dir.name  # e.g. date=2023-01-03
            hdfs_date_path = f"{hdfs_symbol_path}/{date}"
            webhdfs_mkdirs(hdfs_date_path)

            # رفع كل CSV في الـ date directory
            for csv_file in date_dir.glob("*.csv"):
                hdfs_file_path = f"{hdfs_date_path}/{csv_file.name}"
                webhdfs_put(csv_file, hdfs_file_path)
                total_files += 1
                print(f"  ✅ {date}/{csv_file.name}")

    print("\n" + "=" * 50)
    print(f"  Done! Uploaded {total_files} files across {total_symbols} symbols")
    print("=" * 50)

    # تحقق نهائي
    print("\n📊 HDFS Structure:")
    try:
        params = {"op": "LISTSTATUS", "recursive": "true"}
        url = f"{WEBHDFS_BASE}{HDFS_RAW}?{urlencode(params)}"
        with urlopen(url) as resp:
            print(resp.read().decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        print(f"  ⚠️  Unable to list HDFS content via WebHDFS: {exc}")


if __name__ == "__main__":
    main()
