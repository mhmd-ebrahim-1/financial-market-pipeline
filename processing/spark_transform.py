"""
spark_transform.py
==================
PySpark transform reading local raw partitions, building star schema,
and writing CSVs for downstream validation/loading.

Usage:
    docker exec -it spark-master /opt/spark/bin/spark-submit \
        --master local[*] \
        /opt/project/processing/spark_transform.py
"""

import os
import re
import shutil
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType

# ─────────────────────────────────────────
# 1. إنشاء SparkSession
# ─────────────────────────────────────────
spark = SparkSession.builder \
    .appName("MarketDataTransformer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

PROJECT_ROOT = Path("/opt/project")
RAW_PATH = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse"
CURATED_PATH = PROJECT_ROOT / "data" / "curated"

def _write_single_csv(df, target_path: Path) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = target_path.parent / f"_{target_path.stem}_tmp"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)

    (
        df.coalesce(1)
        .write.mode("overwrite")
        .option("header", True)
        .csv(tmp_dir.as_posix())
    )

    part_files = list(tmp_dir.glob("part-*.csv"))
    if not part_files:
        raise FileNotFoundError(f"No CSV part files written to {tmp_dir}")

    if target_path.exists():
        target_path.unlink()
    shutil.move(part_files[0].as_posix(), target_path.as_posix())
    shutil.rmtree(tmp_dir)

# ─────────────────────────────────────────
# 2. قراءة البيانات الخام من الملفات المحلية
# ─────────────────────────────────────────
print(">>> Reading raw data from local raw partitions...")

df = (
    spark.read
    .schema(
        StructType(
            [
                StructField("Date", StringType(), True),
                StructField("Adj Close", StringType(), True),
                StructField("Close", StringType(), True),
                StructField("High", StringType(), True),
                StructField("Low", StringType(), True),
                StructField("Open", StringType(), True),
                StructField("Volume", StringType(), True),
                StructField("symbol", StringType(), True),
                StructField("ingested_at_utc", StringType(), True),
            ]
        )
    )
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("recursiveFileLookup", "true")
    .csv(RAW_PATH.as_posix(), header=True, inferSchema=True)
)

def _norm_name(name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return cleaned

normalized = {_norm_name(col): col for col in df.columns}
required = {
    "date": None,
    "open": None,
    "close": None,
    "volume": None,
    "symbol": None,
    "ingested_at_utc": None,
}

for key in required.keys():
    if key in normalized:
        required[key] = normalized[key]

missing = [k for k, v in required.items() if v is None]
if missing:
    raise ValueError(f"Missing required columns in raw data: {', '.join(missing)}")

df = df.select(
    F.col(required["date"]).alias("date"),
    F.col(required["open"]).alias("openprice"),
    F.col(required["close"]).alias("closeprice"),
    F.col(required["volume"]).alias("volume"),
    F.col(required["symbol"]).alias("symbol"),
    F.col(required["ingested_at_utc"]).alias("ingested_at_utc"),
)

df = df.withColumn("date", F.to_date("date"))
df = df.withColumn("openprice", F.expr("try_cast(openprice as double)"))
df = df.withColumn("closeprice", F.expr("try_cast(closeprice as double)"))
df = df.withColumn("volume", F.expr("try_cast(volume as double)"))
df = df.dropna(subset=["date", "symbol", "openprice", "closeprice", "volume"])

# ─────────────────────────────────────────
# 3. Cleaning
# ─────────────────────────────────────────
print(">>> Cleaning data...")

# إزالة الـ nulls في الأعمدة المهمة
df = df.dropna(subset=["symbol", "date", "openprice", "closeprice", "volume"])

# ─────────────────────────────────────────
# 4. Feature Engineering (MA_7 و RSI)
# ─────────────────────────────────────────
print(">>> Feature engineering: MA_7 and RSI...")

# Window مرتبة بـ symbol و date
window_ma = Window.partitionBy("symbol").orderBy("date").rowsBetween(-6, 0)

# Moving Average 7 أيام
df = df.withColumn("ma_7", F.avg("closeprice").over(window_ma))

# RSI - نحسبه بـ Spark SQL Window functions
# الخطوة 1: حساب الـ price change
window_rsi = Window.partitionBy("symbol").orderBy("date")
df = df.withColumn("price_change", F.col("closeprice") - F.lag("closeprice", 1).over(window_rsi))

# الخطوة 2: gain و loss
df = df.withColumn("gain", F.when(F.col("price_change") > 0, F.col("price_change")).otherwise(0))
df = df.withColumn("loss", F.when(F.col("price_change") < 0, -F.col("price_change")).otherwise(0))

# الخطوة 3: average gain/loss على 14 يوم
window_rsi_14 = Window.partitionBy("symbol").orderBy("date").rowsBetween(-13, 0)
df = df.withColumn("avg_gain", F.avg("gain").over(window_rsi_14))
df = df.withColumn("avg_loss", F.avg("loss").over(window_rsi_14))

# الخطوة 4: RSI formula
df = df.withColumn(
    "rsi",
    F.when(F.col("avg_loss") == 0, 100)
     .otherwise(100 - (100 / (1 + F.col("avg_gain") / F.col("avg_loss"))))
)

# إزالة الأعمدة المؤقتة
df = df.drop("price_change", "gain", "loss", "avg_gain", "avg_loss")

# ─────────────────────────────────────────
# 5. بناء Star Schema
# ─────────────────────────────────────────
print(">>> Building Star Schema...")

# --- Dim_Stocks ---
dim_stocks = (
    df.select("symbol")
    .distinct()
    .orderBy("symbol")
    .withColumn("tickerid", F.row_number().over(Window.orderBy("symbol")))
    .select("tickerid", "symbol", F.col("symbol").alias("companyname"))
)

# --- Dim_Date ---
dim_date = (
    df.select("date")
    .distinct()
    .withColumn("dateid", F.date_format("date", "yyyyMMdd").cast("int"))
    .withColumn("fulldate", F.col("date"))
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .select("dateid", "fulldate", "year", "month", "day")
    .orderBy("dateid")
)

# --- Fact_Market_Trades ---
fact_base = (
    df.join(dim_stocks, on="symbol", how="left")
    .join(dim_date, on=F.date_format("date", "yyyyMMdd").cast("int") == dim_date["dateid"], how="left")
)

fact = (
    fact_base
    .withColumn("tradeid", F.row_number().over(Window.orderBy("tickerid", "date")))
    .select(
        "tradeid",
        "tickerid",
        "dateid",
        "openprice",
        "closeprice",
        "volume",
        "ma_7",
        "rsi",
    )
)

# ─────────────────────────────────────────
# 6. الكتابة كـ CSV
# ─────────────────────────────────────────
print(">>> Writing warehouse CSVs...")

_write_single_csv(fact, WAREHOUSE_PATH / "Fact_Market_Trades.csv")
_write_single_csv(dim_stocks, WAREHOUSE_PATH / "Dim_Stocks.csv")
_write_single_csv(dim_date, WAREHOUSE_PATH / "Dim_Date.csv")

print(">>> Writing curated CSV...")
curated = df.select("symbol", "date", "openprice", "closeprice", "volume")
_write_single_csv(curated, CURATED_PATH / "market_data_curated.csv")

# ─────────────────────────────────────────
# 7. التحقق
# ─────────────────────────────────────────
print("\n>>> ✅ Transform complete!")
print(f"    fact_market_trades : {fact.count()} rows")
print(f"    dim_stocks         : {dim_stocks.count()} rows")
print(f"    dim_date           : {dim_date.count()} rows")

spark.stop()
