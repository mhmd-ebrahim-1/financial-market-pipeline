from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("MarketTransform").getOrCreate()

# =========================
# 1) READ FROM HDFS
# =========================
df = spark.read \
    .option("header", "true") \
    .option("basePath", "hdfs://namenode:9000/data/raw") \
    .csv("hdfs://namenode:9000/data/raw/*")

print("RAW COUNT:", df.count())

# =========================
# 2) CLEAN HEADERS
# =========================
for col_name in df.columns:
    df = df.withColumnRenamed(col_name, col_name.strip())

# =========================
# 3) REMOVE BAD ROWS
# =========================
df = df.withColumn("Date", F.trim(F.col("Date")))

df = df.filter(
    (F.col("Date").isNotNull()) &
    (F.col("Date") != "") &
    (F.col("Date") != "Date")
)

# =========================
# 4) SAFE CAST
# =========================
df = df.withColumn("Date", F.expr("try_cast(Date as date)"))

numeric_cols = ["Adj Close", "Close", "High", "Low", "Open", "Volume"]

for c in numeric_cols:
    df = df.withColumn(c, F.expr(f"try_cast(`{c}` as double)"))

df = df.filter(F.col("Date").isNotNull())

print("AFTER CLEAN:", df.count())
df.show(5)

# =========================
# 5) ADD SYMBOL
# =========================
df = df.withColumn(
    "symbol",
    F.regexp_extract(F.input_file_name(), r"symbol=([^/]+)", 1)
)

# =========================
# 6) FINAL SELECT
# =========================
df_final = df.select(
    "symbol",
    "Date",
    F.col("Adj Close").alias("adj_close"),
    F.col("Close").alias("close"),
    F.col("High").alias("high"),
    F.col("Low").alias("low"),
    F.col("Open").alias("open"),
    F.col("Volume").alias("volume")
)

df_final = df_final.withColumn("ingested_at_utc", F.current_timestamp())

# =========================
# 7) WRITE TO HDFS (FIX 🔥)
# =========================
OUTPUT_PATH = "hdfs://namenode:9000/data/curated/market_data_curated"

df_final.write \
    .mode("overwrite") \
    .option("header", "true") \
    .partitionBy("symbol") \
    .csv(OUTPUT_PATH)

print("WRITE DONE ✅")