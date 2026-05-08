import os

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "MARKET_DWH")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "GOLD")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

missing_envs = [
    name
    for name, value in {
        "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
        "SNOWFLAKE_USER": SNOWFLAKE_USER,
        "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    }.items()
    if not value
]

if missing_envs:
    raise EnvironmentError(
        "Missing required environment variables: " + ", ".join(missing_envs)
    )

conn = snowflake.connector.connect(
    account=SNOWFLAKE_ACCOUNT,
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    database=SNOWFLAKE_DATABASE,
    schema=SNOWFLAKE_SCHEMA,
    warehouse=SNOWFLAKE_WAREHOUSE,
)
print("Connected to Snowflake!")

DATA_PATH = os.getenv("POWERBI_DATA_PATH", r"D:\Downloads\big data\data\powerbi")

# 1. DIM_STOCKS
df = pd.read_csv(f"{DATA_PATH}\\dim_stocks.csv")
df.columns = [c.upper() for c in df.columns]
conn.cursor().execute("TRUNCATE TABLE DIM_STOCKS")
write_pandas(conn, df, "DIM_STOCKS")
print(f"DIM_STOCKS: {len(df)} rows loaded")

# 2. DIM_DATE
df = pd.read_csv(f"{DATA_PATH}\\dim_date.csv")
df.columns = [c.upper() for c in df.columns]
conn.cursor().execute("TRUNCATE TABLE DIM_DATE")
write_pandas(conn, df, "DIM_DATE")
print(f"DIM_DATE: {len(df)} rows loaded")

# 3. FACT_MARKET_TRADES
df = pd.read_csv(f"{DATA_PATH}\\fact_market_trades.csv")
df.columns = [c.upper() for c in df.columns]
conn.cursor().execute("TRUNCATE TABLE FACT_MARKET_TRADES")
write_pandas(conn, df, "FACT_MARKET_TRADES")
print(f"FACT_MARKET_TRADES: {len(df)} rows loaded")

conn.close()
print("\nAll data loaded to Snowflake successfully!")
