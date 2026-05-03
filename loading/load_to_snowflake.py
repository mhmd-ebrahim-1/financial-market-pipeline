import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd

conn = snowflake.connector.connect(
    account="to38000.eu-central-2.aws",
    user="mhmd1",
    password="zgj1knc@mum.TRB5yzu",
    database="MARKET_DWH",
    schema="GOLD",
    warehouse="COMPUTE_WH"
)
print("Connected to Snowflake!")

DATA_PATH = r"D:\Downloads\big data\data\powerbi"

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
