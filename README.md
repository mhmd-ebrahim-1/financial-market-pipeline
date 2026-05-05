# Financial Market Big Data Pipeline

![Python](https://img.shields.io/badge/Python-3.8-3776AB?logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?logo=apachespark&logoColor=white)
![Hadoop](https://img.shields.io/badge/Apache%20Hadoop-HDFS-66CCFF?logo=apachehadoop&logoColor=black)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-financial_market_pipeline-017CEE?logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-MARKET_DWH.GOLD-29B5E8?logo=snowflake&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-10%20containers-2496ED?logo=docker&logoColor=white)

University graduation project — Faculty of AI, Kafr El-Sheikh University
Student: Mohamed Ebrahim | Cohort: 2023-2027
GitHub: https://github.com/mhmd-ebrahim-1/financial-market-pipeline

## Data Source

- Provider: Yahoo Finance (via yfinance Python library)
- Symbols: AAPL (Apple), MSFT (Microsoft), BTC-USD (Bitcoin)
- Date Range: 2023-01-01 to 2026-04-26
- Total Records: 2,872 daily OHLCV records
- Direct links:
  - https://finance.yahoo.com/quote/AAPL/history/
  - https://finance.yahoo.com/quote/MSFT/history/
  - https://finance.yahoo.com/quote/BTC-USD/history/

## Pipeline Architecture

```
Yahoo Finance
    |
    v
Python ETL (ingest.py)
    |
    v
HDFS Raw (2,872 partitioned CSV files)
    |
    v
Spark/PySpark (spark_transform.py)
    |
    v
Star Schema (Fact + 2 Dims)
    |
    v
Snowflake DWH (MARKET_DWH.GOLD)
    |
    v
Power BI

Airflow DAG runs daily at 10 PM (Sun-Thu)
All services run in Docker (10 containers)
```

## Tech Stack

- Python 3.8, yfinance, pandas, snowflake-connector-python
- Apache Hadoop (HDFS) — namenode, datanode
- Apache Spark (PySpark) — MA7, RSI indicators
- Apache Airflow — DAG: financial_market_pipeline
- Snowflake (Free Trial) — account: to38000.eu-central-2.aws
- Docker Desktop (Windows) — 10 containers

## Star Schema

- FACT_MARKET_TRADES: 2,872 rows (TRADEID, TICKERID, DATEID, OPENPRICE, CLOSEPRICE, VOLUME, MA_7, RSI)
- DIM_STOCKS: 3 rows (AAPL, MSFT, BTC-USD)
- DIM_DATE: 1,212 rows

```
DIM_STOCKS -----------\
                        \
                         > FACT_MARKET_TRADES <---- DIM_DATE
                        /
DIM_DATE   ------------/
```

## Snowflake Schema (MARKET_DWH.GOLD)

```sql
CREATE DATABASE IF NOT EXISTS MARKET_DWH;
CREATE SCHEMA IF NOT EXISTS MARKET_DWH.GOLD;

CREATE TABLE IF NOT EXISTS MARKET_DWH.GOLD.DIM_STOCKS (
    TICKERID NUMBER,
    SYMBOL STRING,
    COMPANYNAME STRING
);

CREATE TABLE IF NOT EXISTS MARKET_DWH.GOLD.DIM_DATE (
    DATEID NUMBER,
    FULLDATE DATE,
    YEAR NUMBER,
    MONTH NUMBER,
    DAY NUMBER
);

CREATE TABLE IF NOT EXISTS MARKET_DWH.GOLD.FACT_MARKET_TRADES (
    TRADEID NUMBER,
    TICKERID NUMBER,
    DATEID NUMBER,
    OPENPRICE FLOAT,
    CLOSEPRICE FLOAT,
    VOLUME NUMBER,
    MA_7 FLOAT,
    RSI FLOAT
);
```

## How to Run

1. Start Docker:
   ```powershell
   cd docker
   docker-compose up -d
   ```
2. Connect Airflow to network:
   ```powershell
   docker network connect docker-hadoop_hadoop_network airflow-webserver
   docker network connect docker-hadoop_hadoop_network airflow-scheduler
   ```
3. Copy Spark script:
   ```powershell
   docker cp processing/spark_transform.py spark-master:/opt/spark_transform.py
   ```
4. Re-upload HDFS data if needed:
   ```powershell
   python upload_to_hdfs.py
   ```
5. Trigger DAG at http://localhost:8081 (admin/admin)

## Access UIs

- HDFS NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Airflow: http://localhost:8081

## Project Structure

```
big-data-pipeline/
├── airflow/
│   └── dag.py
├── ingestion/
│   └── ingest.py
├── processing/
│   └── spark_transform.py
├── validation/
│   └── quality_checks.py
├── loading/
│   ├── load.py
│   └── load_to_snowflake.py
├── utils/
│   └── helpers.py
├── docker/
│   ├── docker-compose.yml
│   └── hadoop.env
├── docs/
│   └── images/
├── data/
│   ├── raw/
│   ├── curated/
│   └── warehouse/
├── config.yaml
├── requirements.txt
├── start.bat
└── README.md
```
