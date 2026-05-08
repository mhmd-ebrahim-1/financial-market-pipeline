# Pipeline Overview

This project implements a complete Big Data pipeline for financial market analytics. Data is ingested from Yahoo Finance, stored in HDFS, transformed with PySpark (MA7 and RSI), validated, and loaded into a Snowflake star schema for BI reporting.

## Flow
- Ingest daily OHLCV data with Python
- Store raw partitions in HDFS
- Transform with PySpark into Fact and Dim tables
- Run data quality checks
- Load into Snowflake and consume in Power BI
