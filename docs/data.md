# Data Guide

## Raw Layer (HDFS)
- Path: /data/raw/symbol=SYMBOL/date=YYYY-MM-DD/market_data.csv
- Content: OHLCV, symbol, ingested_at_utc

## Local Raw
- Path: data/raw/symbol=SYMBOL/date=YYYY-MM-DD/market_data.csv

## Warehouse (Star Schema)
- data/warehouse/Fact_Market_Trades.csv
- data/warehouse/Dim_Stocks.csv
- data/warehouse/Dim_Date.csv

## Curated
- data/curated/market_data_curated.csv (if enabled)
- data/curated/validation_report.json

## Power BI Export
- data/powerbi/dim_date.csv
- data/powerbi/dim_stocks.csv
- data/powerbi/fact_market_trades.csv

## State
- state/state.json tracks last ingested date per symbol
