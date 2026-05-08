# Airflow DAG

The Airflow DAG orchestrates the end-to-end pipeline with four tasks in order:

1) ingest_market_data
2) spark_transform
3) validate_data_quality
4) load_to_warehouse

## Schedule
- Cron: 0 22 * * 0-4
- Runs daily at 10 PM, Sunday to Thursday

## How to Run
- Open http://localhost:8081
- Enable and trigger the DAG named financial_market_pipeline
