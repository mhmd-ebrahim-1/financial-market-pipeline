@echo off
REM Start the Hadoop + Spark + Airflow stack
docker compose -f docker\docker-compose.yml up -d
