# 📈 Financial Market Big Data Pipeline

<div align="center">

![Python](https://img.shields.io/badge/Python-3.8-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Hadoop](https://img.shields.io/badge/Hadoop-HDFS-66CCFF?style=for-the-badge&logo=apachehadoop&logoColor=black)
![Airflow](https://img.shields.io/badge/Airflow-2.8.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-DWH-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-10%20containers-2496ED?style=for-the-badge&logo=docker&logoColor=white)

**A production-style end-to-end Big Data pipeline for financial market analytics**

*Faculty of Artificial Intelligence — Kafr El-Sheikh University | Graduation Project 2023–2027*

</div>

---

## 📊 Dashboard Preview

![Power BI Dashboard](docs/images/Screenshot%202026-05-08%20110429.png)

> Real-time financial dashboard showing AAPL, MSFT, and BTC-USD with price trends, RSI indicators, moving averages, and volume analysis.

---

## 🏗️ Architecture

![Pipeline Architecture](docs/architecture_diagram.svg)

```
Yahoo Finance API
      │
      ▼
Python ETL (ingest.py)          ← Daily ingestion via yfinance
      │
      ▼
HDFS Raw Layer                  ← 2,872 partitioned CSV files
/data/raw/symbol=*/date=*/
      │
      ▼
Apache Spark (PySpark)          ← MA-7, RSI, Star Schema
spark_transform.py
      │
      ▼
HDFS Warehouse                  ← Parquet + CSV
/data/warehouse/
      │
      ▼
Snowflake DWH                   ← MARKET_DWH.GOLD
FACT + DIM tables
      │
      ▼
Power BI Dashboard              ← Live visualizations

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Apache Airflow DAG              ← Orchestrates daily at 10 PM (Sun–Thu)
Docker (10 containers)          ← Full local infrastructure
```

---

## 🛠️ Tech Stack

| Layer | Technology | Details |
|-------|-----------|---------|
| Data Source | Yahoo Finance API | AAPL, MSFT, BTC-USD |
| Ingestion | Python 3.8 + yfinance | Daily OHLCV data |
| Storage | Apache Hadoop HDFS 3.2.1 | Distributed file system |
| Processing | Apache Spark 4.1.1 (PySpark) | MA-7, RSI calculation |
| Orchestration | Apache Airflow 2.8.1 | Daily DAG scheduling |
| Data Warehouse | Snowflake (Free Trial) | Cloud DWH |
| Visualization | Power BI Desktop | Interactive dashboard |
| Infrastructure | Docker Desktop | 10 containerized services |

---

## 📐 Data Model — Star Schema

```
        ┌─────────────────┐
        │   DIM_STOCKS    │
        │─────────────────│
        │ TICKERID (PK)   │
        │ SYMBOL          │
        │ COMPANYNAME     │
        └────────┬────────┘
                 │
                 │ 1:N
                 ▼
┌────────────────────────────────┐
│      FACT_MARKET_TRADES        │
│────────────────────────────────│
│ TRADEID (PK)                   │
│ TICKERID (FK) ─────────────────┤──▶ DIM_STOCKS
│ DATEID   (FK) ─────────────────┤──▶ DIM_DATE
│ OPENPRICE                      │
│ CLOSEPRICE                     │
│ VOLUME                         │
│ MA_7    ← 7-day Moving Average │
│ RSI     ← Relative Str. Index  │
└────────────────────────────────┘
                 ▲
                 │ 1:N
                 │
        ┌────────┴────────┐
        │    DIM_DATE     │
        │─────────────────│
        │ DATEID (PK)     │
        │ FULLDATE        │
        │ YEAR            │
        │ MONTH           │
        │ DAY             │
        └─────────────────┘
```

| Table | Rows | Description |
|-------|------|-------------|
| FACT_MARKET_TRADES | 2,872 | Daily trading records |
| DIM_STOCKS | 3 | AAPL, MSFT, BTC-USD |
| DIM_DATE | 1,212 | Date dimension 2023–2026 |

---

## 🐳 Docker Infrastructure

| Container | Image | Port | Role |
|-----------|-------|------|------|
| namenode | bde2020/hadoop-namenode | 9870, 9000 | HDFS master |
| datanode | bde2020/hadoop-datanode | 9864 | HDFS storage |
| resourcemanager | bde2020/hadoop-resourcemanager | 8088 | YARN manager |
| nodemanager | bde2020/hadoop-nodemanager | — | YARN executor |
| historyserver | bde2020/hadoop-historyserver | — | Job history |
| spark-master | apache/spark:latest | 8080, 7077 | Spark master |
| spark-worker | apache/spark:latest | — | Spark worker |
| airflow-postgres | postgres:13 | — | Airflow metadata |
| airflow-webserver | apache/airflow:2.8.1 | 8081 | Airflow UI |
| airflow-scheduler | apache/airflow:2.8.1 | — | DAG scheduler |

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (Windows)
- Python 3.10+
- Git

### 1. Clone the repository
```bash
git clone https://github.com/mhmd-ebrahim-1/financial-market-pipeline.git
cd financial-market-pipeline
```

### 2. Start the cluster
```powershell
cd docker
docker compose up -d
```

### 3. Connect Airflow to network
```powershell
docker network connect docker-hadoop_hadoop_network airflow-webserver
docker network connect docker-hadoop_hadoop_network airflow-scheduler
```

### 4. Copy Spark script to container
```powershell
docker cp processing/spark_transform.py spark-master:/opt/spark_transform.py
```

### 5. Upload raw data to HDFS (first time only)
```powershell
python upload_to_hdfs.py
```

### 6. Trigger the pipeline
Open **http://localhost:8081** → login (admin/admin) → Trigger DAG ▶️

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8081 | admin / admin |
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8080 | — |
| YARN ResourceManager | http://localhost:8088 | — |

---

## ⚙️ Airflow DAG

```
ingest_market_data
      │
      ▼
spark_transform          ← PySpark job via spark-submit
      │
      ▼
validate_data_quality
      │
      ▼
load_to_warehouse        ← Snowflake via write_pandas
```

**Schedule:** `0 22 * * 0-4` — Daily at 10 PM, Sunday to Thursday

---

## 📁 Project Structure

```
financial-market-pipeline/
├── airflow/
│   └── dag.py                    # Airflow DAG definition
├── ingestion/
│   ├── ingest.py                 # Yahoo Finance → HDFS
│   └── realtime_ingest.py        # Optional real-time polling
├── processing/
│   └── spark_transform.py        # PySpark: MA7, RSI, Star Schema
├── validation/
│   └── quality_checks.py         # Data quality checks
├── loading/
│   ├── load.py                   # Snowflake loader (write_pandas)
│   └── load_to_snowflake.py      # Standalone Snowflake script
├── utils/
│   └── helpers.py                # Shared utilities
├── docker/
│   ├── docker-compose.yml        # 10-container cluster setup
│   └── hadoop.env                # Hadoop configuration
├── docs/
│   ├── architecture_diagram.svg  # Architecture diagram
│   └── images/
│       └── Screenshot 2026-05-08 110429.png
├── data/
│   └── .gitkeep
├── config.yaml                   # Pipeline configuration
├── requirements.txt              # Python dependencies
├── upload_to_hdfs.py             # HDFS upload utility
├── start.bat                     # One-click startup script
└── README.md
```

---

## 🔧 Troubleshooting

**Airflow not opening after restart:**
```powershell
docker exec airflow-webserver rm -f /opt/airflow/airflow-webserver.pid
docker restart airflow-webserver
```

**HDFS data missing after restart:**
```powershell
python upload_to_hdfs.py
```

**spark_transform missing in container:**
```powershell
docker cp processing/spark_transform.py spark-master:/opt/spark_transform.py
```

---

## 👤 Author

**Mohamed Ebrahim**
Faculty of Artificial Intelligence — Kafr El-Sheikh University
Cohort 2023–2027

[![GitHub](https://img.shields.io/badge/GitHub-mhmd--ebrahim--1-181717?style=flat&logo=github)](https://github.com/mhmd-ebrahim-1)