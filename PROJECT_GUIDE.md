# دليل المشروع الكامل | Complete Project Guide

## 1) Overview | نبذة سريعة
مشروع تخرج لكلية الذكاء الاصطناعي، جامعة كفر الشيخ. خط معالجة بيانات سوق المال
من المصدر حتى التحليل داخل Snowflake وPower BI.

**Project:** Financial Market Big Data Pipeline
**Student:** Mohamed Ebrahim | **Cohort:** 2023-2027
**GitHub:** https://github.com/mhmd-ebrahim-1/financial-market-pipeline

**Main Flow:**
- Yahoo Finance -> Python ETL -> HDFS Raw
- Spark Transform (MA7, RSI) -> Star Schema + Curated
- Validation (quality checks)
- Load to Snowflake (MARKET_DWH.GOLD)
- Visualization (Power BI)

**Orchestration:**
- Airflow DAG: `financial_market_pipeline`
- Schedule: daily at 10 PM (Sun-Thu)
- All services in Docker (10 containers)

---

## 2) Data Source | مصدر البيانات
- Provider: Yahoo Finance (via yfinance)
- Symbols: AAPL, MSFT, BTC-USD
- Date Range: 2023-01-01 to 2026-04-26
- Total Records: 2,872 daily OHLCV
- Links:
  - https://finance.yahoo.com/quote/AAPL/history/
  - https://finance.yahoo.com/quote/MSFT/history/
  - https://finance.yahoo.com/quote/BTC-USD/history/

---

## 3) Project Structure | هيكل المشروع
```
config.yaml
requirements.txt
start.bat
airflow/
  dag.py
ingestion/
  ingest.py
  upload_to_hdfs.py
  realtime_ingest.py
processing/
  spark_transform.py
validation/
  quality_checks.py
loading/
  load.py
  load_to_snowflake.py
  sql/
    ddl.sql
    copy_into.sql
utils/
  helpers.py
state/
  state.json
data/
  raw/
  curated/
  warehouse/
  powerbi/
docs/
  architecture_diagram.svg
```

**أهم الملفات:**
- `airflow/dag.py`: تعريف الـ DAG وخط التشغيل.
- `ingestion/ingest.py`: جلب البيانات من Yahoo Finance.
- `ingestion/realtime_ingest.py`: Real-time polling بين الـ DAG runs.
- `ingestion/upload_to_hdfs.py`: رفع الـ raw data إلى HDFS.
- `processing/spark_transform.py`: تحويل البيانات إلى Fact/Dim + Curated.
- `validation/quality_checks.py`: فحوصات الجودة.
- `loading/load_to_snowflake.py`: تحميل فعلي إلى Snowflake.
- `config.yaml`: إعدادات عامة.
- `state/state.json`: آخر تاريخ تم ingestion له.

---

## 4) Prerequisites | المتطلبات قبل التشغيل
- Docker Desktop
- Python 3.8+
- Snowflake account (اختياري للتحويل الحقيقي)
- Power BI (اختياري)

---

## 5) تشغيل المشروع خطوة بخطوة | Full Run Steps

### Step A: تشغيل الخدمات
```powershell
cd "D:\Downloads\big data"
cd docker
docker-compose up -d
```

### Step B: ربط Airflow على شبكة Hadoop
```powershell
docker network connect docker-hadoop_hadoop_network airflow-webserver
docker network connect docker-hadoop_hadoop_network airflow-scheduler
```

### Step C: نسخ سكربت Spark داخل الـ container
```powershell
docker cp processing/spark_transform.py spark-master:/opt/spark_transform.py
```

### Step D: رفع raw data إلى HDFS (لو محتاج)
```powershell
python ingestion\upload_to_hdfs.py
```

### Step E: تشغيل الـ DAG من Airflow
1) افتح Airflow UI: http://localhost:8081
2) فعّل DAG `financial_market_pipeline`
3) اضغط Run

**Airflow credentials** (لو اتعملت):
- Username: admin
- Password: admin

---

## 6) Access UIs | الروابط
- HDFS NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Airflow: http://localhost:8081

---

## 7) Ingestion | شرح الـ Ingestion
**المصدر:** Yahoo Finance عبر `yfinance`.

**المخرجات:**
- Local raw data:
  `data/raw/symbol=.../date=.../market_data.csv`
- HDFS raw data:
  `/data/raw/symbol=.../date=.../market_data.csv`

**رفع إلى HDFS يدويًا:**
```powershell
python ingestion\upload_to_hdfs.py
```

**Real-Time Ingestion (Optional):**
```powershell
python ingestion\realtime_ingest.py
```

**فكرة الـ state.json:**
- يمنع تنزيل نفس البيانات مرتين
- يحفظ آخر تاريخ تم تنزيله لكل symbol

**Reset state (لو عايز تحميل كامل):**
```json
{ "last_ingested": {} }
```

---

## 8) Spark Transform | شرح التحويل
الملف: `processing/spark_transform.py`

**بيعمل:**
- قراءة raw data من local partitions
- تنظيف وتطبيع الأعمدة
- حساب MA7 و RSI
- إنشاء Dim_Stocks, Dim_Date, Fact_Market_Trades
- إخراج CSVs

**المخرجات:**
- `data/warehouse/Fact_Market_Trades.csv`
- `data/warehouse/Dim_Stocks.csv`
- `data/warehouse/Dim_Date.csv`
- `data/curated/market_data_curated.csv`

**تشغيل يدوي:**
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master local[*] /opt/project/processing/spark_transform.py
```

---

## 9) Validation | شرح التحقق من الجودة
الملف: `validation/quality_checks.py`

**Checks:**
- Null ratio
- Primary key uniqueness
- Foreign key integrity
- Domain checks (RSI range, volume >= 0)
- Minimum row count

**Output:**
`data/curated/validation_report.json`

**مثال Summary متوقع:**
```
"status": "passed",
"error_count": 0
```

---

## 10) Loading | تحميل البيانات

### A) Simulate (افتراضي)
- يولد SQL في:
  `loading/sql/ddl.sql`
  `loading/sql/copy_into.sql`

### B) Snowflake (لو عايز تفعيل حقيقي)
**Target:** `MARKET_DWH.GOLD`

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

في `config.yaml` (مثال):
```
warehouse:
  mode: snowflake
  database: MARKET_DWH
  schema: GOLD
  warehouse_name: COMPUTE_WH
  role: SYSADMIN
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
  account_env_var: SNOWFLAKE_ACCOUNT
```

ثم ضبط الـ env vars:
```powershell
$env:SNOWFLAKE_USER="<user>"
$env:SNOWFLAKE_PASSWORD="<password>"
$env:SNOWFLAKE_ACCOUNT="to38000.eu-central-2.aws"
```

---

## 11) Tech Stack | التقنيات
- Python 3.8, yfinance, pandas, snowflake-connector-python
- Apache Hadoop (HDFS) — namenode, datanode
- Apache Spark (PySpark) — MA7, RSI
- Apache Airflow — DAG: financial_market_pipeline
- Snowflake (Free Trial) — account: to38000.eu-central-2.aws
- Docker Desktop (Windows) — 10 containers

---

## 12) Testing | الاختبار النهائي (عند التسليم)

### Test 1: HDFS Raw موجود
```powershell
docker exec -it namenode hdfs dfs -ls /data/raw
```

### Test 2: Warehouse ملفات محلية
- `data/warehouse/Fact_Market_Trades.csv`
- `data/warehouse/Dim_Stocks.csv`
- `data/warehouse/Dim_Date.csv`

### Test 3: تقرير الجودة
افتح:
`data/curated/validation_report.json`
وتأكد:
- status = passed
- error_count = 0

### Test 4: Snowflake (اختياري)
```sql
SELECT COUNT(*) FROM MARKET_DWH.GOLD.DIM_DATE;
SELECT COUNT(*) FROM MARKET_DWH.GOLD.DIM_STOCKS;
SELECT COUNT(*) FROM MARKET_DWH.GOLD.FACT_MARKET_TRADES;
```
**Expected:**
- DIM_STOCKS = 3
- DIM_DATE ~ 1212
- FACT_MARKET_TRADES ~ 2872

---

## 13) Common Errors | أشهر المشاكل وحلولها

### مشكلة: Invalid login في Airflow
**حل:**
- امسح cookies
- اعمل user جديد:
```powershell
docker exec -it airflow-webserver airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com
```

### مشكلة: spark_transform فشل
**حل:**
- شغل التحويل يدويًا وشوف السبب:
```powershell
docker exec -it spark-master /opt/spark/bin/spark-submit --master local[*] /opt/project/processing/spark_transform.py
```

### مشكلة: HDFS `/data` مش موجود
**حل:**
- شغل upload_to_hdfs
```powershell
python ingestion\upload_to_hdfs.py
```

### مشكلة: yfinance فشل
**حل:**
- Reset state.json
- جرب تاريخ قديم في config.yaml

---

## 14) FAQ | أسئلة متوقعة من الدكتور

**Q: ليه بتخزن Local + HDFS؟**
A: Local للتصحيح والتطوير السريع، وHDFS لتخزين Big Data وتكامل Spark/Hadoop.

**Q: ليه عملت Star Schema؟**
A: لتسهيل التحليل ورفع الأداء في الـ BI.

**Q: ليه Airflow؟**
A: Orchestration + scheduling + monitoring + retry.

**Q: ازاي بتضمن الجودة؟**
A: Data Quality Validator + تقرير JSON.

**Q: لو البيانات وقفت؟**
A: يتم استخدام `state.json` لتتبع آخر تاريخ، وممكن reset عند الحاجة.

---

## 15) Final Checklist | تشيك ليست التسليم
- [ ] Airflow DAG كله Success
- [ ] HDFS `/data/raw` موجود
- [ ] ملفات warehouse موجودة
- [ ] validation_report status passed
- [ ] Snowflake counts صحيحة (لو مفعّل)

---

## 16) Commands Quick Reference | أوامر سريعة
```powershell
# تشغيل الخدمات
cd docker
docker-compose up -d

# ربط Airflow بالشبكة
docker network connect docker-hadoop_hadoop_network airflow-webserver
docker network connect docker-hadoop_hadoop_network airflow-scheduler

# رفع raw لـ HDFS
python ingestion\upload_to_hdfs.py

# تشغيل Spark Transform يدوي
docker exec -it spark-master /opt/spark/bin/spark-submit --master local[*] /opt/project/processing/spark_transform.py

# تشغيل real-time ingestion
python ingestion\realtime_ingest.py

# فحص HDFS
docker exec -it namenode hdfs dfs -ls /data/raw
```

---

لو محتاج نسخة مختصرة للتسليم أو عرض، قولّي.