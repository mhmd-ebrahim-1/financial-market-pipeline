# دليل المشروع الكامل | Complete Project Guide

## 1) Overview | نبذة سريعة
مشروع  لكلية الذكاء الاصطناعي، جامعة كفر الشيخ. خط معالجة بيانات سوق المال
من المصدر حتى التحليل داخل Snowflake و Power BI.

**Project:** Financial Market Big Data Pipeline
**Student:** Mohamed Ebrahim | **Cohort:** 2023-2027
**GitHub:** https://github.com/mhmd-ebrahim-1/financial-market-pipeline

**Main Flow:**
- Yahoo Finance -> Python ETL -> HDFS Raw
- Spark Transform (MA7, RSI) -> Star Schema (Fact + Dim)
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
- Interval: 1 day
- Date Range: يبدأ من 2023-01-01 لحد اليوم
- Total Records: تقريبا 2,872 صف يومي (يتغير حسب تاريخ التشغيل)
- Links:
  - https://finance.yahoo.com/quote/AAPL/history/
  - https://finance.yahoo.com/quote/MSFT/history/
  - https://finance.yahoo.com/quote/BTC-USD/history/

---

## 3) Full Project Structure | هيكل المشروع بالكامل
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
  images/
docker/
  docker-compose.yml
  hadoop.env
```

---

## 4) Configuration | ملف الإعدادات config.yaml
الملف ده هو مصدر الحقيقة لأي تشغيل:

- `project.timezone`: المنطقة الزمنية للمشروع.
- `market.tickers`: الرموز المستهدفة (AAPL/MSFT/BTC-USD).
- `market.default_start_date`: أول تاريخ للتحميل.
- `market.interval`: الفاصل الزمني للبيانات (1d).
- `features.ma_window_days`: نافذة المتوسط المتحرك (MA7).
- `features.rsi_period_days`: نافذة RSI (14).
- `paths.*`: مسارات الـ data داخل المشروع.
- `validation.*`: إعدادات فحوصات الجودة.
- `warehouse.*`: وضع التحميل (simulate / snowflake) وإعدادات الاتصال.

---

## 5) File-by-File Explanation | شرح كل ملف كود

### airflow/dag.py
الـ DAG الأساسي ويحدد ترتيب المهام:
1) `ingest_market_data`
2) `spark_transform`
3) `validate_data_quality`
4) `load_to_warehouse`

الجدولة: `0 22 * * 0-4` (10 مساء من الأحد للخميس).

### ingestion/ingest.py
المسؤول عن الـ batch ingestion:
- يقرأ `state/state.json` لتجنب إعادة تنزيل نفس الأيام.
- ينزل بيانات Yahoo Finance عبر `yfinance`.
- يحفظ partitions محلية بنمط:
  `data/raw/symbol=.../date=.../market_data.csv`.
- يرفع نفس الملفات إلى HDFS عبر WebHDFS.

### ingestion/realtime_ingest.py
وضع real-time اختياري:
- يسحب `fast_info` من yfinance كل 15 دقيقة.
- يكتب ملفات صغيرة على HDFS في:
  `/data/realtime/symbol=.../date=.../HHMMSS.csv`.
- مفيد للديمو فقط، مش جزء من الـ DAG الأساسي.

### ingestion/upload_to_hdfs.py
سكريبت مساعد لرفع البيانات المحلية إلى HDFS:
- يمشي على `data/raw` ويرفع كل partition.
- يعالج مشكلة Hostname الخاص بالـ DataNode داخل Docker.

### processing/spark_transform.py
قلب المعالجة بالـ PySpark:
- يقرأ الـ raw partitions من `data/raw` (local داخل container).
- ينظف الأعمدة ويحاول تحويل القيم إلى double.
- يحسب MA7 و RSI باستخدام Window Functions.
- يبني Star Schema:
  - Dim_Stocks
  - Dim_Date
  - Fact_Market_Trades
- يكتب ملفات CSV في `data/warehouse`.

ملاحظة مهمة: السكريبت حاليا لا يكتب `market_data_curated.csv`.
لو عايز التحقق الكامل في خطوة الـ validation، لازم وجود ملف curated
يدويا أو تحديث السكريبت لاحقا.

### validation/quality_checks.py
مدقق جودة البيانات:
- يتحقق من null ratio.
- uniqueness للـ PKs.
- integrity للـ FKs.
- Domain checks (RSI, Volume, ClosePrice).
- minimum rows.

مهم: المدقق يتوقع وجود:
`data/curated/market_data_curated.csv`
وفي حال عدم وجوده، سيحدث فشل.

### loading/load.py
لودر رئيسي للـ warehouse وله وضعين:
- **simulate**: ينشئ SQL فقط في `loading/sql/ddl.sql`.
- **snowflake**: ينفذ DDL ويكتب البيانات عبر `write_pandas`.

يقرأ إعدادات Snowflake من `config.yaml` ومن env vars.

### loading/load_to_snowflake.py
سكريبت مستقل لتحميل ملفات Power BI إلى Snowflake:
- يستخدم مسار `data/powerbi`.
- يحتوي بيانات اتصال ثابتة (تُعدل محليا).
- مفيد عند تحضير الـ dashboard فقط.

### loading/sql/ddl.sql
تعريف جداول الـ DWH بصيغة Star Schema.

### loading/sql/copy_into.sql
سكريبت بديل للتحميل باستخدام COPY INTO بعد PUT للملفات.

### utils/helpers.py
مكتبة مساعدة:
- قراءة config.yaml
- read/write JSON
- logging
- حساب RSI (في حالة استخدام Pandas)
- بناء الـ paths بناء على config

### start.bat
أبسط أمر لتشغيل المنظومة:
`docker compose -f docker\docker-compose.yml up -d`

### requirements.txt
تعريف الحزم الرئيسية (yfinance, pandas, pyspark, airflow, snowflake connector).

---

## 6) Data Directories | شرح مجلدات البيانات

### data/raw
ملفات الـ ingestion الخام مقسمة حسب symbol/date.
كل ملف يحتوي OHLCV + timestamp.

### data/warehouse
الناتج النهائي للـ Star Schema:
- Fact_Market_Trades.csv
- Dim_Stocks.csv
- Dim_Date.csv

### data/curated
مفترض يحتوي `market_data_curated.csv` + `validation_report.json`.

### data/powerbi
نسخة مهيأة للـ Power BI (dim_date, dim_stocks, fact_market_trades).

---

## 7) تشغيل المشروع خطوة بخطوة | Full Run Steps

### Step A: تشغيل الخدمات
```powershell
cd "D:\Downloads\big data"
cd docker
docker compose up -d
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
3) Trigger DAG

**Airflow credentials** (لو اتعملت):
- Username: admin
- Password: admin

---

## 8) Access UIs | الروابط
- HDFS NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Airflow: http://localhost:8081
- YARN ResourceManager: http://localhost:8088

---

## 9) Snowflake Setup | إعداد Snowflake
**Target:** `MARKET_DWH.GOLD`

إعداد env vars:
```powershell
$env:SNOWFLAKE_USER="<user>"
$env:SNOWFLAKE_PASSWORD="<password>"
$env:SNOWFLAKE_ACCOUNT="to38000.eu-central-2.aws"
```

ثم تأكد أن `config.yaml` مضبوط على:
```
warehouse:
  mode: snowflake
  database: MARKET_DWH
  schema: GOLD
  warehouse_name: COMPUTE_WH
  role: ACCOUNTADMIN
  user_env_var: SNOWFLAKE_USER
  password_env_var: SNOWFLAKE_PASSWORD
  account_env_var: SNOWFLAKE_ACCOUNT
```

---

## 10) Validation | شرح التحقق من الجودة
الملف: `validation/quality_checks.py`

**Checks:**
- Null ratio
- Primary key uniqueness
- Foreign key integrity
- Domain checks (RSI range, volume >= 0, closeprice > 0)
- Minimum row count

**Output:**
`data/curated/validation_report.json`

---

## 11) Testing | الاختبار النهائي (عند التسليم)

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
**Expected (تقريبي):**
- DIM_STOCKS = 3
- DIM_DATE ~ 1212
- FACT_MARKET_TRADES ~ 2872

---

## 12) Common Errors | أشهر المشاكل وحلولها

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

## 13) FAQ | أسئلة متوقعة من الدكتور

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

## 14) Final Checklist | تشيك ليست التسليم
- [ ] Airflow DAG كله Success
- [ ] HDFS `/data/raw` موجود
- [ ] ملفات warehouse موجودة
- [ ] validation_report status passed
- [ ] Snowflake counts صحيحة (لو مفعّل)

---

## 15) Commands Quick Reference | أوامر سريعة
```powershell
# تشغيل الخدمات
cd docker
docker compose up -d

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