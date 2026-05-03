# دليل المشروع الكامل | Complete Project Guide

## 1) Overview | نبذة سريعة
هذا المشروع عبارة عن Financial Market Big Data Pipeline كاملة من ingestion الى transform الى validation الى loading ثم التحليل في Snowflake/Power BI.

**Main Flow:**
- Ingestion (yfinance) -> Raw data (local + HDFS)
- Spark Transform (PySpark) -> Star Schema + curated data
- Validation (quality checks)
- Load to warehouse (simulate / Snowflake)
- Visualization (Power BI)

**Technologies:**
- Python, Pandas, PySpark
- Hadoop HDFS
- Apache Spark
- Apache Airflow
- Snowflake

---

## 2) Project Structure | هيكل المشروع
```
config.yaml
requirements.txt
start.bat
airflow/
  dag.py
ingestion/
  ingest.py
  upload_to_hdfs.py
processing/
  spark_transform.py
validation/
  quality_checks.py
loading/
  load.py
  load_to_snowflake.py
utils/
  helpers.py
state/
  state.json
data/
  raw/
  curated/
  warehouse/
  powerbi/
```

**أهم الملفات:**
- `airflow/dag.py`: تعريف الـ DAG وخط التشغيل.
- `ingestion/ingest.py`: جلب البيانات من Yahoo Finance.
- `ingestion/upload_to_hdfs.py`: رفع الـ raw data الى HDFS.
- `processing/spark_transform.py`: تحويل البيانات إلى Fact/Dim + Curated.
- `validation/quality_checks.py`: فحوصات الجودة.
- `loading/load.py`: تحميل (simulate أو Snowflake).
- `config.yaml`: إعدادات عامة للمشروع.
- `state/state.json`: آخر تاريخ تم ingestion له.

---

## 3) Prerequisites | المتطلبات قبل التشغيل
- Docker Desktop
- Python 3.10+
- Snowflake account (لو هتشغل التحميل الحقيقي)
- Power BI (اختياري)

---

## 4) تشغيل المشروع خطوة بخطوة | Full Run Steps

### Step A: تشغيل الخدمات
```powershell
cd "D:\Downloads\big data"
.\start.bat
```

### Step B: فتح الواجهات
- HDFS NameNode: http://localhost:9870
- Spark Master: http://localhost:8080
- Airflow: http://localhost:8081

**Airflow credentials** (لو اتعملت):
- Username: admin
- Password: admin

### Step C: تشغيل الـ DAG من Airflow
1) افتح Airflow UI
2) فعّل DAG `financial_market_pipeline`
3) اضغط Run

---

## 5) Ingestion | شرح الـ Ingestion
**المصدر:** Yahoo Finance API باستخدام `yfinance`.

**المخرجات:**
- Local raw data في:
  `data/raw/symbol=.../date=.../market_data.csv`
- HDFS raw data في:
  `/data/raw/symbol=.../date=.../market_data.csv`

**لو عايز ترفع لـ HDFS يدويًا:**
```powershell
python ingestion\upload_to_hdfs.py
```

**فكرة الـ state.json:**
- يمنع تنزيل نفس البيانات مرتين
- يحفظ آخر تاريخ تم تنزيله لكل symbol

**Reset state (لو عايز تحميل كامل):**
```json
{ "last_ingested": {} }
```

---

## 6) Spark Transform | شرح التحويل
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

## 7) Validation | شرح التحقق من الجودة
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

## 8) Loading | تحميل البيانات

### A) Simulate (افتراضي)
- يولد SQL في:
  `loading/sql/ddl.sql`
  `loading/sql/copy_into.sql`

### B) Snowflake (لو عايز تفعيل حقيقي)
في `config.yaml`:
```
warehouse:
  mode: snowflake
  database: FINANCE_DB
  schema: MARKET_ANALYTICS
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
$env:SNOWFLAKE_ACCOUNT="<account>"
```

---

## 9) Testing | الاختبار النهائي (عند التسليم)

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

## 10) Common Errors | أشهر المشاكل وحلولها

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

## 11) FAQ | أسئلة متوقعة من الدكتور

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

## 12) Final Checklist | تشيك ليست التسليم
- [ ] Airflow DAG كله Success
- [ ] HDFS `/data/raw` موجود
- [ ] ملفات warehouse موجودة
- [ ] validation_report status passed
- [ ] Snowflake counts صحيحة (لو مفعّل)

---

## 13) Commands Quick Reference | أوامر سريعة
```powershell
# تشغيل الخدمات
.\start.bat

# رفع raw لـ HDFS
python ingestion\upload_to_hdfs.py

# تشغيل Spark Transform يدوي
Docker exec -it spark-master /opt/spark/bin/spark-submit --master local[*] /opt/project/processing/spark_transform.py

# فحص HDFS
docker exec -it namenode hdfs dfs -ls /data/raw
```

---

لو محتاج نسخة مختصرة للتسليم أو عرض، قولّي.