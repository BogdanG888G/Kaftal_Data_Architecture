```
┌──────────────────────────────────────────────────────────────┐
│                   АРХИТЕКТУРА                                │
├──────────────────────────────────────────────────────────────┤
│                                                              │
│  Airflow          ← Оркестрация                              │
│    │                                                         │
│    ├─► CSV → Parquet (S3)        ← Bronze (raw data lake)    │
│    │                                                         │
│    ├─► Spark → Iceberg           ← Silver (нормализация)     │
│    │                                                         │
│    ├─► dbt → ClickHouse          ← Gold (агрегаты для BI)    │
│    │                                                         │
│    └─► Superset                  ← Визуализация              │
│                                                              │
│  Trino (ad-hoc)  ← Для аналитиков (гибкие запросы по Raw)    │
│                                                              │
└──────────────────────────────────────────────────────────────┘


┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  BRONZE (Data Lake)                                         │
│  ├─ S3: s3://data/2024/september/okey.csv                   │
│  ├─ S3: s3://data/2024/september/perekrestok.csv            │
│  └─ Формат: CSV/JSON (как есть)                             │
│      └─► Airflow Task 1: конвертация в Parquet              │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  SILVER (Normalized)                                        │
│  ├─ Iceberg: iceberg.silver.sales                           │
│  ├─ Формат: Parquet                                         │
│  ├─ Партиции: retail_chain, year, month                     │
│  └─ Схема: единая, типизированная                           │
│      └─► Spark Task 2: нормализация                         │
│           ├─ Маппинг колонок (Вкус → flavor)                │
│           ├─ Каст типов (string → double)                   │
│           └─ Добавление метаданных (source_file)            │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  GOLD (Business Metrics)                                    │
│  ├─ ClickHouse: gold.monthly_sales                          │
│  ├─ ClickHouse: gold.top_brands                             │
│  ├─ ClickHouse: gold.chain_comparison                       │
│  └─ Формат: MergeTree (оптимизировано для агрегатов)        │
│      └─► dbt Task 3: SQL-трансформации                      │
│           ├─ Агрегации (SUM, AVG по месяцам)                │
│           ├─ Джойны (если нужно)                            │
│           └─ Бизнес-метрики (margin %, top products)        │
│                                                             │
└─────────────────────────────────────────────────────────────┘
         │                      │                     │
         ▼                      ▼                     ▼
      Trino                  Trino               ClickHouse
  (для ad-hoc           (для разработки)      (для Superset)
   по сырым данным)

```
