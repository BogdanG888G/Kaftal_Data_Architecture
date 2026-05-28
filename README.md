Подключение к ВМ:

`ssh -l bogdangor 213.165.222.200`

Перенос файлов:

1) Перейти в нужную папку

2) Выполнить команду:

`scp -i ~/.ssh/id_ed25519 <имя_файла> bogdangor@213.165.222.200:~/Kaftal_Data_Architecture/data/`

3) Скопировать все файлы из папки:

`scp -i ~/.ssh/id_ed25519 * bogdangor@213.165.222.200:~/Kaftal_Data_Architecture/data/`

# Конфиги к подключению БД к BI

Clickhouse: `clickhousedb://admin:123@clickhouse:8123/default`

Iceberg: `trino://admin@trino:8080/iceberg`

Postgres: `postgresql://airflow:airflow@postgres:5432/airflow`


# Запуск dbt-моделей

1) `source venv/bin/activate`
2) cd `dbt_part`
3) `dbt debug`


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

# Модель DWH Kimball Star Schema ⭐ в Clickhouse

```
┌─────────────────────────────────────────────────────┐
│                   S3 (MinIO)                        │
├─────────────────────────────────────────────────────┤
│  Bronze: CSV (сырые данные)                         │
│  Silver: Iceberg (очищенные, нормализованные)       │
│  Gold:   Iceberg (витрины, агрегаты) [ОПЦИОНАЛЬНО]  │
└─────────────────────────────────────────────────────┘
                      ↓
              ┌───────────────┐
              │  dbt (Trino)  │ ← читает Silver из Iceberg
              │               │   создаёт факты + dims
              └───────────────┘
                      ↓
┌─────────────────────────────────────────────────────┐
│              ClickHouse (DWH)                       │
├─────────────────────────────────────────────────────┤
│  fact_sales        ← ФАКТЫ (продажи)                │
│  dim_product       ← справочник продуктов           │
│  dim_retail        ← справочник магазинов           │
│  dim_date          ← справочник дат                 │
│  dim_supplier      ← справочник поставщиков         │
└─────────────────────────────────────────────────────┘
                      ↓
              ┌───────────────┐
              │   Superset    │ ← дашборды
              └───────────────┘

```

# TODO: 

1. Грамотнее справочники/разметку сделать + добавить категории вкуса (сметана и лук, и т.д)

2. Superset (дашборды)

3. Мониторинг и алерты (Telegram)

4. Data Quality (dbt tests). Настроить более корректно обновление

5. Incremental Load

6. Soda / Great Expectations

7. GitHub Actions (CI/CD для dbt)

8. Grafana + Prometheus

9. Data Vault / Anchor Modeling