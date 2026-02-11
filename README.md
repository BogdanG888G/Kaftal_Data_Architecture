```
Airflow (загрузка файлов)
    ↓
S3 landing (сырые csv/xlsx)
    ↓
Airflow → Spark (парсинг в Iceberg)
    ↓
Iceberg raw (партиционированные таблицы на S3)
    ↓
Airflow → dbt run (через Trino или Spark)
    ↓
Iceberg staging → Iceberg core (трансформации в SQL)
    ↓
    ├─→ Trino (ad-hoc запросы аналитиков)
    │
    ├─→ Airflow → ClickHouse (обновление mart-ов для дашбордов)
    │       ↓
    │   Superset / Grafana (визуализация)
    │
    └─→ (опционально) MS SQL Server (если нужен для Power BI)
```