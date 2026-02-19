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

```
graph TD
    A[Источники: Файлы (CSV/XLSX)] -->|Airflow (сенсоры)| B(S3 Landing)
    G[Kafka (События)] -->|Spark Struct. Streaming| C
    
    B -->|Airflow -> Spark| C(Iceberg Raw - S3)
    C -->|dbt test / Great Expectations| D{Quality Check}
    D -- OK --> E[dbt run via Trino]
    D -- Fail --> F[Alert in Slack/Telegram]
    
    E --> G[(Iceberg Core / Features)]
    
    G --> H[Trino (Ad-hoc)]
    G --> I[Airflow -> ClickHouse (Mart Update)]
    G --> J[MLflow (Log features)]
    
    I --> K[(ClickHouse Marts)]
    K --> L[Superset / Grafana]
    
    H --> M[FastAPI (Data API)]
    M --> N[JSON for external apps]
    
    J --> O[MLflow Tracking Server]
    
    G -.-> P[(Greenplum - Archive/Heavy loads)]
```