"""
Обновление справочника товаров из Iceberg → ClickHouse
"""
import traceback
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import clickhouse_connect
from datetime import datetime

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder.appName("Update Product Mapping").getOrCreate()

print(f"✓ Spark: {spark.version}")
print(f"✓ Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# ============================================================
# КОНСТАНТЫ
# ============================================================
TABLES = [
    ("aushan_silver",      "iceberg.aushan_silver.sales"),
    ("bristol_silver",     "iceberg.bristol_silver.sales"),
    ("diksi_silver",       "iceberg.diksi_silver.sales"),
    ("lenta_silver",       "iceberg.lenta_silver.sales"),
    ("magnit_silver",      "iceberg.magnit_silver.sales"),
    ("okey_silver",        "iceberg.okey_silver.sales"),
    ("perekrestok_silver", "iceberg.perekrestok_silver.sales"),
    ("pyaterochka_silver", "iceberg.pyaterochka_silver.sales"),
    ("redwhite_silver",    "iceberg.redwhite_silver.sales"),
    ("vernyi_silver",      "iceberg.vernyi_silver.sales"),
    ("x5_silver",          "iceberg.x5_silver.sales"),
    ("magnit_new_silver",  "iceberg.magnit_new_silver.magnit_new_sales"),
    ("samokat_silver",     "iceberg.samokat_silver.samokat_sales"),
    ("globus_silver", "iceberg.globus_silver.sales")
]

CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "123"
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "product_mapping"

JUNK_FILTERS = ["тест", "test", "удалить", "delete", "xxx", "---"]
MIN_NAME_LENGTH = 3


# ============================================================
# ХЕЛПЕРЫ
# ============================================================
def get_products_from_table(table_name: str):
    try:
        df = spark.sql(f"""
            SELECT DISTINCT product_name
            FROM {table_name}
            WHERE product_name IS NOT NULL
              AND TRIM(product_name) != ''
        """)
        count = df.count()
        return df, count
    except Exception as e:
        print(f"  ⚠ {table_name}: ошибка — {str(e)[:80]}")
        return None, 0


def clean_products(df):
    df = df.filter(F.col("product_name").isNotNull())
    df = df.filter(F.trim(F.col("product_name")) != "")
    df = df.filter(F.length(F.trim(F.col("product_name"))) > MIN_NAME_LENGTH)

    for junk in JUNK_FILTERS:
        df = df.filter(~F.lower(F.col("product_name")).contains(junk))

    df = df.dropDuplicates(["product_name"])
    return df


def ensure_clickhouse_table(client):
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_TABLE} (
            original_name   String,
            brand_manual    String      DEFAULT '',
            chip_type_manual String     DEFAULT '',
            package_manual  String      DEFAULT '',
            flavor_manual   String      DEFAULT '',
            weight_manual   Float64     DEFAULT 0.0,
            created_at      DateTime    DEFAULT now(),
            updated_at      DateTime    DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY original_name
    """)
    print(f"✓ Таблица {CLICKHOUSE_TABLE} проверена/создана")


# ============================================================
# СБОР ТОВАРОВ ИЗ ВСЕХ СЕТЕЙ
# ============================================================
print("\n" + "=" * 80)
print("СБОР УНИКАЛЬНЫХ ТОВАРОВ ИЗ ВСЕХ СЕТЕЙ")
print("=" * 80)

all_products = None
stats = {}

for name, table in TABLES:
    df, count = get_products_from_table(table)

    if df is not None and count > 0:
        stats[name] = count
        print(f"  ✓ {name}: {count:,} уникальных товаров")

        if all_products is None:
            all_products = df
        else:
            all_products = all_products.unionByName(df)
    else:
        stats[name] = 0
        print(f"  ⊘ {name}: нет данных или таблица недоступна")

# ============================================================
# ОБРАБОТКА
# ============================================================
print("\n" + "=" * 80)
print("ОБРАБОТКА ДАННЫХ")
print("=" * 80)

if all_products is None:
    print("⚠ Нет данных для обработки!")
    spark.stop()
    raise SystemExit(0)

all_products = clean_products(all_products)
total_unique = all_products.count()
print(f"✓ Уникальных товаров после очистки: {total_unique:,}")

print("\nСтатистика по сетям:")
for name, cnt in stats.items():
    status = f"{cnt:,}" if cnt > 0 else "—"
    print(f"  • {name}: {status}")

new_names_pd = all_products.toPandas()
new_names_pd = new_names_pd.dropna(subset=["product_name"])
new_names_pd["original_name"] = new_names_pd["product_name"].astype(str).str.strip()
new_names_pd = new_names_pd.drop(columns=["product_name"])

new_names_pd = new_names_pd[~new_names_pd["original_name"].isin(["nan", "None", "", "NULL"])]
new_names_pd = new_names_pd[new_names_pd["original_name"].str.len() > MIN_NAME_LENGTH]
new_names_pd = new_names_pd.drop_duplicates(subset=["original_name"])

print(f"✓ После финальной очистки: {len(new_names_pd):,} товаров")

# ============================================================
# ЗАПИСЬ В CLICKHOUSE
# ============================================================
print("\n" + "=" * 80)
print("ЗАПИСЬ В CLICKHOUSE")
print("=" * 80)

try:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        connect_timeout=30,
        send_receive_timeout=60,
    )
    print("✓ Подключение к ClickHouse установлено")

    ensure_clickhouse_table(client)

    existing_df = client.query_df(
        f"SELECT DISTINCT original_name FROM {CLICKHOUSE_TABLE}"
    )
    existing_names = (
        set(existing_df["original_name"].tolist())
        if not existing_df.empty
        else set()
    )
    print(f"✓ Существующих товаров в справочнике: {len(existing_names):,}")

    new_ones = new_names_pd[~new_names_pd["original_name"].isin(existing_names)].copy()
    cnt_new = len(new_ones)
    print(f"✓ Новых товаров для добавления: {cnt_new:,}")

    if cnt_new > 0:
        new_ones["brand_manual"] = ""
        new_ones["chip_type_manual"] = ""
        new_ones["package_manual"] = ""
        new_ones["flavor_manual"] = ""
        new_ones["weight_manual"] = 0.0

        client.insert_df(CLICKHOUSE_TABLE, new_ones)
        print(f"✅ Добавлено {cnt_new:,} новых товаров в справочник!")

        print("\nПримеры новых товаров (первые 10):")
        for _, row in new_ones.head(10).iterrows():
            print(f"  • {row['original_name'][:80]}")
    else:
        print("✅ Новых товаров нет — справочник актуален")

    total_after = client.query_df(
        f"SELECT COUNT(*) AS cnt FROM {CLICKHOUSE_TABLE}"
    )
    print(f"\n📊 Итоговая статистика:")
    print(f"  • Всего товаров в справочнике: {total_after['cnt'].iloc[0]:,}")
    print(f"  • Добавлено сегодня: {cnt_new:,}")

    client.close()
    print("✓ Соединение с ClickHouse закрыто")

except Exception:
    print("❌ Ошибка при работе с ClickHouse")
    traceback.print_exc()
    raise

# ============================================================
# ФИНАЛ
# ============================================================
print("\n" + "=" * 80)
print(f"✅ ОБНОВЛЕНИЕ СПРАВОЧНИКА ЗАВЕРШЕНО")
print(f"   Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

spark.stop()