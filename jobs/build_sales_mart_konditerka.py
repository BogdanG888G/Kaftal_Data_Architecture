import traceback
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import clickhouse_connect

# ============================================================
# SPARK SESSION
# ============================================================
spark = (
    SparkSession.builder
    .appName("Build Konditerka Sales Mart")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.catalog.iceberg.cache-enabled", "false")
    .getOrCreate()
)

print(f"✓ Spark: {spark.version}")

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "123"
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "sales_mart_konditerka"
BATCH_SIZE = 50_000
NUM_PARTITIONS = 4
TODAY = datetime.now().strftime("%Y-%m-%d")


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        connect_timeout=30,
        send_receive_timeout=300,
    )


def get_loaded_files(client) -> set:
    """Получаем список уже загруженных файлов из ClickHouse"""
    rows = client.query(
        f"SELECT DISTINCT source_file FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    ).result_rows
    return {row[0] for row in rows if row[0]}


# ============================================================
# УНИФИКАЦИЯ ПО СЕТЯМ
# ============================================================
def select_x5(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").cast("string").alias("month"),
        F.when(F.col("month") == "Январь", 1)
         .when(F.col("month") == "Февраль", 2)
         .when(F.col("month") == "Март", 3)
         .when(F.col("month") == "Апрель", 4)
         .when(F.col("month") == "Май", 5)
         .when(F.col("month") == "Июнь", 6)
         .when(F.col("month") == "Июль", 7)
         .when(F.col("month") == "Август", 8)
         .when(F.col("month") == "Сентябрь", 9)
         .when(F.col("month") == "Октябрь", 10)
         .when(F.col("month") == "Ноябрь", 11)
         .when(F.col("month") == "Декабрь", 12)
         .cast("int").alias("month_num"),
        F.col("period").cast("date").alias("period"),
        F.col("retail_chain").cast("string").alias("retail_chain"),
        F.col("district_name").cast("string").alias("district_name"),
        F.col("region_name").cast("string").alias("region_name"),
        F.col("city_name").cast("string").alias("city_name"),
        F.col("address").cast("string").alias("address"),
        F.col("factory_code").cast("string").alias("store_code"),
        F.col("factory_name").cast("string").alias("store_name"),
        F.lit(None).cast("string").alias("store_format"),
        F.lit(None).cast("string").alias("store_subformat"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("brand").cast("string").alias("brand"),
        F.col("vendor").cast("string").alias("vendor"),
        F.lit(None).cast("string").alias("barcode"),
        F.lit(None).cast("double").alias("weight"),
        F.lit(None).cast("string").alias("unit"),
        F.col("product_category_2").cast("string").alias("category_main"),
        F.col("product_category_3").cast("string").alias("category_sub"),
        F.col("product_category_4").cast("string").alias("category_detail"),
        F.col("sales_quantity").cast("double").alias("sales_quantity"),
        F.when(
            F.lower(F.col("product_category_4")).rlike("весов"),
            F.col("sales_quantity").cast("double")
        ).otherwise(F.lit(None)).cast("double").alias("sales_kg"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.lit(None).cast("double").alias("sales_amount_no_vat"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        (F.col("sales_amount_rub") - F.col("sales_cost_price")).cast("double").alias("margin_rub"),
        F.when(
            F.col("sales_amount_rub") > 0,
            (F.col("sales_amount_rub") - F.col("sales_cost_price")) / F.col("sales_amount_rub") * 100
        ).cast("double").alias("margin_pct"),
        F.lit(None).cast("double").alias("losses_rub"),
        F.lit(None).cast("double").alias("losses_qty"),
        F.lit(None).cast("double").alias("losses_pct"),
        F.lit(None).cast("double").alias("write_off_rub"),
        F.lit(None).cast("double").alias("write_off_qty"),
        F.lit(None).cast("double").alias("write_off_pct"),
        F.lit(None).cast("string").alias("category_level_0"),
        F.lit(None).cast("string").alias("category_level_1"),
        F.lit(None).cast("string").alias("category_level_2"),
        F.lit(None).cast("string").alias("category_level_3"),
        F.lit(None).cast("string").alias("category_level_4"),
        F.lit("x5_sales").alias("source_table"),
        F.col("file_name").cast("string").alias("source_file"),
        F.lit(TODAY).cast("date").alias("loaded_at"),
    )


def select_magnit(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").cast("string").alias("month"),
        F.col("month_num").cast("int").alias("month_num"),
        F.col("period").cast("date").alias("period"),
        F.col("retail_chain").cast("string").alias("retail_chain"),
        F.lit(None).cast("string").alias("district_name"),
        F.col("region_name").cast("string").alias("region_name"),
        F.lit(None).cast("string").alias("city_name"),
        F.col("address").cast("string").alias("address"),
        F.col("store_code").cast("string").alias("store_code"),
        F.col("store_name").cast("string").alias("store_name"),
        F.col("format").cast("string").alias("store_format"),
        F.col("subformat").cast("string").alias("store_subformat"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.col("brand").cast("string").alias("brand"),
        F.col("vendor").cast("string").alias("vendor"),
        F.col("barcode").cast("string").alias("barcode"),
        F.col("weight").cast("double").alias("weight"),
        F.col("unit").cast("string").alias("unit"),
        F.col("category_level_1").cast("string").alias("category_main"),
        F.col("category_level_2").cast("string").alias("category_sub"),
        F.col("category_level_3").cast("string").alias("category_detail"),
        F.col("sales_quantity").cast("double").alias("sales_quantity"),
        F.when(
            F.lower(F.col("unit")) == "кг",
            F.col("sales_quantity").cast("double")
        ).otherwise(F.lit(None)).cast("double").alias("sales_kg"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.lit(None).cast("double").alias("sales_amount_no_vat"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        (F.col("sales_amount_rub") - F.col("sales_cost_price")).cast("double").alias("margin_rub"),
        F.when(
            F.col("sales_amount_rub") > 0,
            (F.col("sales_amount_rub") - F.col("sales_cost_price")) / F.col("sales_amount_rub") * 100
        ).cast("double").alias("margin_pct"),
        F.lit(None).cast("double").alias("losses_rub"),
        F.lit(None).cast("double").alias("losses_qty"),
        F.lit(None).cast("double").alias("losses_pct"),
        F.lit(None).cast("double").alias("write_off_rub"),
        F.lit(None).cast("double").alias("write_off_qty"),
        F.lit(None).cast("double").alias("write_off_pct"),
        F.col("category_level_0").cast("string").alias("category_level_0"),
        F.col("category_level_1").cast("string").alias("category_level_1"),
        F.col("category_level_2").cast("string").alias("category_level_2"),
        F.col("category_level_3").cast("string").alias("category_level_3"),
        F.col("category_level_4").cast("string").alias("category_level_4"),
        F.lit("magnit_sales").alias("source_table"),
        F.col("file_name").cast("string").alias("source_file"),
        F.lit(TODAY).cast("date").alias("loaded_at"),
    )


def select_auchan(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").cast("string").alias("month"),
        F.col("month_num").cast("int").alias("month_num"),
        F.col("period").cast("date").alias("period"),
        F.col("retail_chain").cast("string").alias("retail_chain"),
        F.lit(None).cast("string").alias("district_name"),
        F.lit(None).cast("string").alias("region_name"),
        F.col("city_name").cast("string").alias("city_name"),
        F.col("address").cast("string").alias("address"),
        F.regexp_extract(F.col("store"), r"\((\d+)\)", 1).alias("store_code"),
        F.col("store").cast("string").alias("store_name"),
        F.col("format").cast("string").alias("store_format"),
        F.lit(None).cast("string").alias("store_subformat"),
        F.col("product_id").cast("string").alias("product_id"),
        F.col("product_name").cast("string").alias("product_name"),
        F.lit(None).cast("string").alias("brand"),
        F.col("vendor").cast("string").alias("vendor"),
        F.lit(None).cast("string").alias("barcode"),
        F.lit(None).cast("double").alias("weight"),
        F.lit(None).cast("string").alias("unit"),
        F.col("segment").cast("string").alias("category_main"),
        F.col("family").cast("string").alias("category_sub"),
        F.lit(None).cast("string").alias("category_detail"),
        F.col("sales_quantity").cast("double").alias("sales_quantity"),
        F.col("sales_kg").cast("double").alias("sales_kg"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_amount_no_vat").cast("double").alias("sales_amount_no_vat"),
        F.when(
            F.col("average_cost_price").isNotNull() & F.col("sales_quantity").isNotNull(),
            F.col("average_cost_price") * F.col("sales_quantity")
        ).cast("double").alias("sales_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("margin_rub").cast("double").alias("margin_rub"),
        F.when(
            F.col("sales_amount_rub") > 0,
            F.col("margin_rub") / F.col("sales_amount_rub") * 100
        ).cast("double").alias("margin_pct"),
        F.col("losses_rub").cast("double").alias("losses_rub"),
        F.col("losses_qty").cast("double").alias("losses_qty"),
        F.col("losses_pct").cast("double").alias("losses_pct"),
        F.col("write_off_rub").cast("double").alias("write_off_rub"),
        F.col("write_off_qty").cast("double").alias("write_off_qty"),
        F.col("write_off_pct").cast("double").alias("write_off_pct"),
        F.lit(None).cast("string").alias("category_level_0"),
        F.lit(None).cast("string").alias("category_level_1"),
        F.lit(None).cast("string").alias("category_level_2"),
        F.lit(None).cast("string").alias("category_level_3"),
        F.lit(None).cast("string").alias("category_level_4"),
        F.lit("auchan_sales").alias("source_table"),
        F.col("file_name").cast("string").alias("source_file"),
        F.lit(TODAY).cast("date").alias("loaded_at"),
    )


# ============================================================
# ОБОГАЩЕНИЕ ВЕСОМ
# ============================================================
def enrich_weight(df):
    name_clean = F.regexp_replace(
        F.lower(F.coalesce(F.col("product_name"), F.lit(""))),
        r"\s+", ""
    )

    # Приоритет: weight из Магнита → кг из названия → г из названия
    weight_from_field = F.when(
        F.col("weight").isNotNull() & (F.col("weight") > 0),
        F.col("weight") * 1000
    )

    kg_match = F.regexp_extract(name_clean, r"(\d+(?:[.,]\d+)?)кг", 1)
    weight_from_kg = F.when(
        kg_match != "",
        F.regexp_replace(kg_match, ",", ".").cast("double") * 1000
    )

    g_match = F.regexp_extract(name_clean, r"(\d+(?:[.,]\d+)?)гр?", 1)
    weight_from_g = F.when(
        g_match != "",
        F.regexp_replace(g_match, ",", ".").cast("double")
    )

    df = df.withColumn(
        "weight_g_numeric",
        F.coalesce(weight_from_field, weight_from_kg, weight_from_g)
    )

    # Убираем мусор > 20 кг
    df = df.withColumn(
        "weight_g_numeric",
        F.when(
            (F.col("weight_g_numeric") > 0) & (F.col("weight_g_numeric") <= 20000),
            F.col("weight_g_numeric")
        )
    )

    df = df.withColumn("weight_g", F.col("weight_g_numeric").cast("string"))

    df = df.withColumn(
        "weight_segment",
        F.when(F.col("weight_g_numeric").isNull(), "Нет информации о весе")
         .when(F.col("weight_g_numeric") < 50,  "0-49")
         .when(F.col("weight_g_numeric") < 100, "50-99")
         .when(F.col("weight_g_numeric") < 200, "100-199")
         .when(F.col("weight_g_numeric") < 300, "200-299")
         .when(F.col("weight_g_numeric") < 400, "300-399")
         .when(F.col("weight_g_numeric") < 500, "400-499")
         .when(F.col("weight_g_numeric") < 600, "500-599")
         .when(F.col("weight_g_numeric") < 700, "600-699")
         .when(F.col("weight_g_numeric") < 800, "700-799")
         .when(F.col("weight_g_numeric") < 900, "800-899")
         .otherwise("900+")
    )

    return df


# ============================================================
# ЗАПИСЬ В CLICKHOUSE
# ============================================================
def insert_partition_to_clickhouse(iterator):
    import clickhouse_connect
    import math
    from datetime import date, datetime

    def normalize_type(ch_type):
        nullable = False
        t = ch_type
        while True:
            if t.startswith("Nullable("):
                nullable = True
                t = t[len("Nullable("):-1]
                continue
            if t.startswith("LowCardinality("):
                t = t[len("LowCardinality("):-1]
                continue
            break
        return t, nullable

    def cast_val(col_name, value, ch_type):
        base_type, nullable = normalize_type(ch_type)

        if value == "" and base_type in (
            "Int8", "Int16", "Int32", "Int64",
            "UInt8", "UInt16", "UInt32", "UInt64",
            "Float32", "Float64", "Date",
        ):
            value = None

        if value is None:
            if nullable:
                return None
            if base_type == "String":
                return ""
            if base_type == "Date":
                return date.today()
            if base_type.startswith("Int") or base_type.startswith("UInt"):
                return 0
            if base_type.startswith("Float"):
                return 0.0
            raise ValueError(f"NULL in non-nullable `{col_name}` ({ch_type})")

        try:
            if base_type == "String":
                return str(value)
            if base_type.startswith("Int") or base_type.startswith("UInt"):
                return int(value)
            if base_type.startswith("Float"):
                val = float(value)
                if math.isnan(val) or math.isinf(val):
                    return None if nullable else 0.0
                return val
            if base_type == "Date":
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, date):
                    return value
                return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
            return value
        except Exception as e:
            raise ValueError(f"Cast error `{col_name}` to `{ch_type}`: {value!r}") from e

    cols = ch_columns_b.value
    types = ch_types_b.value

    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username="admin",
        password="123",
        database="default",
        connect_timeout=30,
        send_receive_timeout=300,
    )

    batch = []
    for row in iterator:
        record = tuple(cast_val(col, row[col], types[col]) for col in cols)
        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            client.insert(CLICKHOUSE_TABLE, batch, column_names=cols)
            batch = []

    if batch:
        client.insert(CLICKHOUSE_TABLE, batch, column_names=cols)

    client.close()


# ============================================================
# ОСНОВНОЙ ПАЙПЛАЙН
# ============================================================
try:
    chains = {
        "x5":     ("iceberg.konditerka_silver.x5_sales",     select_x5),
        "magnit": ("iceberg.konditerka_silver.magnit_sales", select_magnit),
        "auchan": ("iceberg.konditerka_silver.auchan_sales", select_auchan),
    }

    # ── Шаг 1: Получаем схему CH и уже загруженные файлы ─────
    client = get_ch_client()
    loaded_files = get_loaded_files(client)

    desc_rows = client.query(
        f"DESCRIBE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    ).result_rows

    # ✅ ВАЖНО: убираем MATERIALIZED и ALIAS колонки из INSERT
    insertable_rows = [
        row for row in desc_rows
        if (row[2] or "").upper() not in ("MATERIALIZED", "ALIAS")
    ]

    ch_columns = [row[0] for row in insertable_rows]
    ch_types = {row[0]: row[1] for row in insertable_rows}
    client.close()

    print(f"✓ Схема получена: {len(ch_columns)} колонок для INSERT")
    print(f"✓ Уже загружено файлов: {len(loaded_files)}")

    # ── Шаг 2: Читаем Silver, фильтруем новые файлы ──────────
    print("\n" + "=" * 70)
    print("ИНКРЕМЕНТАЛЬНАЯ ЗАГРУЗКА")
    print("=" * 70)

    dfs = []
    for chain_name, (table_name, select_func) in chains.items():
        try:
            df = spark.table(table_name)

            silver_files = {
                row[0] for row in
                df.select("file_name").distinct().collect()
            }

            new_files = silver_files - loaded_files

            if not new_files:
                print(f"  ⊘ {chain_name}: нет новых файлов")
                continue

            print(f"  ✓ {chain_name}: {len(new_files)} новых файлов")
            for f in sorted(new_files):
                print(f"     + {f}")

            df_new = df.filter(F.col("file_name").isin(new_files))
            unified = select_func(df_new)
            dfs.append(unified)

        except Exception as e:
            print(f"  ⚠ {chain_name}: {str(e)[:200]}")

    if not dfs:
        print("\n✅ Витрина актуальна. Новых данных нет.")
        spark.stop()
        raise SystemExit(0)

    # ── Шаг 3: UNION ─────────────────────────────────────────
    all_data = dfs[0]
    for df in dfs[1:]:
        all_data = all_data.unionByName(df, allowMissingColumns=True)
    print(f"\n✓ UNION: {len(dfs)} сетей")

    # ── Шаг 4: Обогащение весом ──────────────────────────────
    all_data = enrich_weight(all_data)
    print("✓ Вес обогащён")

    # ── Шаг 5: Выравниваем под схему CH ──────────────────────
    for col_name in ch_columns:
        if col_name not in all_data.columns:
            all_data = all_data.withColumn(col_name, F.lit(None))

    all_data = all_data.select(*ch_columns)
    print("✓ Схема выровнена")

    # ── Шаг 6: Запись ────────────────────────────────────────
    print("\nЗаписываем новые данные в ClickHouse...")

    ch_columns_b = spark.sparkContext.broadcast(ch_columns)
    ch_types_b = spark.sparkContext.broadcast(ch_types)

    start_time = time.time()
    all_data.repartition(NUM_PARTITIONS).foreachPartition(insert_partition_to_clickhouse)
    elapsed = time.time() - start_time

    # ── Шаг 7: Итог ──────────────────────────────────────────
    client = get_ch_client()

    final_count = client.query(
        f"SELECT count() FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    ).result_set[0][0]

    chain_stats = client.query(f"""
        SELECT retail_chain,
               count() AS rows,
               round(sum(sales_amount_rub)/1e6, 2) AS revenue_mln_rub
        FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
        GROUP BY retail_chain
        ORDER BY rows DESC
    """).result_rows

    tier_stats = client.query(f"""
        SELECT brand_tier,
               count() AS rows,
               round(sum(sales_amount_rub)/1e6, 2) AS revenue_mln_rub
        FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
        GROUP BY brand_tier
        ORDER BY revenue_mln_rub DESC
    """).result_rows

    client.close()

    print(f"\n{'=' * 70}")
    print(f"✅ Витрина обновлена!")
    print(f"   Всего строк:  {final_count:,}")
    print(f"   Время записи: {elapsed:.1f}с")

    print(f"\n📈 По сетям:")
    for row in chain_stats:
        print(f"   {row[0]:<15} | {row[1]:>10,} стр | {row[2]:>8,.2f} млн ₽")

    print(f"\n🏷 По Brand Tier (считает ClickHouse):")
    for row in tier_stats:
        print(f"   {str(row[0]):<25} | {row[1]:>10,} стр | {row[2]:>8,.2f} млн ₽")

    print("=" * 70)

except SystemExit:
    pass

except Exception:
    print("\n⚠ Ошибка пайплайна:")
    traceback.print_exc()
    raise

finally:
    try:
        spark.stop()
        print("✓ Spark остановлен")
    except Exception:
        pass