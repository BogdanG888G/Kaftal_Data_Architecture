import boto3
import traceback
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder.appName("Iceberg Svetofor ETL").getOrCreate()
print(f"✓ Версия Spark: {spark.version}")

# ============================================================
# S3 CLIENT
# ============================================================
s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

# ============================================================
# КОНСТАНТЫ
# ============================================================
RAW_BUCKET = "raw"
FILE_PREFIX = "svetofor"

TARGET_TABLE = "iceberg.svetofor_silver.sales"

SILVER_COLUMNS = [
    "year", "month", "month_num", "retail_chain",
    "region_name", "address",
    "category_level_1", "category_level_2",
    "product_id", "vendor_code", "product_name",
    "sales_quantity", "sales_amount_rub",
    "average_sell_price", "average_cost_price",
    "sales_cost_price",
    "file_name", "created_at", "updated_at", "period",
]

SILVER_TYPES = {
    "year": "int",
    "month": "string",
    "month_num": "int",
    "retail_chain": "string",
    "region_name": "string",
    "address": "string",
    "category_level_1": "string",
    "category_level_2": "string",
    "product_id": "string",
    "vendor_code": "string",
    "product_name": "string",
    "sales_quantity": "double",
    "sales_amount_rub": "double",
    "average_sell_price": "double",
    "average_cost_price": "double",
    "sales_cost_price": "double",
    "file_name": "string",
    "created_at": "date",
    "updated_at": "date",
    "period": "date",
}

MONTH_MAPPING = {
    "january": "Январь", "february": "Февраль", "march": "Март",
    "april": "Апрель", "may": "Май", "june": "Июнь",
    "july": "Июль", "august": "Август", "september": "Сентябрь",
    "october": "Октябрь", "november": "Ноябрь", "december": "Декабрь",
    "jan": "Январь", "feb": "Февраль", "mar": "Март",
    "apr": "Апрель", "may": "Май", "jun": "Июнь",
    "jul": "Июль", "aug": "Август", "sep": "Сентябрь",
    "oct": "Октябрь", "nov": "Ноябрь", "dec": "Декабрь",
}

MONTH_NAME_TO_NUM = {
    "Январь": 1, "Февраль": 2, "Март": 3,
    "Апрель": 4, "Май": 5, "Июнь": 6,
    "Июль": 7, "Август": 8, "Сентябрь": 9,
    "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12,
}


# ============================================================
# УТИЛИТЫ
# ============================================================
def clean_number(col_name):
    """
    Убирает пробелы (в т.ч. неразрывные), знак ₽, заменяет запятую на точку.
    """
    return F.regexp_replace(
        F.regexp_replace(
            F.regexp_replace(
                F.col(col_name).cast("string"),
                "[₽\u20bd]", ""
            ),
            r"[\s\u00a0]+", ""
        ),
        ",", "."
    )


def normalize_text_for_compare(col_name):
    """
    Нормализация строки для сравнения:
    - lower
    - убираем кавычки всех видов
    - убираем содержимое скобок
    - убираем все пробелы и неразрывные пробелы
    - оставляем только буквы и цифры
    """
    return F.lower(
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(
                        F.col(col_name).cast("string"),
                        r'[«»""„\"\']',
                        ""
                    ),
                    r"\(.*?\)",
                    ""
                ),
                r"[\s\u00a0]+",
                ""
            ),
            r"[^a-zA-Zа-яА-ЯёЁ0-9]",
            ""
        )
    )


def parse_month_year_from_filename(filename: str):
    parts = filename.replace(".csv", "").split("_")
    month_str = None
    parsed_year = None

    for i, part in enumerate(parts):
        mc = MONTH_MAPPING.get(part.lower())
        if mc:
            month_str = mc
            if i + 1 < len(parts) and parts[i + 1].isdigit():
                parsed_year = int(parts[i + 1])
            break

    if month_str is None:
        month_str = "Неизвестно"
    if parsed_year is None:
        parsed_year = datetime.now().year

    month_num = MONTH_NAME_TO_NUM.get(month_str, 1)
    return month_str, month_num, parsed_year


def read_csv_smart(file_path: str):
    """
    Пытается прочитать CSV с разными разделителями и кодировками.
    Возвращает DataFrame с наибольшим числом колонок.
    """
    candidates = []
    for enc in ["UTF-8", "Windows-1251"]:
        for sep in [";", "\t", ","]:
            try:
                df_try = spark.read \
                    .option("header", "true") \
                    .option("sep", sep) \
                    .option("encoding", enc) \
                    .option("multiLine", "true") \
                    .option("quote", '"') \
                    .csv(file_path, inferSchema=False)
                cols_count = len(df_try.columns)
                candidates.append((cols_count, enc, sep, df_try))
                print(f"  Проба: encoding={enc}, sep='{sep}' → {cols_count} колонок")
            except Exception as e:
                print(f"  ✗ encoding={enc}, sep='{sep}': {e}")

    if not candidates:
        raise RuntimeError(f"Не удалось прочитать {file_path}")

    candidates.sort(key=lambda x: x[0], reverse=True)
    best = candidates[0]
    print(f"  ✓ Выбрано: encoding={best[1]}, sep='{best[2]}', колонок={best[0]}")
    return best[3]


# ============================================================
# BOOTSTRAP
# ============================================================
def ensure_namespace_and_table():
    print("Проверяем/создаём namespace и таблицу для Светофора...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.svetofor_silver")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            year INT,
            month STRING,
            month_num INT,
            retail_chain STRING,
            region_name STRING,
            address STRING,
            category_level_1 STRING,
            category_level_2 STRING,
            product_id STRING,
            vendor_code STRING,
            product_name STRING,
            sales_quantity DOUBLE,
            sales_amount_rub DOUBLE,
            average_sell_price DOUBLE,
            average_cost_price DOUBLE,
            sales_cost_price DOUBLE,
            file_name STRING,
            created_at DATE,
            updated_at DATE,
            period DATE
        )
        USING iceberg
        PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/svetofor_silver/sales'
    """)
    print("✓ Namespace и таблица Светофора готовы")


try:
    ensure_namespace_and_table()
except Exception:
    print("❌ Ошибка bootstrap для svetofor_silver")
    traceback.print_exc()
    raise


# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix=FILE_PREFIX)

if "Contents" not in objects:
    print(f"⚠ Нет файлов {FILE_PREFIX} в бакете {RAW_BUCKET}")
else:
    files = objects["Contents"]

    year_cr = datetime.now().year
    month_cr = datetime.now().month
    day_cr = datetime.now().day
    date_created = f"{year_cr}-{month_cr:02d}-{day_cr:02d}"

    for obj in files:
        file = obj["Key"]
        if not file.endswith(".csv"):
            continue

        file_path = f"s3a://{RAW_BUCKET}/{file}"

        print("=" * 100)
        print(f"Обработка: {file_path}")
        print("=" * 100)

        df = read_csv_smart(file_path)

        print(f"✓ Прочитано строк: {df.count()}")
        print(f"Исходные колонки: {df.columns}")
        print("=" * 100)

        # ============================================================
        # ШАГ 1: чистим BOM и пробелы в заголовках
        # ============================================================
        for old_name in df.columns:
            cleaned = old_name.replace("\ufeff", "").strip()
            if cleaned != old_name:
                df = df.withColumnRenamed(old_name, cleaned)

        # ============================================================
        # ШАГ 2: поиск нужных колонок по частичным совпадениям
        # ============================================================
        cols = df.columns
        cols_lower = {c: c.lower() for c in cols}

        def find_col(*keywords):
            """Возвращает первую колонку, содержащую все keywords (lower)."""
            for c, lc in cols_lower.items():
                if all(kw in lc for kw in keywords):
                    return c
            return None

        def find_all(*keywords):
            return [c for c, lc in cols_lower.items()
                    if all(kw in lc for kw in keywords)]

        col_region     = find_col("округ")
        col_product    = find_col("товар")
        col_address    = find_col("адрес")
        parents        = find_all("родител")
        col_code       = find_col("код")
        col_article    = find_col("артикул")
        col_sum_total  = find_col("итог", "сумма")
        col_qty_total  = find_col("итог", "количеств")
        col_avg_price  = find_col("средн", "цена")
        col_avg_cost   = find_col("средн", "себестоим")

        print(f"  region:     {col_region}")
        print(f"  product:    {col_product}")
        print(f"  address:    {col_address}")
        print(f"  parents:    {parents}")
        print(f"  code:       {col_code}")
        print(f"  article:    {col_article}")
        print(f"  sum_total:  {col_sum_total}")
        print(f"  qty_total:  {col_qty_total}")
        print(f"  avg_price:  {col_avg_price}")
        print(f"  avg_cost:   {col_avg_cost}")

        # ============================================================
        # ШАГ 3: переименование
        # ============================================================
        rename_map = {}
        if col_region:        rename_map[col_region]    = "region_name"
        if col_product:       rename_map[col_product]   = "product_name"
        if col_address:       rename_map[col_address]   = "address"
        if len(parents) >= 1: rename_map[parents[0]]    = "category_level_1"
        if len(parents) >= 2: rename_map[parents[1]]    = "category_level_2"
        if col_code:          rename_map[col_code]      = "product_id_raw"
        if col_article:       rename_map[col_article]   = "vendor_code"
        if col_sum_total:     rename_map[col_sum_total] = "sales_amount_raw"
        if col_qty_total:     rename_map[col_qty_total] = "sales_quantity_raw"
        if col_avg_price:     rename_map[col_avg_price] = "avg_sell_raw"
        if col_avg_cost:      rename_map[col_avg_cost]  = "avg_cost_raw"

        for old, new in rename_map.items():
            df = df.withColumnRenamed(old, new)

        # ============================================================
        # ШАГ 4: оставляем только нужные колонки (выкидываем недели)
        # ============================================================
        keep_cols = [c for c in [
            "region_name", "product_name", "address",
            "category_level_1", "category_level_2",
            "product_id_raw", "vendor_code",
            "sales_amount_raw", "sales_quantity_raw",
            "avg_sell_raw", "avg_cost_raw",
        ] if c in df.columns]

        df = df.select(*keep_cols)
        print(f"  Оставили колонки: {df.columns}")

        # ============================================================
        # ШАГ 5: нормализация пустых строк
        # ============================================================
        for c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
            )

        # ============================================================
        # ШАГ 5.1: удаляем агрегатные строки,
        # где address фактически дублирует product_name
        # (в исходных данных Светофора такие строки — итоги по товару,
        #  а не реальные адреса магазинов)
        # ============================================================
        if "address" in df.columns and "product_name" in df.columns:
            addr_norm = normalize_text_for_compare("address")
            prod_norm = normalize_text_for_compare("product_name")

            before_cnt = df.count()

            df = df.filter(
                ~(
                    F.col("address").isNotNull() &
                    F.col("product_name").isNotNull() &
                    (addr_norm == prod_norm)
                )
            )

            after_cnt = df.count()
            dropped = before_cnt - after_cnt
            print(f"  Удалено агрегатных строк (address == product_name): {dropped}")
            print(f"  Осталось строк после фильтра: {after_cnt}")

        # ============================================================
        # ШАГ 6: product_id (Код) — расшифровка научной нотации
        # 9E+09  →  9000000000 (примерно)
        # ============================================================
        if "product_id_raw" in df.columns:
            df = df.withColumn(
                "product_id",
                F.when(
                    F.col("product_id_raw").rlike("(?i)e"),
                    F.col("product_id_raw").cast("double").cast("long").cast("string")
                ).otherwise(F.col("product_id_raw").cast("string"))
            ).drop("product_id_raw")
        else:
            df = df.withColumn("product_id", F.lit(None).cast("string"))

        # ============================================================
        # ШАГ 7: retail_chain + служебные поля
        # ============================================================
        df = df.withColumn("retail_chain", F.lit("Светофор"))
        df = df.withColumn("file_name", F.lit(file[:-4]))
        df = df.withColumn("created_at", F.lit(date_created))
        df = df.withColumn("updated_at", F.lit(date_created))

        # ============================================================
        # ШАГ 8: месяц/год/период
        # ============================================================
        month_str, month_num, parsed_year = parse_month_year_from_filename(file)
        df = df.withColumn("month", F.lit(month_str))
        df = df.withColumn("month_num", F.lit(month_num))
        df = df.withColumn("year", F.lit(parsed_year))
        period_str = f"{parsed_year}-{month_num:02d}-01"
        df = df.withColumn("period", F.lit(period_str))

        # ============================================================
        # ШАГ 9: чистка чисел (убираем пробелы, ₽, меняем , на .)
        # ============================================================
        for col_name in ["sales_amount_raw", "sales_quantity_raw",
                         "avg_sell_raw", "avg_cost_raw"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, clean_number(col_name))

        # ============================================================
        # ШАГ 10: перенос raw → silver
        # ============================================================
        if "sales_amount_raw" in df.columns:
            df = df.withColumn("sales_amount_rub", F.col("sales_amount_raw"))
        if "sales_quantity_raw" in df.columns:
            df = df.withColumn("sales_quantity", F.col("sales_quantity_raw"))
        if "avg_sell_raw" in df.columns:
            df = df.withColumn("average_sell_price", F.col("avg_sell_raw"))
        if "avg_cost_raw" in df.columns:
            df = df.withColumn("average_cost_price", F.col("avg_cost_raw"))

        # ============================================================
        # ШАГ 11: недостающие колонки
        # ============================================================
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        print(f"Оставшиеся колонки (NULL): {remaining}")
        for col in remaining:
            df = df.withColumn(col, F.lit(None))

        # ============================================================
        # ШАГ 12: каст типов
        # ============================================================
        for col_name in df.columns:
            dtype_str = SILVER_TYPES.get(col_name)
            if dtype_str:
                try:
                    df = df.withColumn(col_name, F.col(col_name).cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {col_name}: {e}")

        # ============================================================
        # ШАГ 13: расчёт sales_cost_price = avg_cost * qty
        # ============================================================
        final_df = df.select(*SILVER_COLUMNS)

        final_df = final_df.withColumn(
            "sales_cost_price",
            F.coalesce(
                F.col("sales_cost_price"),
                F.when(
                    F.col("average_cost_price").isNotNull()
                    & F.col("sales_quantity").isNotNull(),
                    F.col("average_cost_price") * F.col("sales_quantity")
                )
            )
        )

        # average_sell_price fallback: sales_amount / qty
        final_df = final_df.withColumn(
            "average_sell_price",
            F.coalesce(
                F.col("average_sell_price"),
                F.when(
                    F.col("sales_quantity").isNotNull() & (F.col("sales_quantity") != 0),
                    F.col("sales_amount_rub") / F.col("sales_quantity")
                )
            )
        )

        print(f"Финальные колонки: {final_df.columns}")

        # ============================================================
        # ШАГ 14: проверка дубликатов и запись
        # ============================================================
        try:
            res = spark.sql(f"""
                SELECT COUNT(*)
                FROM {TARGET_TABLE}
                WHERE file_name = '{file[:-4]}'
            """).first()
            file_exists = res[0] > 0
        except Exception:
            file_exists = False

        if not file_exists:
            rows = final_df.count()
            print(f"Записываем {rows} строк...")
            try:
                final_df.writeTo(TARGET_TABLE).append()
                print(f"✓ {file} → {rows} строк")
            except Exception:
                print(f"❌ Ошибка записи файла {file}")
                traceback.print_exc()
                raise
        else:
            print(f"⊘ {file} уже есть в таблице")

        print("=" * 100)
        print()

print("✅ Обработка Светофора завершена!")
spark.stop()