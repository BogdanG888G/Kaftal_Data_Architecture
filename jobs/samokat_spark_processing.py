import boto3
import traceback
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder.appName("Iceberg Samokat ETL").getOrCreate()
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
FILE_PREFIX = "samokat_"

TARGET_TABLE = "iceberg.samokat_silver.samokat_sales"

SILVER_COLUMNS = [
    "year", "month", "month_num", "retail_chain",
    "city_name",
    "category_level_1", "category_level_2", "category_level_3", "category_level_4",
    "product_name", "vendor", "brand",
    "sales_quantity", "average_cost_price", "average_sell_price",
    "sales_amount_rub", "sales_cost_price",
    "file_name", "created_at", "updated_at", "period",
]

SILVER_TYPES = {
    "year": "int",
    "month": "string",
    "month_num": "int",
    "retail_chain": "string",
    "city_name": "string",
    "category_level_1": "string",
    "category_level_2": "string",
    "category_level_3": "string",
    "category_level_4": "string",
    "product_name": "string",
    "vendor": "string",
    "brand": "string",
    "sales_quantity": "double",
    "average_cost_price": "double",
    "average_sell_price": "double",
    "sales_amount_rub": "double",
    "sales_cost_price": "double",
    "file_name": "string",
    "created_at": "date",
    "updated_at": "date",
    "period": "date",
}

COLUMN_RENAME_MAP = {
    "Самокат/МегаМаркет": "retail_chain_raw",
    "Город": "city_name",
    "Категория 1": "category_level_1",
    "Категория 2": "category_level_2",
    "Категория 3": "category_level_3",
    "Категория 4": "category_level_4",
    "Наименование": "product_name",
    "Производитель": "vendor",
    "Бренд": "brand",
    "Выручка": "sales_amount_raw",
    "Количество продаж": "sales_quantity_raw",
    "Себестоимость": "sales_cost_raw",
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


def clean_money(col_name):
    """
    Убирает из денежного поля:
    - символ ₽
    - пробелы (в том числе неразрывные)
    - заменяет запятую на точку
    """
    return F.regexp_replace(
        F.regexp_replace(
            F.regexp_replace(
                F.col(col_name).cast("string"),
                "[₽\u20bd]", ""          # убираем знак рубля
            ),
            r"[\s\u00a0]+", ""           # убираем пробелы и неразрывные пробелы
        ),
        ",", "."                         # запятая → точка
    )


def parse_month_year_from_filename(filename: str):
    """
    Парсит месяц и год из имени файла вида samokat_may_2026.csv
    """
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


# ============================================================
# BOOTSTRAP: NAMESPACE + TABLE
# ============================================================
def ensure_namespace_and_table():
    print("Проверяем/создаём namespace и таблицу для Самоката...")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.samokat_silver")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            year INT,
            month STRING,
            month_num INT,
            retail_chain STRING,
            city_name STRING,
            category_level_1 STRING,
            category_level_2 STRING,
            category_level_3 STRING,
            category_level_4 STRING,
            product_name STRING,
            vendor STRING,
            brand STRING,
            sales_quantity DOUBLE,
            average_cost_price DOUBLE,
            average_sell_price DOUBLE,
            sales_amount_rub DOUBLE,
            sales_cost_price DOUBLE,
            file_name STRING,
            created_at DATE,
            updated_at DATE,
            period DATE
        )
        USING iceberg
        PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/samokat_silver/samokat_sales'
    """)

    print("✓ Namespace и таблица Самоката готовы")


try:
    ensure_namespace_and_table()
except Exception:
    print("❌ Ошибка bootstrap для samokat_silver")
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

        df = spark.read \
            .option("header", "true") \
            .option("sep", ";") \
            .option("encoding", "UTF-8") \
            .csv(file_path, inferSchema=False)

        print(f"✓ Прочитано строк: {df.count()}")
        print(f"Исходные колонки: {df.columns}")
        print("=" * 100)

        # ШАГ 1: Чистим BOM и пробелы в заголовках
        for old_name in df.columns:
            cleaned = old_name.replace("\ufeff", "").strip()
            if cleaned != old_name:
                df = df.withColumnRenamed(old_name, cleaned)

        # ШАГ 2: Переименование колонок
        for old_name in df.columns:
            new_name = COLUMN_RENAME_MAP.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)

        # ШАГ 3: Нормализация пустых строк
        for c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
            )

        # ШАГ 4: retail_chain из колонки или дефолт
        if "retail_chain_raw" in df.columns:
            df = df.withColumn("retail_chain", F.trim(F.col("retail_chain_raw")))
            df = df.drop("retail_chain_raw")
        else:
            df = df.withColumn("retail_chain", F.lit("Самокат"))

        # ШАГ 5: Служебные поля
        df = df.withColumn("file_name", F.lit(file[:-4]))
        df = df.withColumn("created_at", F.lit(date_created))
        df = df.withColumn("updated_at", F.lit(date_created))

        # ШАГ 6: Месяц / год / период из имени файла
        month_str, month_num, parsed_year = parse_month_year_from_filename(file)

        df = df.withColumn("month", F.lit(month_str))
        df = df.withColumn("month_num", F.lit(month_num))
        df = df.withColumn("year", F.lit(parsed_year))

        period_str = f"{parsed_year}-{month_num:02d}-01"
        df = df.withColumn("period", F.lit(period_str))

        # ШАГ 7: Очистка денежных полей (убираем ₽, пробелы)
        for col_name in ["sales_amount_raw", "sales_cost_raw"]:
            if col_name in df.columns:
                df = df.withColumn(col_name, clean_money(col_name))

        for col_name in ["sales_quantity_raw"]:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.regexp_replace(
                        F.regexp_replace(F.col(col_name).cast("string"), r"[\s\u00a0]+", ""),
                        ",", "."
                    )
                )

        # ШАГ 8: Перенос raw → silver
        df = df.withColumn("sales_amount_rub", F.col("sales_amount_raw"))
        df = df.withColumn("sales_cost_price", F.col("sales_cost_raw"))
        df = df.withColumn("sales_quantity", F.col("sales_quantity_raw"))

        # ШАГ 9: Недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        print(f"Оставшиеся колонки (NULL): {remaining}")
        for col in remaining:
            df = df.withColumn(col, F.lit(None))

        # ШАГ 10: Каст типов
        for col_name in df.columns:
            dtype_str = SILVER_TYPES.get(col_name)
            if dtype_str:
                try:
                    df = df.withColumn(col_name, F.col(col_name).cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {col_name}: {e}")

        print(f"Финальные колонки: {df.columns}")
        print("=" * 100)

        # ШАГ 11: Финальный датафрейм
        final_df = df.select(*SILVER_COLUMNS)

        # ШАГ 12: Средние цены
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

        final_df = final_df.withColumn(
            "average_cost_price",
            F.coalesce(
                F.col("average_cost_price"),
                F.when(
                    F.col("sales_quantity").isNotNull() & (F.col("sales_quantity") != 0),
                    F.col("sales_cost_price") / F.col("sales_quantity")
                )
            )
        )

        # ШАГ 13: Проверка дубликатов и запись
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

print("✅ Обработка Самокат завершена!")
spark.stop()