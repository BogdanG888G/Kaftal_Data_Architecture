import boto3
import traceback
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import re as re_m

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder.appName("Iceberg Lenta ETL").getOrCreate()
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
TARGET_TABLE = "iceberg.lenta_silver.sales"

SILVER_COLUMNS = [
    "year", "month", "retail_chain", "store_format", "store_code",
    "city_name", "product_segment", "product_id", "product_name",
    "vendor_code", "vendor",
    "sales_quantity", "sales_amount_rub", "sales_amount_vat_rub",
    "cost_price_rub", "average_sell_price", "average_cost_price",
    "margin_rub",
    "file_name", "created_at", "updated_at", "period",
]

SILVER_TYPES = {
    "year": "int",
    "month": "string",
    "retail_chain": "string",
    "store_format": "string",
    "store_code": "string",
    "city_name": "string",
    "product_segment": "string",
    "product_id": "string",
    "product_name": "string",
    "vendor_code": "string",
    "vendor": "string",
    "sales_quantity": "int",
    "sales_amount_rub": "float",
    "sales_amount_vat_rub": "float",
    "cost_price_rub": "float",
    "average_sell_price": "float",
    "average_cost_price": "float",
    "margin_rub": "float",
    "file_name": "string",
    "created_at": "date",
    "updated_at": "date",
    "period": "date",
}

COLUMN_RENAME_MAP = {
    "КалендГод/Месяц": "date_raw",
    "ТК": "store_id_raw",
    "ТК №": "store_code",
    "Товар": "product_id",
    "Наименвоание": "product_name",
    "Наименование": "product_name",
    "Подкатегория (текущая)": "product_segment",
    "Код Города": "city_name",
    "Поставщик (по ОЗМ)": "vendor_code",
    "Поставщик": "vendor",
    "Оборот ЗЦ": "cost_price_rub",
    "Оборот ПЦ": "sales_amount_rub",
    "Оборот ПЦ НДС": "sales_amount_vat_rub",
    "Оборот БЕИ": "sales_quantity",
}

MONTH_MAPPING = {
    "январь": "Январь", "февраль": "Февраль", "март": "Март",
    "апрель": "Апрель", "май": "Май", "июнь": "Июнь",
    "июль": "Июль", "август": "Август", "сентябрь": "Сентябрь",
    "октябрь": "Октябрь", "ноябрь": "Ноябрь", "декабрь": "Декабрь",
    "january": "Январь", "february": "Февраль", "march": "Март",
    "april": "Апрель", "may": "Май", "june": "Июнь",
    "july": "Июль", "august": "Август", "september": "Сентябрь",
    "october": "Октябрь", "november": "Ноябрь", "december": "Декабрь",
}

MONTH_NUM_TO_NAME = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}

MONTH_NAME_TO_NUM = {v: k for k, v in MONTH_NUM_TO_NAME.items()}

SHORT_MONTH = {
    "jan": "january", "feb": "february", "mar": "march",
    "apr": "april", "may": "may", "jun": "june",
    "jul": "july", "aug": "august", "sep": "september",
    "oct": "october", "nov": "november", "dec": "december",
}


# ============================================================
# BOOTSTRAP: NAMESPACE + TABLE
# ============================================================
def ensure_namespace_and_table():
    """Создаёт namespace и таблицу если их нет"""
    print("Проверяем/создаём namespace и таблицу для Ленты...")

    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.lenta_silver")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            year INT,
            month STRING,
            retail_chain STRING,
            store_format STRING,
            store_code STRING,
            city_name STRING,
            product_segment STRING,
            product_id STRING,
            product_name STRING,
            vendor_code STRING,
            vendor STRING,
            sales_quantity INT,
            sales_amount_rub FLOAT,
            sales_amount_vat_rub FLOAT,
            cost_price_rub FLOAT,
            average_sell_price FLOAT,
            average_cost_price FLOAT,
            margin_rub FLOAT,
            file_name STRING,
            created_at DATE,
            updated_at DATE,
            period DATE
        )
        USING iceberg
        PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/lenta_silver/sales'
    """)

    print("✓ Namespace и таблица Ленты готовы")


try:
    ensure_namespace_and_table()
except Exception:
    print("❌ Ошибка bootstrap для lenta_silver")
    traceback.print_exc()
    raise


# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket="raw", Prefix="lenta_")

if "Contents" not in objects:
    print("⚠ Нет файлов lenta_ в бакете raw")
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

        file_name = f"s3a://raw/{file}"

        print("=" * 100)
        print(f"Обработка: {file_name}")
        print("=" * 100)

        df = spark.read.csv(file_name, sep=";", header=True, inferSchema=False)
        print(f"✓ Прочитано строк: {df.count()}")
        print(f"Исходные колонки: {df.columns}")
        print("=" * 100)

        # ШАГ 1: Переименование
        for old_name in df.columns:
            new_name = COLUMN_RENAME_MAP.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)

        # ШАГ 2: Нормализация пустых строк
        for c in df.columns:
            df = df.withColumn(
                c,
                F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c)))
            )

        # ШАГ 3: Служебные
        df = df.withColumn("retail_chain", F.lit("Лента"))
        df = df.withColumn("store_format", F.lit("Лента"))
        df = df.withColumn("file_name", F.lit(file[:-4]))
        df = df.withColumn("created_at", F.lit(date_created))
        df = df.withColumn("updated_at", F.lit(date_created))

        # ШАГ 4: Месяц и год
        month_str = None
        parsed_year = None

        # --- из колонки date_raw (формат "04.2026") ---
        if "date_raw" in df.columns:
            first = (
                df.select("date_raw")
                .where(F.col("date_raw").isNotNull())
                .first()
            )
            if first and first[0]:
                drv = str(first[0]).strip()
                m = re_m.match(r"^(\d{1,2})\.(\d{4})$", drv)
                if m:
                    month_int = int(m.group(1))
                    parsed_year = int(m.group(2))
                    month_str = MONTH_NUM_TO_NAME.get(month_int)
            df = df.drop("date_raw")

        # --- из имени файла (fallback) ---
        if month_str is None:
            parts = file.replace(".csv", "").split("_")
            for i, part in enumerate(parts):
                part_lower = SHORT_MONTH.get(part.lower(), part.lower())
                mc = MONTH_MAPPING.get(part_lower)
                if mc:
                    month_str = mc
                    if i + 1 < len(parts) and parts[i + 1].isdigit():
                        parsed_year = int(parts[i + 1])
                    break

        if month_str is None:
            month_str = "Неизвестно"
        month_int = MONTH_NAME_TO_NUM.get(month_str, 1)
        if parsed_year is None:
            parsed_year = datetime.now().year

        df = df.withColumn("month", F.lit(month_str))
        df = df.withColumn("year", F.lit(int(parsed_year)))
        period_done = datetime.strptime(f"{parsed_year}-{month_int}-01", "%Y-%m-%d")
        df = df.withColumn("period", F.lit(period_done))

        # ШАГ 5: Удаляем store_id_raw
        if "store_id_raw" in df.columns:
            df = df.drop("store_id_raw")

        # ШАГ 6: Замена запятых и пробелов → числа
        for col_name in ["cost_price_rub", "sales_amount_rub", "sales_amount_vat_rub"]:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.regexp_replace(
                        F.regexp_replace(F.col(col_name).cast("string"), r"\s+", ""),
                        ",", "."
                    )
                )

        for col_name in ["sales_quantity"]:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.regexp_replace(F.col(col_name).cast("string"), r"\s+", "")
                )

        # ШАГ 7: vendor_code как строка
        if "vendor_code" in df.columns:
            df = df.withColumn("vendor_code", F.col("vendor_code").cast("string"))

        # ШАГ 8: Недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        print(f"Оставшиеся колонки (NULL): {remaining}")
        for col in remaining:
            df = df.withColumn(col, F.lit(None))

        # ШАГ 9: Типы
        for col_name in df.columns:
            dtype_str = SILVER_TYPES.get(col_name)
            if dtype_str:
                try:
                    df = df.withColumn(col_name, F.col(col_name).cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {col_name}: {e}")

        print(f"Финальные колонки: {df.columns}")
        print("=" * 100)

        # ШАГ 10: Финальный датафрейм
        final_df = df.select(*SILVER_COLUMNS)

        # ШАГ 11: Расчёт средних цен и маржи
        final_df = final_df.withColumn(
            "average_sell_price",
            F.coalesce(
                F.col("average_sell_price"),
                F.when(
                    F.col("sales_quantity").isNotNull() & (F.col("sales_quantity") > 0),
                    F.col("sales_amount_rub") / F.col("sales_quantity")
                )
            )
        )

        final_df = final_df.withColumn(
            "average_cost_price",
            F.coalesce(
                F.col("average_cost_price"),
                F.when(
                    F.col("sales_quantity").isNotNull() & (F.col("sales_quantity") > 0),
                    F.col("cost_price_rub") / F.col("sales_quantity")
                )
            )
        )

        final_df = final_df.withColumn(
            "margin_rub",
            F.coalesce(
                F.col("margin_rub"),
                F.col("sales_amount_rub") - F.col("cost_price_rub")
            )
        )

        # ШАГ 12: Проверка дубликатов и запись
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

print("✅ Обработка Лента завершена!")