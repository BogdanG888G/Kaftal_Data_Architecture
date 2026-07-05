import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
from datetime import datetime
import re

spark = SparkSession.builder.appName("Iceberg Globus ETL").getOrCreate()
print(f"✓ Spark: {spark.version}")

# ============================================================
# КОНСТАНТЫ
# ============================================================
SILVER_COLUMNS = [
    'retail_chain', 'store_code', 'product_id', 'product_name',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_cost_price', 'average_sell_price', 'margin_rub',
    'year', 'month', 'period',
    'file_name', 'created_at', 'updated_at'
]

SILVER_TYPES = {
    'retail_chain': 'string',
    'store_code': 'string',
    'product_id': 'string',
    'product_name': 'string',
    'sales_quantity': 'double',      # чтобы не было проблем с null
    'sales_amount_rub': 'double',
    'sales_cost_price': 'double',
    'average_cost_price': 'double',
    'average_sell_price': 'double',
    'margin_rub': 'double',
    'year': 'int',
    'month': 'string',
    'period': 'date',
    'file_name': 'string',
    'created_at': 'date',
    'updated_at': 'date'
}

COLUMN_RENAME_MAP = {
    'Артикул': 'product_id',
    'Магазин': 'store_code',
    'Товар': 'product_name',
    'Итого_кол-во': 'sales_quantity',
    'Итого_выручка_нетто': 'sales_amount_rub',
    'Итого_выручка_брутто': 'sales_amount_rub_gross',
    'Средняя_ЦЗ': 'average_cost_price',
    'Средняя_ЦП': 'average_sell_price',
    'Итоговая_маржа': 'margin_rub',
}

# ============================================================
# ХЕЛПЕРЫ
# ============================================================
def read_csv_with_auto_sep(path):
    """
    Пробует разные разделители и возвращает DataFrame с наибольшим числом колонок.
    """
    separators = ['\t', ',', ';']
    best_df = None
    best_cols = 0

    for sep in separators:
        try:
            df_tmp = spark.read \
                .option("header", "true") \
                .option("sep", sep) \
                .option("quote", '"') \
                .option("escape", '"') \
                .option("mode", "PERMISSIVE") \
                .csv(path)
            num_cols = len(df_tmp.columns)
            if num_cols > best_cols:
                best_cols = num_cols
                best_df = df_tmp
                print(f"✓ Разделитель '{sep}' дал {num_cols} колонок")
        except Exception as e:
            print(f"Ошибка с разделителем '{sep}': {e}")
            continue

    if best_df is None:
        raise ValueError("Не удалось прочитать CSV ни с одним разделителем")
    return best_df

def parse_month_year_from_filename(filename: str):
    """
    Извлекает месяц и год из имени файла.
    Например: 'globus_ceptember_2025.csv' → ('Сентябрь', 2025)
    """
    month_mapping = {
        'january': 'Январь', 'february': 'Февраль', 'march': 'Март',
        'april': 'Апрель', 'may': 'Май', 'june': 'Июнь',
        'july': 'Июль', 'august': 'Август', 'september': 'Сентябрь',
        'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
        'январь': 'Январь', 'февраль': 'Февраль', 'март': 'Март',
        'апрель': 'Апрель', 'май': 'Май', 'июнь': 'Июнь',
        'июль': 'Июль', 'август': 'Август', 'сентябрь': 'Сентябрь',
        'октябрь': 'Октябрь', 'ноябрь': 'Ноябрь', 'декабрь': 'Декабрь',
        'ceptember': 'Сентябрь'   # исправляем опечатку
    }
    base = filename.replace('.csv', '')
    parts = re.split(r'[_\-.]+', base)
    month_str = None
    year = None

    for i, part in enumerate(parts):
        part_lower = part.lower()
        if part_lower in month_mapping:
            month_str = month_mapping[part_lower]
            # Ищем год рядом (перед или после)
            if i + 1 < len(parts) and parts[i+1].isdigit():
                year = int(parts[i+1])
            elif i > 0 and parts[i-1].isdigit():
                year = int(parts[i-1])
            break

    # Если год не найден, ищем любое 4-значное число
    if year is None:
        for part in parts:
            if part.isdigit() and len(part) == 4:
                year = int(part)
                break

    if year is None:
        year = datetime.now().year   # fallback
    if month_str is None:
        month_str = 'Сентябрь'        # fallback

    return month_str, year

def normalize_number(col_expr):
    """
    Заменяет запятую на точку и кастует в DoubleType.
    """
    return F.regexp_replace(F.col(col_expr).cast("string"), ",", ".").cast("double")

# ============================================================
# ЧТЕНИЕ ФАЙЛОВ ИЗ S3
# ============================================================
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

objects = s3.list_objects_v2(Bucket='raw', Prefix='globus_')

if 'Contents' not in objects:
    print("⚠ Нет файлов globus_")
    spark.stop()
    exit(0)

date_created = datetime.now().strftime('%Y-%m-%d')

for obj in objects['Contents']:
    file = obj['Key']
    if not file.endswith('.csv'):
        continue

    file_path = f's3a://raw/{file}'
    print('=' * 100)
    print(f'Обработка: {file_path}')

    # 1. Читаем CSV с автоопределением разделителя
    df = read_csv_with_auto_sep(file_path)
    print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
    print(f"Колонки до переименования: {df.columns}")

    # 2. Переименовываем колонки
    for old_name in df.columns:
        new_name = COLUMN_RENAME_MAP.get(old_name)
        if new_name and new_name != old_name:
            df = df.withColumnRenamed(old_name, new_name)

    # 3. Добавляем служебные поля
    df = df.withColumn('retail_chain', F.lit('Глобус'))
    df = df.withColumn('file_name', F.lit(file[:-4]))

    # Парсим месяц и год из имени файла
    month_str, year_int = parse_month_year_from_filename(file)
    month_int = {
        'Январь': 1, 'Февраль': 2, 'Март': 3, 'Апрель': 4,
        'Май': 5, 'Июнь': 6, 'Июль': 7, 'Август': 8,
        'Сентябрь': 9, 'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
    }.get(month_str, 9)

    df = df.withColumn('year', F.lit(year_int))
    df = df.withColumn('month', F.lit(month_str))
    df = df.withColumn('period', F.lit(f'{year_int}-{month_int:02d}-01').cast('date'))
    df = df.withColumn('created_at', F.lit(date_created).cast('date'))
    df = df.withColumn('updated_at', F.lit(date_created).cast('date'))

    # 4. Нормализация числовых колонок (замена , на . и каст в double)
    numeric_cols = ['sales_quantity', 'sales_amount_rub', 'average_cost_price',
                    'average_sell_price', 'margin_rub']
    for col_name in numeric_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, normalize_number(col_name))
        else:
            df = df.withColumn(col_name, F.lit(None).cast('double'))

    # 5. Если sales_cost_price отсутствует, вычисляем как average_cost_price * sales_quantity
    if 'sales_cost_price' not in df.columns:
        df = df.withColumn(
            'sales_cost_price',
            F.when(
                F.col('average_cost_price').isNotNull() & F.col('sales_quantity').isNotNull(),
                F.round(F.col('average_cost_price') * F.col('sales_quantity'), 2)
            ).otherwise(F.lit(None).cast('double'))
        )
    else:
        # тоже нормализуем
        df = df.withColumn('sales_cost_price', normalize_number('sales_cost_price'))

    # 6. Добавляем недостающие колонки (если их нет)
    for col_name in SILVER_COLUMNS:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast(SILVER_TYPES[col_name]))

    # 7. Выбираем только нужные колонки в правильном порядке
    final_df = df.select(*SILVER_COLUMNS)

    print(f'Финальные колонки: {final_df.columns}')
    print('=' * 100)

    # 8. Проверка на повторную загрузку
    try:
        res = spark.sql(
            f"""
            SELECT COUNT(*)
            FROM iceberg.globus_silver.sales
            WHERE file_name = '{file[:-4]}'
            """
        ).first()
        file_exists = res[0] > 0
    except Exception:
        file_exists = False

    if not file_exists:
        rows = final_df.count()
        print(f'Записываем {rows} строк...')

        final_df.writeTo('iceberg.globus_silver.sales') \
            .partitionedBy('retail_chain', 'year', 'month') \
            .append()

        print(f'✓ {file} → {rows} строк')
    else:
        print(f'⊘ {file} уже есть')

    print('=' * 100)
    print()

print('✅ Обработка Глобус завершена!')
spark.stop()