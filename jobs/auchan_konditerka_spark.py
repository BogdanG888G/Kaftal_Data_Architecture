import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder \
    .appName('Iceberg Auchan Konditerka ETL') \
    .getOrCreate()

print(f"✓ Версия Spark: {spark.version}")

# ============================================================
# S3 CLIENT
# ============================================================
s3 = boto3.client('s3',
                  endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin'
                 )

# ============================================================
# КОНСТАНТЫ
# ============================================================
SILVER_COLUMNS = [
    'year', 'month', 'month_num', 'retail_chain',
    'segment', 'family',
    'product_id', 'product_name',
    'vendor_code', 'vendor',
    'store', 'format', 'okato', 'city_name', 'address',
    'sales_kg', 'sales_amount_no_vat', 'sales_amount_rub',
    'average_cost_price', 'average_sell_price',
    'sales_quantity', 'margin_rub',
    'losses_rub', 'losses_qty',
    'write_off_pct', 'write_off_rub', 'write_off_qty', 'losses_pct',
    'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'month_num': 'int', 'retail_chain': 'string',
    'segment': 'string', 'family': 'string',
    'product_id': 'string', 'product_name': 'string',
    'vendor_code': 'string', 'vendor': 'string',
    'store': 'string', 'format': 'string', 'okato': 'string',
    'city_name': 'string', 'address': 'string',
    'sales_kg': 'double', 'sales_amount_no_vat': 'double', 'sales_amount_rub': 'double',
    'average_cost_price': 'double', 'average_sell_price': 'double',
    'sales_quantity': 'double', 'margin_rub': 'double',
    'losses_rub': 'double', 'losses_qty': 'double',
    'write_off_pct': 'double', 'write_off_rub': 'double', 'write_off_qty': 'double',
    'losses_pct': 'double',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

# Маппинг колонок Ашана → Silver
# Ключи — точные имена из исходного файла (после .strip())
SILVER_MAPPING = {
    'Месяц': 'month_num',
    'Год': 'year',
    'Сегмент': 'segment',
    'Семья': 'family',
    'Артикул код': 'product_id',
    'Артикул': 'product_name',
    'Поставщик код': 'vendor_code',
    'Поставщик': 'vendor',
    'Магазин': 'store',
    'Формат': 'format',
    'ОКАТО': 'okato',
    'Город': 'city_name',
    'Адрес ТТ': 'address',
    'Продажи, кг': 'sales_kg',
    'Продажи, бНДС': 'sales_amount_no_vat',
    'Продажи, cНДС': 'sales_amount_rub',
    'Ср.цена покупки': 'average_cost_price',
    'Ср.цена продажи': 'average_sell_price',
    'Продажи, шт': 'sales_quantity',
    'Маржа, руб.': 'margin_rub',
    'Потери, руб.': 'losses_rub',
    'Потери,шт': 'losses_qty',
    'Списания, % .': 'write_off_pct',
    'Списания, руб.': 'write_off_rub',
    'Списания, шт.': 'write_off_qty',
    'Потери, % .': 'losses_pct'
}

MONTH_MAPPING = {
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март',
    'april': 'Апрель', 'may': 'Май', 'june': 'Июнь',
    'july': 'Июль', 'august': 'Август', 'september': 'Сентябрь',
    'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь'
}

MONTH_MAPPING_INT = {
    'Январь': 1, 'Февраль': 2, 'Март': 3,
    'Апрель': 4, 'Май': 5, 'Июнь': 6,
    'Июль': 7, 'Август': 8, 'Сентябрь': 9,
    'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
}

# Числовые колонки, где надо менять запятую на точку
NUMERIC_COLS = [
    'sales_kg', 'sales_amount_no_vat', 'sales_amount_rub',
    'average_cost_price', 'average_sell_price',
    'sales_quantity', 'margin_rub',
    'losses_rub', 'losses_qty',
    'write_off_pct', 'write_off_rub', 'write_off_qty', 'losses_pct'
]

# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket='rawkonditerka', Prefix='auchan_')

if 'Contents' not in objects:
    print("⚠ Нет файлов auchan_ в бакете rawkonditerka")
else:
    files = objects['Contents']

    year_cr = datetime.now().year
    month_cr = datetime.now().month
    day_cr = datetime.now().day
    date_created = f'{year_cr}-{month_cr:02d}-{day_cr:02d}'

    for obj in files:
        file = obj['Key']
        if not file.endswith('.csv'):
            continue

        file_name = f's3a://rawkonditerka/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        print('=' * 100)

        # Читаем CSV. Разделитель — табуляция (как в исходнике).
        # Если у тебя ; — поменяй sep='\t' на sep=';'
        df = spark.read.option('header', 'true') \
                       .option('encoding', 'UTF-8') \
                       .csv(file_name, sep=';')

        # Чистим имена колонок от пробелов
        df = df.toDF(*[c.strip() for c in df.columns])

        print(f'✓ Строк до фильтрации: {df.count()}')
        print(f'✓ Колонки в файле: {df.columns}')

        # ШАГ 1: Маппинг колонок
        for column in df.columns:
            new_name = SILVER_MAPPING.get(column)
            if new_name:
                df = df.withColumnRenamed(column, new_name)

        # ШАГ 2: Служебные поля
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))

        # ШАГ 3: Год и месяц из имени файла
        # Пример: auchan_candies_march_2026_part1.csv
        # parts[0]="auchan", parts[1]="candies", parts[2]="march", parts[3]="2026"
        parts = file.split('_')
        month_str = MONTH_MAPPING.get(parts[2].lower())
        month_int = MONTH_MAPPING_INT.get(month_str)
        parsed_year = parts[3].replace('.csv', '')

        # Перезаписываем month и year из имени файла (надёжнее, чем из ячеек)
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(int(parsed_year)))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', F.lit(period_done))

        # ШАГ 4: Retail chain
        df = df.withColumn('retail_chain', F.lit('Ашан'))

        # ШАГ 5: Запятые → точки в числовых колонках
        for col_name in NUMERIC_COLS:
            if col_name in df.columns:
                df = df.withColumn(
                    col_name,
                    F.regexp_replace(F.col(col_name), ',', '.')
                )
                # Пустые строки → null
                df = df.withColumn(
                    col_name,
                    F.when(F.trim(F.col(col_name)) == '', None).otherwise(F.col(col_name))
                )

        # ШАГ 6: Добавляем недостающие колонки как null
        remaining_cols = set(SILVER_COLUMNS) - set(df.columns)
        for column in remaining_cols:
            df = df.withColumn(column, F.lit(None))

        # ШАГ 7: Приведение типов
        for column in df.columns:
            dtype_str = SILVER_TYPES.get(column)
            if dtype_str:
                try:
                    df = df.withColumn(column, F.col(column).cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {column}: {e}")

        # ШАГ 8: Финальный DataFrame в правильном порядке колонок
        final_df = df.select(*SILVER_COLUMNS)

        # ШАГ 9: МЯГКАЯ фильтрация — оставляем строки с продажами
        # (либо в шт, либо в кг, либо в рублях > 0)
        final_df = final_df.filter(
            (F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0)) |
            (F.col('sales_kg').isNotNull() & (F.col('sales_kg') > 0)) |
            (F.col('sales_amount_rub').isNotNull() & (F.col('sales_amount_rub') > 0))
        )

        # ШАГ 10: Запись в Iceberg
        TARGET_TABLE = 'iceberg.konditerka_silver.auchan_sales'

        try:
            res = spark.sql(
                f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE file_name = '{file[:-4]}'"
            ).first()
            file_exists = res[0] > 0
        except:
            file_exists = False

        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.konditerka_silver")
            final_df.writeTo(TARGET_TABLE) \
                    .partitionedBy('retail_chain', 'year', 'month') \
                    .append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть, пропускаем')

        print('=' * 100)
        print()

print('✅ Обработка Ашан Кондитерка завершена!')

spark.stop()