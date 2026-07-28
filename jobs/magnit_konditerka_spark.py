import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder \
    .appName('Iceberg Magnit Konditerka ETL') \
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
    'store_code', 'store_name', 'address', 'region_name',
    'format', 'subformat',
    'category_level_0', 'category_level_1', 'category_level_2',
    'category_level_3', 'category_level_4',
    'product_id', 'product_name', 'brand', 'vendor',
    'weight', 'unit', 'barcode',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_sell_price', 'average_cost_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'month_num': 'int', 'retail_chain': 'string',
    'store_code': 'string', 'store_name': 'string', 'address': 'string', 'region_name': 'string',
    'format': 'string', 'subformat': 'string',
    'category_level_0': 'string', 'category_level_1': 'string', 'category_level_2': 'string',
    'category_level_3': 'string', 'category_level_4': 'string',
    'product_id': 'string', 'product_name': 'string', 'brand': 'string', 'vendor': 'string',
    'weight': 'double', 'unit': 'string', 'barcode': 'string',
    'sales_quantity': 'double', 'sales_amount_rub': 'double', 'sales_cost_price': 'double',
    'average_sell_price': 'double', 'average_cost_price': 'double',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

# Маппинг колонок Магнита → Silver
SILVER_MAPPING = {
    'Месяц': 'month_num',
    'Год': 'year',
    'Код ТТ': 'store_code',
    'Наименование магазина': 'store_name',
    'Адрес': 'address',
    'Регион': 'region_name',
    'Формат': 'format',
    'Субформат': 'subformat',
    'Уровень 0': 'category_level_0',
    'Уровень 1': 'category_level_1',
    'Уровень 2': 'category_level_2',
    'Уровень 3': 'category_level_3',
    'Уровень 4': 'category_level_4',
    'Код позиции': 'product_id',
    'Наименование позиции': 'product_name',
    'Бренд': 'brand',
    'Поставщик': 'vendor',
    'Вес': 'weight',
    'ЕИ': 'unit',
    'Штрих код': 'barcode',
    'Продажи в шт.': 'sales_quantity',
    'Продажи в руб. с НДС': 'sales_amount_rub',
    'Себестоимость в руб. с НДС': 'sales_cost_price'
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
    'weight', 'sales_quantity', 'sales_amount_rub', 'sales_cost_price'
]

# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket='rawkonditerka', Prefix='magnit_')

if 'Contents' not in objects:
    print("⚠ Нет файлов magnit_ в бакете rawkonditerka")
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
        # Пример: magnit_candies_march_2026_part1.csv
        # parts[0]="magnit", parts[1]="candies", parts[2]="march", parts[3]="2026"
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
        df = df.withColumn('retail_chain', F.lit('Магнит'))

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

        # ШАГ 9: Рассчитываем средние цены (их в исходнике нет)
        final_df = final_df.withColumn(
            'average_sell_price',
            F.when(
                F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0),
                F.col('sales_amount_rub') / F.col('sales_quantity')
            )
        ).withColumn(
            'average_cost_price',
            F.when(
                F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0),
                F.col('sales_cost_price') / F.col('sales_quantity')
            )
        )

        # ШАГ 10: МЯГКАЯ фильтрация — оставляем строки с продажами > 0
        final_df = final_df.filter(
            (F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0)) |
            (F.col('sales_amount_rub').isNotNull() & (F.col('sales_amount_rub') > 0))
        )

        # ШАГ 11: Запись в Iceberg
        TARGET_TABLE = 'iceberg.konditerka_silver.magnit_sales'

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
                    .append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть, пропускаем')

        print('=' * 100)
        print()

print('✅ Обработка Магнит Кондитерка завершена!')

spark.stop()