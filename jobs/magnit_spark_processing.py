import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName('Iceberg Magnit ETL') \
    .getOrCreate()

print(f"✓ Версия Spark: {spark.version}")

# ============================================================
# КОНСТАНТЫ
# ============================================================
SILVER_COLUMNS = [
    'year', 'month', 'retail_chain', 'store_format', 'region_name', 'city_name',
    'address', 'store_code', 'store_name', 'product_category_2', 'product_category_3',
    'product_category_4', 'product_category_5', 'product_id', 'product_name', 'barcode',
    'brand', 'vendor', 'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_cost_price', 'average_sell_price', 'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'store_format': 'string', 'region_name': 'string', 'city_name': 'string',
    'address': 'string', 'store_code': 'string', 'store_name': 'string',
    'product_category_2': 'string', 'product_category_3': 'string',
    'product_category_4': 'string', 'product_category_5': 'string',
    'product_id': 'string', 'product_name': 'string', 'barcode': 'string',
    'brand': 'string', 'vendor': 'string',
    'sales_quantity': 'int', 'sales_amount_rub': 'float', 'sales_cost_price': 'float',
    'average_cost_price': 'float', 'average_sell_price': 'float',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

COLUMN_RENAME_MAP = {
    'Месяц': 'month_ru', 'Формат': 'store_format',
    'Наименование ТТ': 'store_name', 'Название магазина': 'store_name',
    'Код ТТ': 'store_code', 'Код': 'store_code',
    'Адрес ТТ': 'address', 'Адрес': 'address',
    'Уровень 1': 'level_1', 'Уровень 2': 'level_2', 'Уровень 3': 'level_3', 'Уровень 4': 'level_4',
    'Поставщик': 'vendor', 'Бренд': 'brand',
    'Наименование ТП': 'product_name', 'Наименование': 'product_name',
    'Код ТП': 'product_id', 'Код позиции': 'product_id',
    'ШК': 'barcode', 'Штриховой код': 'barcode',
    'Оборот руб': 'sales_amount_rub', 'Продажи в руб.': 'sales_amount_rub', 'Продажи в руб': 'sales_amount_rub',
    'Оборот шт': 'sales_quantity', 'Продажи в шт.': 'sales_quantity', 'Продажи в шт': 'sales_quantity',
    'Входящая цена': 'sales_cost_price', 'Себестоимость в руб.': 'sales_cost_price',
    'Себестоимсть в руб.': 'sales_cost_price', 'Себестоимость в руб': 'sales_cost_price',
    'Год': 'year_raw', 'Неделя': 'week_raw',
}

LEVEL_RENAME = {
    'level_1': 'product_category_2', 'level_2': 'product_category_3',
    'level_3': 'product_category_4', 'level_4': 'product_category_5',
}

MONTH_MAPPING = {
    'январь': 'Январь', 'февраль': 'Февраль', 'март': 'Март', 'апрель': 'Апрель',
    'май': 'Май', 'июнь': 'Июнь', 'июль': 'Июль', 'август': 'Август',
    'сентябрь': 'Сентябрь', 'октябрь': 'Октябрь', 'ноябрь': 'Ноябрь', 'декабрь': 'Декабрь',
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март', 'april': 'Апрель',
    'may': 'Май', 'june': 'Июнь', 'july': 'Июль', 'august': 'Август',
    'september': 'Сентябрь', 'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
}

MONTH_MAPPING_INT = {
    'Январь': 1, 'Февраль': 2, 'Март': 3, 'Апрель': 4,
    'Май': 5, 'Июнь': 6, 'Июль': 7, 'Август': 8,
    'Сентябрь': 9, 'Октябрь': 10, 'Ноябрь': 11, 'Декабрь': 12
}

# ============================================================
# S3 CLIENT
# ============================================================
s3 = boto3.client('s3', 
                  endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin')

# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket='raw', Prefix='magnit_')

if 'Contents' not in objects:
    print("⚠ Нет файлов magnit_")
else:
    files = objects['Contents']
    y, m, d = datetime.now().year, datetime.now().month, datetime.now().day
    date_created = f'{y}-{m:02d}-{d:02d}'

    for obj in files:
        file = obj['Key']
        if not file.endswith('.csv'): continue
        
        file_name = f's3a://raw/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        print('=' * 100)
        
        df = spark.read.csv(file_name, sep=';', header=True, inferSchema=False)
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
        print(f'Колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 1: Переименование
        for old_name in df.columns:
            new_name = COLUMN_RENAME_MAP.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)
        
        # ШАГ 2: Уровни → product_category
        for old_name, new_name in LEVEL_RENAME.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        # ШАГ 3: Служебные
        df = df.withColumn('retail_chain', F.lit('Магнит'))
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))
        
        # ШАГ 4: Дата из имени файла
        parts = file.replace('.csv', '').split('_')
        month_str, parsed_year = None, None
        
        for i, part in enumerate(parts):
            mc = MONTH_MAPPING.get(part.lower())
            if mc:
                month_str = mc
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    parsed_year = int(parts[i + 1])
                break
        
        if 'month_ru' in df.columns and month_str is None:
            first = df.select('month_ru').first()
            if first and first[0]:
                month_str = MONTH_MAPPING.get(str(first[0]).lower())
            df = df.drop('month_ru')
        
        if month_str is None: month_str = 'Неизвестно'
        month_int = MONTH_MAPPING_INT.get(month_str, 1)
        
        if parsed_year is None and 'year_raw' in df.columns:
            first = df.select('year_raw').first()
            if first and first[0]:
                try: parsed_year = int(str(first[0]))
                except: pass
            df = df.drop('year_raw')
        
        if 'week_raw' in df.columns: df = df.drop('week_raw')
        if parsed_year is None: parsed_year = y
        
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(parsed_year))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', F.lit(period_done))
        
        # ШАГ 5: Город и регион из адреса
        if 'address' in df.columns:
            df = df.withColumn('city_name', 
                F.coalesce(
                    F.regexp_extract(df['address'], r'г\.\s*([^,]+)', 1),
                    F.regexp_extract(df['address'], r'г\s+([^,]+)', 1)
                )
            )
            df = df.withColumn('region_name',
                F.coalesce(
                    F.regexp_extract(df['address'], r'([^,]*область[^,]*)', 1),
                    F.regexp_extract(df['address'], r'([^,]*край[^,]*)', 1),
                    F.regexp_extract(df['address'], r'([^,]*республика[^,]*)', 1),
                    F.regexp_extract(df['address'], r'([^,]*АО[^,]*)', 1)
                )
            )
        
        # Очистка barcode
        if 'barcode' in df.columns:
            df = df.withColumn('barcode', F.regexp_replace(df['barcode'], ', PSEUDOBARCODE', ''))
        
        # ШАГ 6: Запятые → точки
        for col_name in ['sales_amount_rub', 'sales_cost_price']:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.regexp_replace(df[col_name].cast('string'), ',', '.'))
        
        # ШАГ 7: Недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        if remaining: print(f'NULL колонки: {remaining}')
        for col in remaining:
            df = df.withColumn(col, F.lit(None))
        
        # ШАГ 8: Типы
        for col_name in df.columns:
            dtype_str = SILVER_TYPES.get(col_name)
            if dtype_str:
                try:
                    df = df.withColumn(col_name, df[col_name].cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {col_name}: {e}")
        
        print(f'Финальные колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 9: Финальный датафрейм
        final_df = df.select(*SILVER_COLUMNS)
        
        # ШАГ 10: Фильтрация
        final_df = final_df.filter(
            df['sales_quantity'].isNull() | (df['sales_quantity'] > 0)
        ).filter(
            df['sales_amount_rub'].isNull() | (df['sales_amount_rub'] > 0)
        ).filter(
            df['sales_cost_price'].isNull() | (df['sales_cost_price'] > 0)
        )
        
        # ШАГ 11: Средние цены
        final_df = final_df.withColumn(
            'average_cost_price', 
            F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0), 
                 df['sales_cost_price'] / df['sales_quantity'])
        ).withColumn(
            'average_sell_price', 
            F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0), 
                 df['sales_amount_rub'] / df['sales_quantity'])
        )
        
        # ШАГ 12: Запись
        try:
            res = spark.sql(f"SELECT COUNT(*) FROM iceberg.magnit_silver.sales WHERE file_name = '{file[:-4]}'").first()
            file_exists = res[0] > 0
        except:
            file_exists = False
        
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            final_df.writeTo('iceberg.magnit_silver.sales').partitionedBy('retail_chain', 'year', 'month').append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')
        print('=' * 100)
        print()

print('✅ Обработка Магнит завершена!')