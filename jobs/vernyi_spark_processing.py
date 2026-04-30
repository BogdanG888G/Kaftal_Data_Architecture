import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder.appName('Iceberg Vernyi ETL').getOrCreate()
print(f"✓ Spark: {spark.version}")

# ============================================================
# КОНСТАНТЫ
# ============================================================
SILVER_COLUMNS = [
    'year', 'month', 'retail_chain',
    'week_num', 'store_code', 'address', 'region_name', 'city_name',
    'product_category_3', 'product_category_4',
    'product_id', 'product_name',
    'vendor', 'manufacturer',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_cost_price', 'average_sell_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'week_num': 'int', 'store_code': 'string', 'address': 'string', 'region_name': 'string', 'city_name': 'string',
    'product_category_3': 'string', 'product_category_4': 'string',
    'product_id': 'string', 'product_name': 'string',
    'vendor': 'string', 'manufacturer': 'string',
    'sales_quantity': 'int', 'sales_amount_rub': 'float', 'sales_cost_price': 'float',
    'average_cost_price': 'float', 'average_sell_price': 'float',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

MAPPING = {
    'Неделя': 'week_num', '№ магазина': 'store_code', 'Адрес': 'address', 'Регион': 'region_name',
    'Группа': 'product_category_3', 'Подгруппа': 'product_category_4',
    'Код товара': 'product_id', 'Наименование товара': 'product_name',
    'Поставщик': 'vendor', 'Производитель': 'manufacturer',
    'Реализация шт.(кг)': 'sales_quantity', 'Реализация Сумма': 'sales_amount_rub',
    'Цена ед.': 'average_sell_price', 'Цена вх.': 'average_cost_price',
}

MONTH_MAPPING = {
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март', 'april': 'Апрель',
    'may': 'Май', 'june': 'Июнь', 'july': 'Июль', 'august': 'Август',
    'september': 'Сентябрь', 'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
}
MONTH_MAPPING_INT = {v: k for k, v in {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}.items()}

s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
objects = s3.list_objects_v2(Bucket='raw', Prefix='vernyi_')

if 'Contents' not in objects:
    print("⚠ Нет файлов vernyi_")
else:
    y, m, d = datetime.now().year, datetime.now().month, datetime.now().day
    date_created = f'{y}-{m:02d}-{d:02d}'

    for obj in objects['Contents']:
        file = obj['Key']
        if not file.endswith('.csv'): continue
        
        file_name = f's3a://raw/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        
        df = spark.read.csv(file_name, sep=';', header=True, inferSchema=False)
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
        print(f'Колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 1: Переименование
        for old_name in df.columns:
            new_name = MAPPING.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)
        
        # ШАГ 2: Служебные
        df = df.withColumn('retail_chain', F.lit('Верный'))
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))
        
        # ШАГ 3: Месяц и год из имени файла
        parts = file.replace('.csv', '').split('_')
        month_str, parsed_year = None, None
        for i, part in enumerate(parts):
            mc = MONTH_MAPPING.get(part.lower())
            if mc:
                month_str = mc
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    parsed_year = int(parts[i + 1])
                break
        
        if month_str is None: month_str = 'Неизвестно'
        month_int = MONTH_MAPPING_INT.get(month_str, 1)
        if parsed_year is None: parsed_year = y
        
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(parsed_year))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', F.lit(period_done))
        
        # ШАГ 4: Город из адреса
        if 'address' in df.columns:
            df = df.withColumn('city_name',
                F.coalesce(
                    F.regexp_extract(df['address'], r'г\.\s*([^,]+)', 1),
                    F.regexp_extract(df['address'], r'г\s+([^,]+)', 1),
                    F.regexp_extract(df['address'], r'^([^,]+)', 1)
                )
            )
        
        # ШАГ 5: Запятые → точки
        for col_name in ['sales_amount_rub', 'average_sell_price', 'average_cost_price']:
            if col_name in df.columns:
                try:
                    df = df.withColumn(col_name, F.regexp_replace(df[col_name].cast('string'), ',', '.'))
                except: pass
        
        # ШАГ 6: Общая себестоимость = средняя * количество
        if 'average_cost_price' in df.columns and 'sales_quantity' in df.columns:
            df = df.withColumn('sales_cost_price',
                F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0),
                     df['average_cost_price'] * df['sales_quantity']))
        
        # ШАГ 7: Недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
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
        
        # ШАГ 9: Запись
        final_df = df.select(*SILVER_COLUMNS)
        
        try:
            res = spark.sql(f"SELECT COUNT(*) FROM iceberg.vernyi_silver.sales WHERE file_name = '{file[:-4]}'").first()
            file_exists = res[0] > 0
        except: file_exists = False
        
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            final_df.writeTo('iceberg.vernyi_silver.sales').partitionedBy('retail_chain', 'year', 'month').append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')
        print('=' * 100)
        print()

print('✅ Обработка Верный завершена!')