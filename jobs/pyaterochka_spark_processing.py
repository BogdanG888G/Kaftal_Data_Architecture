import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import requests
import re as re_m

spark = SparkSession.builder.appName('Iceberg Pyaterochka ETL').getOrCreate()
print(f"✓ Spark: {spark.version}")

def cleanup():
    BASE = "http://iceberg-rest:8181/v1"
    try: requests.delete(f"{BASE}/namespaces/pyaterochka_silver/tables/sales?purgeRequested=true")
    except: pass
    try:
        s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
        ct = None
        while True:
            kw = {'Bucket': 'warehouse', 'Prefix': 'pyaterochka_silver/'}
            if ct: kw['ContinuationToken'] = ct
            objs = s3.list_objects_v2(**kw)
            if 'Contents' in objs:
                for o in objs['Contents']: s3.delete_object(Bucket='warehouse', Key=o['Key'])
            if not objs.get('IsTruncated'): break
            ct = objs.get('NextContinuationToken')
    except: pass

try:
    spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.pyaterochka_silver')
    spark.sql('''
        CREATE TABLE IF NOT EXISTS iceberg.pyaterochka_silver.sales (
            year INT, month STRING, retail_chain STRING,
            product_category_3 STRING, base_type STRING, product_category_4 STRING,
            vendor STRING, brand STRING, product_name STRING, product_uni_name STRING,
            weight_grams STRING, flavor STRING,
            sales_quantity INT, sales_amount_rub FLOAT, sales_cost_price FLOAT, sales_tons FLOAT,
            average_cost_price FLOAT, average_sell_price FLOAT,
            file_name STRING, created_at DATE, updated_at DATE, period DATE
        ) USING iceberg PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/pyaterochka_silver/sales'
    ''')
    print("✓ Таблица готова\n")
except Exception as e:
    print(f"⚠ {e}")
    cleanup()
    spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.pyaterochka_silver')
    spark.sql('''
        CREATE TABLE iceberg.pyaterochka_silver.sales (
            year INT, month STRING, retail_chain STRING,
            product_category_3 STRING, base_type STRING, product_category_4 STRING,
            vendor STRING, brand STRING, product_name STRING, product_uni_name STRING,
            weight_grams STRING, flavor STRING,
            sales_quantity INT, sales_amount_rub FLOAT, sales_cost_price FLOAT, sales_tons FLOAT,
            average_cost_price FLOAT, average_sell_price FLOAT,
            file_name STRING, created_at DATE, updated_at DATE, period DATE
        ) USING iceberg PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/pyaterochka_silver/sales'
    ''')
    print("✓ Таблица создана\n")

SILVER_COLUMNS = [
    'year', 'month', 'retail_chain',
    'product_category_3', 'base_type', 'product_category_4',
    'vendor', 'brand', 'product_name', 'product_uni_name',
    'weight_grams', 'flavor',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price', 'sales_tons',
    'average_cost_price', 'average_sell_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'product_category_3': 'string', 'base_type': 'string', 'product_category_4': 'string',
    'vendor': 'string', 'brand': 'string', 'product_name': 'string', 'product_uni_name': 'string',
    'weight_grams': 'string', 'flavor': 'string',
    'sales_quantity': 'int', 'sales_amount_rub': 'float', 'sales_cost_price': 'float', 'sales_tons': 'float',
    'average_cost_price': 'float', 'average_sell_price': 'float',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

MAPPING = {
    'Период': 'period_raw', 'Сеть': 'retail_chain_raw',
    'Категория': 'product_category_3', 'Тип основы': 'base_type',
    'Поставщики': 'vendor', 'Бренды': 'brand',
    'Наименование': 'product_name', 'УНИ Наименование': 'product_uni_name',
    'Граммовка': 'weight_grams', 'Вкусы': 'flavor',
    'Продажи, шт': 'sales_quantity', 'Продажи, руб ': 'sales_amount_rub',
    'Продажи, тонн ': 'sales_tons', 'Себест., руб ': 'sales_cost_price',
}

MONTH_MAPPING = {
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март', 'april': 'Апрель',
    'may': 'Май', 'june': 'Июнь', 'july': 'Июль', 'august': 'Август',
    'september': 'Сентябрь', 'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
    'ceptember': 'Сентябрь',
}
MONTH_MAPPING_INT = {v: k for k, v in {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}.items()}
SHORT_MONTH = {'jan': 'january', 'feb': 'february', 'mar': 'march', 'apr': 'april', 'may': 'may', 'jun': 'june', 'jul': 'july', 'aug': 'august', 'sep': 'september', 'oct': 'october', 'nov': 'november', 'dec': 'december'}

s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
objects = s3.list_objects_v2(Bucket='raw', Prefix='pyaterochka_')

if 'Contents' not in objects:
    print("⚠ Нет файлов pyaterochka_")
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
        df = df.withColumn('retail_chain', F.lit('Пятерочка'))
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))
        
        # ШАГ 3: Месяц и год
        parts = file.replace('.csv', '').split('_')
        month_str, parsed_year = None, None
        for i, part in enumerate(parts):
            mc = MONTH_MAPPING.get(part.lower())
            if mc:
                month_str = mc
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    parsed_year = int(parts[i + 1])
                break
        
        if 'period_raw' in df.columns:
            first = df.select('period_raw').first()
            if first and first[0]:
                p = str(first[0])
                mm = re_m.search(r'-([A-Za-z]{3})', p)
                if mm:
                    m3 = mm.group(1).lower()
                    full = SHORT_MONTH.get(m3, m3)
                    if month_str is None: month_str = MONTH_MAPPING.get(full)
                ym = re_m.search(r'(\d{2})-', p)
                if ym and parsed_year is None: parsed_year = 2000 + int(ym.group(1))
            df = df.drop('period_raw')
        
        if 'retail_chain_raw' in df.columns: df = df.drop('retail_chain_raw')
        
        if month_str is None: month_str = 'Неизвестно'
        month_int = MONTH_MAPPING_INT.get(month_str, 1)
        if parsed_year is None: parsed_year = y
        
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(parsed_year))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', F.lit(period_done))
        
        # ШАГ 4: Категория 4 из base_type
        if 'base_type' in df.columns:
            df = df.withColumn('product_category_4', df['base_type'])
        
        # ШАГ 5: Запятые → точки
        for col_name in df.columns:
            if col_name in SILVER_TYPES and SILVER_TYPES[col_name] in ('float', 'int'):
                try:
                    df = df.withColumn(col_name, F.regexp_replace(df[col_name].cast('string'), ',', '.'))
                except: pass
        
        # ШАГ 6: average_*
        if 'sales_quantity' in df.columns and 'sales_cost_price' in df.columns:
            df = df.withColumn('average_cost_price',
                F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0),
                     df['sales_cost_price'] / df['sales_quantity']))
        
        if 'sales_quantity' in df.columns and 'sales_amount_rub' in df.columns:
            df = df.withColumn('average_sell_price',
                F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0),
                     df['sales_amount_rub'] / df['sales_quantity']))
        
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
                    print(f"  ⚠ {col_name} → {dtype_str}: {e}")
        
        print(f'Финальные колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 9: Запись
        final_df = df.select(*SILVER_COLUMNS)
        
        try:
            res = spark.sql(f"SELECT COUNT(*) FROM iceberg.pyaterochka_silver.sales WHERE file_name = '{file[:-4]}'").first()
            file_exists = res[0] > 0
        except: file_exists = False
        
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            final_df.writeTo('iceberg.pyaterochka_silver.sales').partitionedBy('retail_chain', 'year', 'month').append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')
        print('=' * 100)
        print()

print('✅ Обработка Пятерочка завершена!')