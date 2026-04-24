import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import requests
import re as re_module

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName('Iceberg Aushan ETL') \
    .getOrCreate()

print(f"✓ Версия Spark: {spark.version}")

# ============================================================
# ОЧИСТКА КАТАЛОГА
# ============================================================
def cleanup_aushan_catalog():
    BASE = "http://iceberg-rest:8181/v1"
    NS = "aushan_silver"
    TABLE = "sales"
    
    print("🔧 Чиним каталог Ашан...")
    try:
        r = requests.delete(f"{BASE}/namespaces/{NS}/tables/{TABLE}?purgeRequested=true")
        if r.status_code in [200, 204]:
            print(f"  ✓ Таблица удалена")
    except Exception as e:
        print(f"  ⚠ {e}")
    
    try:
        s3 = boto3.client('s3', endpoint_url='http://minio:9000',
                          aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
        deleted = 0
        ct = None
        while True:
            kw = {'Bucket': 'warehouse', 'Prefix': 'aushan_silver/'}
            if ct:
                kw['ContinuationToken'] = ct
            objs = s3.list_objects_v2(**kw)
            if 'Contents' in objs:
                for obj in objs['Contents']:
                    s3.delete_object(Bucket='warehouse', Key=obj['Key'])
                    deleted += 1
            if not objs.get('IsTruncated'):
                break
            ct = objs.get('NextContinuationToken')
        print(f"  ✓ Удалено {deleted} объектов из MinIO")
    except Exception as e:
        print(f"  ⚠ MinIO: {e}")

# ============================================================
# СОЗДАНИЕ ТАБЛИЦЫ
# ============================================================
try:
    spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.aushan_silver')
    spark.sql('''
        CREATE TABLE IF NOT EXISTS iceberg.aushan_silver.sales (
            year INT, month STRING, retail_chain STRING, store_format STRING,
            region_name STRING, city_name STRING, address STRING, store_code STRING,
            product_segment STRING, family_code STRING, family_name STRING,
            product_id STRING, product_name STRING, vendor_code STRING, vendor STRING,
            sales_quantity INT, sales_amount_rub FLOAT, sales_cost_price FLOAT, sales_kg FLOAT,
            average_cost_price FLOAT, average_sell_price FLOAT, margin_rub FLOAT,
            write_off_rub FLOAT, write_off_qty INT, loss_rub FLOAT, loss_qty INT, promo_sales_rub FLOAT,
            week_num INT, file_name STRING, created_at DATE, updated_at DATE, period DATE
        )
        USING iceberg PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/aushan_silver/sales'
    ''')
    print("✓ Таблица готова\n")
except Exception as e:
    print(f"⚠ Ошибка: {e}")
    cleanup_aushan_catalog()
    spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.aushan_silver')
    spark.sql('''
        CREATE TABLE iceberg.aushan_silver.sales (
            year INT, month STRING, retail_chain STRING, store_format STRING,
            region_name STRING, city_name STRING, address STRING, store_code STRING,
            product_segment STRING, family_code STRING, family_name STRING,
            product_id STRING, product_name STRING, vendor_code STRING, vendor STRING,
            sales_quantity INT, sales_amount_rub FLOAT, sales_cost_price FLOAT, sales_kg FLOAT,
            average_cost_price FLOAT, average_sell_price FLOAT, margin_rub FLOAT,
            write_off_rub FLOAT, write_off_qty INT, loss_rub FLOAT, loss_qty INT, promo_sales_rub FLOAT,
            week_num INT, file_name STRING, created_at DATE, updated_at DATE, period DATE
        )
        USING iceberg PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/aushan_silver/sales'
    ''')
    print("✓ Таблица создана\n")

# ============================================================
# КОНСТАНТЫ
# ============================================================
SILVER_COLUMNS = [
    'year', 'month', 'retail_chain', 'store_format', 'region_name', 'city_name',
    'address', 'store_code', 'product_segment', 'family_code', 'family_name',
    'product_id', 'product_name', 'vendor_code', 'vendor',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price', 'sales_kg',
    'average_cost_price', 'average_sell_price', 'margin_rub',
    'write_off_rub', 'write_off_qty', 'loss_rub', 'loss_qty', 'promo_sales_rub',
    'week_num', 'file_name', 'created_at', 'updated_at', 'period'
]

# Типы как строки для .cast()
SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'store_format': 'string', 'region_name': 'string', 'city_name': 'string',
    'address': 'string', 'store_code': 'string',
    'product_segment': 'string', 'family_code': 'string', 'family_name': 'string',
    'product_id': 'string', 'product_name': 'string',
    'vendor_code': 'string', 'vendor': 'string',
    'sales_quantity': 'int', 'sales_amount_rub': 'float', 'sales_cost_price': 'float',
    'sales_kg': 'float', 'average_cost_price': 'float', 'average_sell_price': 'float',
    'margin_rub': 'float', 'write_off_rub': 'float', 'write_off_qty': 'int',
    'loss_rub': 'float', 'loss_qty': 'int', 'promo_sales_rub': 'float',
    'week_num': 'int', 'file_name': 'string',
    'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

COLUMN_RENAME_MAP = {
    'Дата': 'date_raw', 'Сегмент': 'product_segment',
    'СЕМЬЯ': 'family_code', 'НАЗВАНИЕ СЕМЬИ': 'family_name',
    'АРТИКУЛ': 'product_id', 'НАИМЕНОВАНИЕ': 'product_name',
    'ПОСТАВЩИК': 'vendor_code', 'НАИМЕНОВАНИЕ ПОСТАВЩИКА': 'vendor',
    'Магазин': 'store_code', 'Город': 'city_raw', 'Адрес': 'address', 'Формат': 'store_format',
    'Месяц': 'month_raw',
    'Ср.цена продажи': 'average_sell_price', 'Списания, руб.': 'write_off_rub',
    'Списания, шт.': 'write_off_qty', 'Продажи, c НДС': 'sales_amount_rub_extra',
    'Продажи, кг': 'sales_kg', 'Продажи, шт': 'sales_quantity',
    'Ср.цена покупки': 'average_cost_price', 'Маржа, руб.': 'margin_rub',
    'Потери, руб.': 'loss_rub', 'Потери,шт': 'loss_qty',
    'Промо Продажи, c НДС': 'promo_sales_rub',
}

MONTH_MAPPING = {
    'январь': 'Январь', 'февраль': 'Февраль', 'март': 'Март',
    'апрель': 'Апрель', 'май': 'Май', 'июнь': 'Июнь',
    'июль': 'Июль', 'август': 'Август', 'сентябрь': 'Сентябрь',
    'октябрь': 'Октябрь', 'ноябрь': 'Ноябрь', 'декабрь': 'Декабрь',
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март',
    'april': 'Апрель', 'may': 'Май', 'june': 'Июнь',
    'july': 'Июль', 'august': 'Август', 'september': 'Сентябрь',
    'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
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
objects = s3.list_objects_v2(Bucket='raw', Prefix='aushan_')

if 'Contents' not in objects:
    print("⚠ Нет файлов aushan_ в бакете raw")
else:
    files = objects['Contents']
    year_cr, month_cr, day_cr = datetime.now().year, datetime.now().month, datetime.now().day
    date_created = f'{year_cr}-{month_cr:02d}-{day_cr:02d}'

    for obj in files:
        file = obj['Key']
        if not file.endswith('.csv'):
            continue
        
        file_name = f's3a://raw/{file}'
        
        print('=' * 100)
        print(f'Обработка: {file_name}')
        print('=' * 100)
        
        df = spark.read.csv(file_name, sep=';', header=True, inferSchema=False)
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
        print(f'Исходные колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 1: Переименование
        for old_name in df.columns:
            new_name = COLUMN_RENAME_MAP.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)
        
        # ШАГ 2: Служебные
        df = df.withColumn('retail_chain', lit('Ашан'))
        df = df.withColumn('file_name', lit(file[:-5]))  # -5 = ".csv"
        df = df.withColumn('created_at', lit(date_created))
        df = df.withColumn('updated_at', lit(date_created))
        
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
        
        if 'month_raw' in df.columns:
            first_month = df.select('month_raw').first()
            if first_month and first_month[0]:
                month_raw_val = str(first_month[0])
                for m_name in MONTH_MAPPING_INT:
                    if m_name in month_raw_val:
                        if month_str is None:
                            month_str = m_name
                        ym = re_module.search(r'(\d{4})', month_raw_val)
                        if ym and parsed_year is None:
                            parsed_year = int(ym.group(1))
                        break
            df = df.drop('month_raw')
        
        if month_str is None:
            month_str = 'Неизвестно'
        month_int = MONTH_MAPPING_INT.get(month_str, 1)
        if parsed_year is None:
            parsed_year = year_cr
        
        df = df.withColumn('month', lit(month_str))
        df = df.withColumn('year', lit(parsed_year))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', lit(period_done))
        
        # ШАГ 4: Номер недели из date_raw
        if 'date_raw' in df.columns:
            # Используем явно pyspark.sql.functions.regexp_extract
            df = df.withColumn('week_num_str', 
                regexp_extract(col('date_raw'), r'(\d+)\.\d+', 1))
            df = df.withColumn('week_num', 
                df['week_num_str'].cast('int'))
            df = df.drop('date_raw', 'week_num_str')
        
        # ШАГ 5: Регион + город
        if 'city_raw' in df.columns:
            df = df.withColumn('region_name', col('city_raw'))
            df = df.drop('city_raw')
        
        if 'address' in df.columns:
            df = df.withColumn('city_name',
                coalesce(
                    regexp_extract(col('address'), r'г\.\s*([^,]+)', 1),
                    regexp_extract(col('address'), r'г\s+([^,]+)', 1)
                )
            )
            df = df.withColumn('city_name',
                coalesce(col('city_name'), col('region_name'))
            )
        
        # ШАГ 6: Запятые → точки
        for col_name in ['average_sell_price', 'average_cost_price', 'margin_rub',
                         'write_off_rub', 'loss_rub', 'promo_sales_rub', 'sales_kg']:
            if col_name in df.columns:
                df = df.withColumn(col_name, 
                    regexp_replace(col(col_name).cast('string'), ',', '.')
                )
        
        # ШАГ 7: Расчёт общих сумм
        if 'average_sell_price' in df.columns and 'sales_quantity' in df.columns:
            df = df.withColumn('sales_amount_rub',
                when(col('sales_quantity').isNotNull() & (col('sales_quantity') > 0),
                     col('average_sell_price').cast('double') * col('sales_quantity').cast('double'))
            )
        
        if 'average_cost_price' in df.columns and 'sales_quantity' in df.columns:
            df = df.withColumn('sales_cost_price',
                when(col('sales_quantity').isNotNull() & (col('sales_quantity') > 0),
                     col('average_cost_price').cast('double') * col('sales_quantity').cast('double'))
            )
        
        if 'sales_amount_rub_extra' in df.columns:
            df = df.drop('sales_amount_rub_extra')
        
        # ШАГ 8: Добавить недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        if remaining:
            print(f'NULL колонки: {remaining}')
        for col in remaining:
            df = df.withColumn(col, lit(None))
        
        # ШАГ 9: Приведение типов (один раз!)
        for col_name in df.columns:
            dtype_str = SILVER_TYPES.get(col_name)
            if dtype_str:
                try:
                    df = df.withColumn(col_name, col(col_name).cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {col_name} → {dtype_str}: {e}")
        
        print(f'Финальные колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 10: Финальный датафрейм
        final_df = df.select(*SILVER_COLUMNS)
        
        # ШАГ 11: Запись
        try:
            res = spark.sql(f'''
                SELECT COUNT(*) FROM iceberg.aushan_silver.sales 
                WHERE file_name = '{file[:-5]}'
            ''').first()
            file_exists = res[0] > 0
        except:
            file_exists = False
        
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            final_df.writeTo('iceberg.aushan_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже в таблице')
        
        print('=' * 100)
        print()

print('✅ Обработка Ашан завершена!')