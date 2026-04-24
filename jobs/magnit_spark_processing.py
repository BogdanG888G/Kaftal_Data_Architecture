import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime
import requests

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName('Iceberg Magnit ETL') \
    .getOrCreate()

print(f"✓ Версия Spark: {spark.version}")

# ============================================================
# ОЧИСТКА ПОВРЕЖДЕННОЙ ТАБЛИЦЫ
# ============================================================
def cleanup_magnit_catalog():
    BASE = "http://iceberg-rest:8181/v1"
    NAMESPACE = "magnit_silver"
    TABLE = "sales"
    
    print("🔧 Чиним каталог Магнит...")
    
    try:
        r = requests.delete(f"{BASE}/namespaces/{NAMESPACE}/tables/{TABLE}?purgeRequested=true")
        if r.status_code in [200, 204]:
            print(f"  ✓ Таблица удалена из каталога")
        elif r.status_code == 404:
            print(f"  ⊘ Таблица не найдена")
    except Exception as e:
        print(f"  ⚠ Ошибка: {e}")
    
    try:
        s3 = boto3.client('s3',
            endpoint_url='http://minio:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin'
        )
        deleted = 0
        continuation_token = None
        while True:
            list_kwargs = {'Bucket': 'warehouse', 'Prefix': 'magnit_silver/'}
            if continuation_token:
                list_kwargs['ContinuationToken'] = continuation_token
            objects = s3.list_objects_v2(**list_kwargs)
            if 'Contents' in objects:
                for obj in objects['Contents']:
                    s3.delete_object(Bucket='warehouse', Key=obj['Key'])
                    deleted += 1
            if not objects.get('IsTruncated'):
                break
            continuation_token = objects.get('NextContinuationToken')
        print(f"  ✓ Удалено {deleted} объектов из MinIO")
    except Exception as e:
        print(f"  ⚠ Ошибка MinIO: {e}")

# ============================================================
# СОЗДАНИЕ ТАБЛИЦЫ
# ============================================================
try:
    spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.magnit_silver')
    spark.sql('''
        CREATE TABLE IF NOT EXISTS iceberg.magnit_silver.sales (
            year                INT,
            month               STRING,
            retail_chain        STRING,
            store_format        STRING,
            region_name         STRING,
            city_name           STRING,
            address             STRING,
            store_code          STRING,
            store_name          STRING,
            product_category_2  STRING,
            product_category_3  STRING,
            product_category_4  STRING,
            product_category_5  STRING,
            product_id          STRING,
            product_name        STRING,
            barcode             STRING,
            brand               STRING,
            vendor              STRING,
            sales_quantity      INT,
            sales_amount_rub    FLOAT,
            sales_cost_price    FLOAT,
            average_cost_price  FLOAT,
            average_sell_price  FLOAT,
            file_name           STRING,
            created_at          DATE,
            updated_at          DATE,
            period              DATE
        )
        USING iceberg
        PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/magnit_silver/sales'
    ''')
    print("✓ Таблица готова\n")
except Exception as e:
    print(f"⚠ Ошибка: {e}")
    cleanup_magnit_catalog()
    spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.magnit_silver')
    spark.sql('''
        CREATE TABLE iceberg.magnit_silver.sales (
            year                INT,
            month               STRING,
            retail_chain        STRING,
            store_format        STRING,
            region_name         STRING,
            city_name           STRING,
            address             STRING,
            store_code          STRING,
            store_name          STRING,
            product_category_2  STRING,
            product_category_3  STRING,
            product_category_4  STRING,
            product_category_5  STRING,
            product_id          STRING,
            product_name        STRING,
            barcode             STRING,
            brand               STRING,
            vendor              STRING,
            sales_quantity      INT,
            sales_amount_rub    FLOAT,
            sales_cost_price    FLOAT,
            average_cost_price  FLOAT,
            average_sell_price  FLOAT,
            file_name           STRING,
            created_at          DATE,
            updated_at          DATE,
            period              DATE
        )
        USING iceberg
        PARTITIONED BY (retail_chain, year, month)
        LOCATION 's3://warehouse/magnit_silver/sales'
    ''')
    print("✓ Таблица создана после очистки\n")

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

# Маппинг: оригинальное имя (может быть с пробелами) → новое имя
COLUMN_RENAME_MAP = {
    # V1 формат (бывшие xlsx)
    'Месяц': 'month_ru',
    'Формат': 'store_format',
    'Наименование ТТ': 'store_name',
    'Название магазина': 'store_name',
    'Код ТТ': 'store_code',
    'Код': 'store_code',
    'Адрес ТТ': 'address',
    'Адрес': 'address',
    'Уровень 1': 'level_1',
    'Уровень 2': 'level_2',
    'Уровень 3': 'level_3',
    'Уровень 4': 'level_4',
    'Поставщик': 'vendor',
    'Бренд': 'brand',
    'Наименование ТП': 'product_name',
    'Наименование': 'product_name',
    'Код ТП': 'product_id',
    'Код позиции': 'product_id',
    'ШК': 'barcode',
    'Штриховой код': 'barcode',
    'Оборот руб': 'sales_amount_rub',
    'Продажи в руб.': 'sales_amount_rub',
    'Продажи в руб': 'sales_amount_rub',
    'Оборот шт': 'sales_quantity',
    'Продажи в шт.': 'sales_quantity',
    'Продажи в шт': 'sales_quantity',
    'Входящая цена': 'sales_cost_price',
    'Себестоимость в руб.': 'sales_cost_price',
    'Себестоимсть в руб.': 'sales_cost_price',
    'Себестоимость в руб': 'sales_cost_price',
    # V2 формат (бывшие xlsb)
    'Год': 'year_raw',
    'Неделя': 'week_raw',
    # Чистые названия (если уже переименованы)
    'month_ru': 'month_ru',
    'store_format': 'store_format',
    'store_name': 'store_name',
    'store_code': 'store_code',
    'address': 'address',
    'level_1': 'level_1',
    'level_2': 'level_2',
    'level_3': 'level_3',
    'level_4': 'level_4',
    'vendor': 'vendor',
    'brand': 'brand',
    'product_name': 'product_name',
    'product_id': 'product_id',
    'barcode': 'barcode',
    'sales_amount_rub': 'sales_amount_rub',
    'sales_quantity': 'sales_quantity',
    'sales_cost_price': 'sales_cost_price',
    'year_raw': 'year_raw',
    'week_raw': 'week_raw',
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
                  aws_secret_access_key='minioadmin'
                 )

# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket='raw', Prefix='magnit_')

if 'Contents' not in objects:
    print("⚠ Нет файлов magnit_ в бакете raw")
else:
    files = objects['Contents']

    year_cr = datetime.now().year
    month_cr = datetime.now().month
    day_cr = datetime.now().day
    date_created = f'{year_cr}-{month_cr:02d}-{day_cr:02d}'

    for obj in files:
        
        file = obj['Key']
        
        # Пропускаем не-CSV
        if not file.endswith('.csv'):
            print(f'⊘ Пропускаем: {file}')
            continue
        
        file_name = f's3a://raw/{file}'
        
        print('=' * 100)
        print(f'Обработка: {file_name}')
        print('=' * 100)
        
        # Читаем CSV
        df = spark.read.csv(file_name, sep=';', header=True, inferSchema=False)
        
        print(f'✓ Прочитано строк: {df.count()}')
        print(f'Исходные колонки: {df.columns}')
        print('=' * 100)
        
        # ============================================================
        # ШАГ 1: Переименование колонок (через цикл, а не selectExpr)
        # ============================================================
        for old_name in df.columns:
            new_name = COLUMN_RENAME_MAP.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)
        
        print(f'Колонки после переименования: {df.columns}')
        
        # ============================================================
        # ШАГ 2: Уровни категорий → product_category
        # ============================================================
        level_rename = {
            'level_1': 'product_category_2',
            'level_2': 'product_category_3',
            'level_3': 'product_category_4',
            'level_4': 'product_category_5',
        }
        for old_name, new_name in level_rename.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        # ============================================================
        # ШАГ 3: Служебные колонки
        # ============================================================
        df = df.withColumn('retail_chain', lit('Магнит'))
        df = df.withColumn('file_name', lit(file[:-4]))
        df = df.withColumn('created_at', lit(date_created))
        df = df.withColumn('updated_at', lit(date_created))
        
        # ============================================================
        # ШАГ 4: Парсинг даты из имени файла
        # ============================================================
        parts = file.replace('.csv', '').split('_')
        
        month_str = None
        parsed_year = None
        
        for i, part in enumerate(parts):
            month_candidate = MONTH_MAPPING.get(part.lower())
            if month_candidate:
                month_str = month_candidate
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    parsed_year = int(parts[i + 1])
                break
        
        # Если месяц есть в данных
        if 'month_ru' in df.columns and month_str is None:
            first_month = df.select('month_ru').first()
            if first_month and first_month[0]:
                month_str = MONTH_MAPPING.get(str(first_month[0]).lower())
            df = df.drop('month_ru')
        
        if month_str is None:
            print(f"⚠ Не удалось определить месяц из файла {file}")
            month_str = 'Неизвестно'
        
        month_int = MONTH_MAPPING_INT.get(month_str, 1)
        
        # Если год есть в данных
        if parsed_year is None and 'year_raw' in df.columns:
            first_year = df.select('year_raw').first()
            if first_year and first_year[0]:
                try:
                    parsed_year = int(str(first_year[0]))
                except:
                    pass
            df = df.drop('year_raw')
        
        if 'week_raw' in df.columns:
            df = df.drop('week_raw')
        
        if parsed_year is None:
            parsed_year = year_cr
        
        df = df.withColumn('month', lit(month_str))
        df = df.withColumn('year', lit(parsed_year))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', lit(period_done))
        
        # ============================================================
        # ШАГ 5: Извлечение города и региона из адреса
        # ============================================================
        if 'address' in df.columns:
            df = df.withColumn('city_name', 
                coalesce(
                    regexp_extract(col('address'), r'г\.\s*([^,]+)', 1),
                    regexp_extract(col('address'), r'г\s+([^,]+)', 1)
                )
            )
            df = df.withColumn('region_name',
                coalesce(
                    regexp_extract(col('address'), r'([^,]*область[^,]*)', 1),
                    regexp_extract(col('address'), r'([^,]*край[^,]*)', 1),
                    regexp_extract(col('address'), r'([^,]*республика[^,]*)', 1),
                    regexp_extract(col('address'), r'([^,]*АО[^,]*)', 1)
                )
            )
        
        # Очистка barcode
        if 'barcode' in df.columns:
            df = df.withColumn('barcode', 
                regexp_replace(col('barcode'), ', PSEUDOBARCODE', '')
            )
        
        # ============================================================
        # ШАГ 6: Замена запятых на точки
        # ============================================================
        for col_name in ['sales_amount_rub', 'sales_cost_price']:
            if col_name in df.columns:
                df = df.withColumn(col_name, 
                    regexp_replace(col(col_name).cast('string'), ',', '.')
                )
        
        # ============================================================
        # ШАГ 7: Недостающие колонки
        # ============================================================
        remaining_cols = set(SILVER_COLUMNS) - set(df.columns)
        print(f'Добавляем NULL колонки: {remaining_cols}')
        
        for column in remaining_cols:
            df = df.withColumn(column, lit(None))
        
        # ============================================================
        # ШАГ 8: Приведение типов
        # ============================================================
        for column in df.columns:
            dtype = SILVER_TYPES.get(column)
            if dtype:
                try:
                    df = df.withColumn(column, col(column).cast(dtype))
                except Exception as e:
                    print(f"  ⚠ Не удалось привести {column} к {dtype}: {e}")
        
        print(f'Финальные колонки: {df.columns}')
        print('=' * 100)
        
        # ============================================================
        # ШАГ 9: Финальный датафрейм
        # ============================================================
        final_df = df.select(*SILVER_COLUMNS)
        
        # ============================================================
        # ШАГ 10: Фильтрация
        # ============================================================
        final_df = final_df.filter(
            col('sales_quantity').isNull() | (col('sales_quantity') > 0)
        )
        final_df = final_df.filter(
            col('sales_amount_rub').isNull() | (col('sales_amount_rub') > 0)
        )
        final_df = final_df.filter(
            col('sales_cost_price').isNull() | (col('sales_cost_price') > 0)
        )
        
        # ============================================================
        # ШАГ 11: Расчет средних цен
        # ============================================================
        final_df = final_df.withColumn(
            'average_cost_price', 
            when(col('sales_quantity').isNotNull() & (col('sales_quantity') > 0), 
                 col('sales_cost_price') / col('sales_quantity'))
        )
        
        final_df = final_df.withColumn(
            'average_sell_price', 
            when(col('sales_quantity').isNotNull() & (col('sales_quantity') > 0), 
                 col('sales_amount_rub') / col('sales_quantity'))
        )
        
        # ============================================================
        # ШАГ 12: Проверка дубликатов и запись
        # ============================================================
        try:
            res = spark.sql(f'''
                SELECT COUNT(*) 
                FROM iceberg.magnit_silver.sales 
                WHERE file_name = '{file[:-4]}'
            ''').first()
            file_exists = res[0] > 0
        except:
            file_exists = False
        
        if not file_exists:
            rows_before = final_df.count()
            print(f'Записываем {rows_before} строк в iceberg.magnit_silver.sales...')
            
            final_df.writeTo('iceberg.magnit_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()
            
            print(f'✓ Файл {file} успешно занесен ({rows_before} строк)')
        else:
            print(f'⊘ Данные из файла {file} уже есть в таблице')
        
        print('=' * 100)
        print()

print('✅ Обработка Магнит завершена!')