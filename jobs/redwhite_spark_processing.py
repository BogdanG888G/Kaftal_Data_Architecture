import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
import re

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName('Iceberg Red&White ETL') \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.catalog.iceberg.client.region", "us-east-1") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print(f"✓ Spark: {spark.version}")

# Колонки для silver слоя
SILVER_COLUMNS = [
    'year', 'month', 'retail_chain',
    'group_number', 'product_name',
    'quantity_shipped', 'quantity_boxes',
    'revenue_rub', 'markup_rub', 'profitability_percent',
    'file_name', 'created_at', 'updated_at', 'period'
]

# Маппинг русских названий на английские
MAPPING = {
    '№ в группе': 'group_number',
    'Товар главный': 'product_name',
    'Количество, шт': 'quantity_shipped',
    'Количество коробок': 'quantity_boxes',
    'Сумма, руб.': 'revenue_rub',
    'Наценка, руб.': 'markup_rub',
    'Рентабельность, %': 'profitability_percent'
}

# Месяца для парсинга
MONTH_MAPPING = {
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март', 'april': 'Апрель',
    'may': 'Май', 'june': 'Июнь', 'july': 'Июль', 'august': 'Август',
    'september': 'Сентябрь', 'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
    'января': 'Январь', 'февраля': 'Февраль', 'марта': 'Март', 'апреля': 'Апрель',
    'мая': 'Май', 'июня': 'Июнь', 'июля': 'Июль', 'августа': 'Август',
    'сентября': 'Сентябрь', 'октября': 'Октябрь', 'ноября': 'Ноябрь', 'декабря': 'Декабрь'
}

# Подключаемся к MinIO
s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                  aws_access_key_id='minioadmin', 
                  aws_secret_access_key='minioadmin')

objects = s3.list_objects_v2(Bucket='raw', Prefix='redwhite_')

if 'Contents' not in objects:
    print("⚠ Нет файлов redwhite_")
else:
    y, m, d = datetime.now().year, datetime.now().month, datetime.now().day
    date_created = f'{y}-{m:02d}-{d:02d}'

    for obj in objects['Contents']:
        file = obj['Key']
        if not file.endswith('.csv') and not file.endswith('.xlsx'):
            continue
        
        file_name = f's3a://raw/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        
        # Читаем CSV с разделителем TAB или ;
        try:
            # Пробуем TAB
            df = spark.read.csv(file_name, sep='\t', header=True, inferSchema=True)
            if len(df.columns) <= 2:
                # Пробуем ;
                df = spark.read.csv(file_name, sep=';', header=True, inferSchema=True)
                print("✓ Разделитель: ;")
            else:
                print("✓ Разделитель: TAB")
        except Exception as e:
            print(f"⚠ Ошибка чтения: {e}")
            continue
        
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
        print(f'Колонки: {df.columns}')
        print('=' * 100)
        
        # Если все еще одна колонка - пробуем другой подход
        if len(df.columns) == 1:
            print("⚠ Файл читается как одна колонка, пробуем разделитель ';'...")
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("delimiter", ";") \
                .option("quote", "\"") \
                .csv(file_name)
            print(f"✓ Новое количество колонок: {len(df.columns)}")
        
        # Переименовываем колонки
        for old_name in df.columns:
            if old_name in MAPPING:
                new_name = MAPPING[old_name]
                if new_name != old_name:
                    df = df.withColumnRenamed(old_name, new_name)
        
        # Добавляем служебные колонки
        df = df.withColumn('retail_chain', F.lit('Красное и Белое'))
        df = df.withColumn('file_name', F.lit(file.replace('.csv', '').replace('.xlsx', '')))
        df = df.withColumn('created_at', F.to_date(F.lit(date_created), 'yyyy-MM-dd'))
        df = df.withColumn('updated_at', F.to_date(F.lit(date_created), 'yyyy-MM-dd'))
        
        # Парсим месяц и год из имени файла
        parts = file.replace('.csv', '').replace('.xlsx', '').split('_')
        month_str = None
        parsed_year = None
        
        for i, part in enumerate(parts):
            # Ищем месяц
            part_lower = part.lower()
            if part_lower in MONTH_MAPPING:
                month_str = MONTH_MAPPING[part_lower]
            # Ищем год (4 цифры)
            elif part.isdigit() and len(part) == 4:
                parsed_year = int(part)
        
        if month_str is None:
            month_str = datetime.now().strftime('%B')
            month_str = MONTH_MAPPING.get(month_str.lower(), 'Неизвестно')
        
        if parsed_year is None:
            parsed_year = y
        
        # Получаем номер месяца
        month_num = {
            'Январь': '01', 'Февраль': '02', 'Март': '03', 'Апрель': '04',
            'Май': '05', 'Июнь': '06', 'Июль': '07', 'Август': '08',
            'Сентябрь': '09', 'Октябрь': '10', 'Ноябрь': '11', 'Декабрь': '12'
        }.get(month_str, '01')
        
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(parsed_year))
        
        # Создаем период (первое число месяца)
        df = df.withColumn('period', F.to_date(F.lit(f'{parsed_year}-{month_num}-01'), 'yyyy-MM-dd'))
        
        # Очищаем числовые поля от пробелов и преобразуем
        if 'group_number' in df.columns:
            df = df.withColumn('group_number', 
                F.regexp_replace(F.col('group_number').cast('string'), ' ', '').cast('int'))
        
        if 'quantity_shipped' in df.columns:
            df = df.withColumn('quantity_shipped', 
                F.regexp_replace(F.col('quantity_shipped').cast('string'), ' ', '').cast('int'))
        
        if 'quantity_boxes' in df.columns:
            df = df.withColumn('quantity_boxes', 
                F.regexp_replace(F.col('quantity_boxes').cast('string'), ' ', '').cast('int'))
        
        if 'revenue_rub' in df.columns:
            df = df.withColumn('revenue_rub', 
                F.regexp_replace(F.col('revenue_rub').cast('string'), ',', '.')
                 .cast('double'))
        
        if 'markup_rub' in df.columns:
            df = df.withColumn('markup_rub', 
                F.regexp_replace(F.col('markup_rub').cast('string'), ',', '.')
                 .cast('double'))
        
        if 'profitability_percent' in df.columns:
            df = df.withColumn('profitability_percent', 
                F.regexp_replace(F.col('profitability_percent').cast('string'), ',', '.')
                 .cast('double'))
        
        # Добавляем недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        for col in remaining:
            df = df.withColumn(col, F.lit(None))
        
        # Выбираем финальные колонки
        final_df = df.select(*SILVER_COLUMNS)
        
        print(f'Финальные колонки: {final_df.columns}')
        print('Пример данных:')
        final_df.show(5, truncate=50)
        
        # Проверяем существование файла в таблице
        try:
            res = spark.sql(f"SELECT COUNT(*) FROM iceberg.redwhite_silver.sales WHERE file_name = '{file.replace('.csv', '').replace('.xlsx', '')}'").first()
            file_exists = res[0] > 0
        except Exception as e:
            print(f"⚠ Таблица еще не существует или ошибка: {e}")
            file_exists = False
        
        # Записываем
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            
            final_df.writeTo('iceberg.redwhite_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()
            
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')
        
        print('=' * 100)
        print()

print('✅ Обработка Красное и Белое завершена!')
spark.stop()