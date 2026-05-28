import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName('Iceberg Vernyi ETL') \
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
    'week_num', 'store_code', 'address', 'region_name', 'city_name',
    'product_category_3', 'product_category_4',
    'product_id', 'product_name',
    'vendor', 'manufacturer',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_cost_price', 'average_sell_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

# Типы данных
SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'week_num': 'int', 'store_code': 'string', 'address': 'string', 
    'region_name': 'string', 'city_name': 'string',
    'product_category_3': 'string', 'product_category_4': 'string',
    'product_id': 'string', 'product_name': 'string',
    'vendor': 'string', 'manufacturer': 'string',
    'sales_quantity': 'int', 'sales_amount_rub': 'float', 'sales_cost_price': 'float',
    'average_cost_price': 'float', 'average_sell_price': 'float',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

# Маппинг названий (если будут старые файлы)
MAPPING = {
    'Неделя': 'week_num', '№ магазина': 'store_code', 'Адрес': 'address', 
    'Регион': 'region_name', 'Группа': 'product_category_3', 
    'Подгруппа': 'product_category_4', 'Код товара': 'product_id', 
    'Наименование товара': 'product_name', 'Поставщик': 'vendor', 
    'Производитель': 'manufacturer', 'Реализация шт.(кг)': 'sales_quantity', 
    'Реализация Сумма': 'sales_amount_rub', 'Цена ед.': 'average_sell_price', 
    'Цена вх.': 'average_cost_price',
}

# Месяца
MONTH_MAPPING = {
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март', 'april': 'Апрель',
    'may': 'Май', 'june': 'Июнь', 'july': 'Июль', 'august': 'Август',
    'september': 'Сентябрь', 'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
}

MONTH_MAPPING_INT = {v: k for k, v in {
    1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 
    5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август', 
    9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'
}.items()}

# Подключаемся к MinIO
s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                  aws_access_key_id='minioadmin', 
                  aws_secret_access_key='minioadmin')

objects = s3.list_objects_v2(Bucket='raw', Prefix='vernyi_')

if 'Contents' not in objects:
    print("⚠ Нет файлов vernyi_")
else:
    y, m, d = datetime.now().year, datetime.now().month, datetime.now().day
    date_created = f'{y}-{m:02d}-{d:02d}'

    for obj in objects['Contents']:
        file = obj['Key']
        if not file.endswith('.csv'): 
            continue
        
        file_name = f's3a://raw/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        
        # ============================================================
        # ЧИТАЕМ CSV С ПРАВИЛЬНЫМ РАЗДЕЛИТЕЛЕМ
        # ============================================================
        try:
            # Пробуем TAB
            df = spark.read.csv(file_name, sep='\t', header=True, inferSchema=True)
            if len(df.columns) > 5 and ('week_num' in df.columns or 'Неделя' in df.columns):
                print("✓ Разделитель: TAB")
            else:
                # Используем ; (основной разделитель)
                df = spark.read.csv(file_name, sep=';', header=True, inferSchema=True)
                print("✓ Разделитель: ; (точка с запятой)")
        except Exception as e:
            print(f"⚠ Ошибка чтения: {e}")
            continue
        
        # Проверяем что колонки распарсились нормально
        if len(df.columns) <= 2:
            print(f"❌ ОШИБКА: всего {len(df.columns)} колонок, файл читается как одна строка")
            print(f"   Первые 100 символов файла:")
            # Показываем начало файла для диагностики
            try:
                obj_data = s3.get_object(Bucket='raw', Key=file)
                content = obj_data['Body'].read(200).decode('utf-8')
                print(f"   {content[:200]}")
            except:
                pass
            continue
        
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
        print(f'Колонки: {df.columns[:10]}')
        print('=' * 100)
        
        # Переименование колонок если нужно (для старых файлов)
        for old_name in df.columns:
            if old_name in MAPPING:
                new_name = MAPPING[old_name]
                if new_name != old_name:
                    df = df.withColumnRenamed(old_name, new_name)
        
        # Добавляем служебные колонки
        df = df.withColumn('retail_chain', F.lit('Верный'))
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))
        
        # Парсим месяц и год из имени файла
        parts = file.replace('.csv', '').split('_')
        month_str = None
        parsed_year = None
        
        for i, part in enumerate(parts):
            # Ищем месяц
            if part.lower() in MONTH_MAPPING:
                month_str = MONTH_MAPPING[part.lower()]
            # Ищем год (4 цифры)
            elif part.isdigit() and len(part) == 4:
                parsed_year = int(part)
        
        if month_str is None:
            month_str = 'Апрель'  # значение по умолчанию
        
        if parsed_year is None:
            parsed_year = y
        
        month_int = MONTH_MAPPING_INT.get(month_str, 4)
        
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(parsed_year))
        period_done = datetime.strptime(f'{parsed_year}-{month_int:02d}-01', '%Y-%m-%d')
        df = df.withColumn('period', F.lit(period_done))
        
        # Извлекаем город из адреса
        if 'address' in df.columns:
            df = df.withColumn('city_name',
                F.coalesce(
                    F.regexp_extract(F.col('address'), r'г\.\s*([^,]+)', 1),
                    F.regexp_extract(F.col('address'), r'г\s+([^,]+)', 1),
                    F.regexp_extract(F.col('address'), r'^([^,]+)', 1)
                )
            )
        else:
            df = df.withColumn('city_name', F.lit(None))
        
        # Преобразуем запятые в точки для числовых полей
        for col_name in ['sales_amount_rub', 'average_sell_price', 'average_cost_price']:
            if col_name in df.columns:
                df = df.withColumn(col_name, 
                    F.regexp_replace(F.col(col_name).cast('string'), ',', '.').cast('float'))
        
        # Вычисляем общую себестоимость
        if 'average_cost_price' in df.columns and 'sales_quantity' in df.columns:
            df = df.withColumn('sales_cost_price',
                F.when(F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0),
                       F.col('average_cost_price') * F.col('sales_quantity'))
                 .otherwise(F.lit(None)))
        
        # Добавляем недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        for col in remaining:
            df = df.withColumn(col, F.lit(None))
        
        # Приводим типы данных
        for col_name in df.columns:
            if col_name in SILVER_TYPES:
                target_type = SILVER_TYPES[col_name]
                try:
                    if target_type == 'int':
                        df = df.withColumn(col_name, F.col(col_name).cast('int'))
                    elif target_type == 'float':
                        df = df.withColumn(col_name, F.col(col_name).cast('float'))
                    elif target_type == 'date':
                        df = df.withColumn(col_name, F.col(col_name).cast('date'))
                    else:
                        df = df.withColumn(col_name, F.col(col_name).cast('string'))
                except Exception as e:
                    print(f"  ⚠ Ошибка при касте {col_name}: {e}")
        
        # Выбираем финальные колонки
        final_df = df.select(*SILVER_COLUMNS)
        
        print(f'Финальные колонки: {len(final_df.columns)}')
        print('=' * 100)
        
        # Проверяем существование файла в таблице
        try:
            res = spark.sql(f"SELECT COUNT(*) FROM iceberg.vernyi_silver.sales WHERE file_name = '{file[:-4]}'").first()
            file_exists = res[0] > 0
        except Exception as e:
            print(f"⚠ Ошибка проверки: {e}")
            file_exists = False
        
        # Записываем данные
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            
            final_df.writeTo('iceberg.vernyi_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()
            
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')
        
        print('=' * 100)
        print()

print('✅ Обработка Верный завершена!')
spark.stop()