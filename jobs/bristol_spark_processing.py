import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from datetime import datetime
import re

# Создаем Spark сессию
spark = SparkSession.builder \
    .appName('Iceberg Bristol ETL') \
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
    'month', 'year', 'retail_chain',
    'product_category', 'product_name',
    'rating_sku', 'numeric_distribution',
    'revenue_rub', 'sales_quantity', 'cost_price_rub',
    'average_sell_price', 'average_cost_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

# Маппинг русских названий на английские
MAPPING = {
    'МЕСЯЦ': 'month',
    'ГРУППА': 'product_category',
    'SKU - НАИМЕНОВАНИЕ АНАЛОГ': 'product_name',
    'Рейтинг SKU Активные магазины, кол-во': 'rating_sku',
    'Нумерическая дистрибуция, %': 'numeric_distribution',
    'ТО с НДС, руб.': 'revenue_rub',
    'ТО, шт.': 'sales_quantity',
    'Себестоимость реализации, руб. с НДС': 'cost_price_rub'
}

# Месяца для парсинга
MONTH_MAPPING = {
    'Jan': 'Январь', 'Feb': 'Февраль', 'Mar': 'Март', 'Apr': 'Апрель',
    'May': 'Май', 'Jun': 'Июнь', 'Jul': 'Июль', 'Aug': 'Август',
    'Sep': 'Сентябрь', 'Oct': 'Октябрь', 'Nov': 'Ноябрь', 'Dec': 'Декабрь'
}

# Подключаемся к MinIO
s3 = boto3.client('s3', endpoint_url='http://minio:9000', 
                  aws_access_key_id='minioadmin', 
                  aws_secret_access_key='minioadmin')

objects = s3.list_objects_v2(Bucket='raw', Prefix='bristol_')

if 'Contents' not in objects:
    print("⚠ Нет файлов bristol_")
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
        
        # Читаем CSV с разделителем ;
        try:
            df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .option("delimiter", ";") \
                .option("quote", "\"") \
                .csv(file_name)
            print("✓ Разделитель: ;")
        except Exception as e:
            print(f"⚠ Ошибка чтения: {e}")
            continue
        
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
        print(f'Колонки: {df.columns}')
        print('=' * 100)
        
        # Переименовываем колонки
        for old_name in df.columns:
            if old_name in MAPPING:
                new_name = MAPPING[old_name]
                if new_name != old_name:
                    df = df.withColumnRenamed(old_name, new_name)
        
        # Добавляем служебные колонки
        df = df.withColumn('retail_chain', F.lit('Бристоль'))
        df = df.withColumn('file_name', F.lit(file.replace('.csv', '').replace('.xlsx', '')))
        df = df.withColumn('created_at', F.to_date(F.lit(date_created), 'yyyy-MM-dd'))
        df = df.withColumn('updated_at', F.to_date(F.lit(date_created), 'yyyy-MM-dd'))
        
        # Парсим месяц и год из колонки month
        if 'month' in df.columns:
            # Парсим строку типа 'Dec-25'
            def parse_month_year(ms):
                if not ms:
                    return ('Неизвестно', 2026)
                parts = str(ms).split('-')
                if len(parts) == 2:
                    month_abbr = parts[0].strip()
                    year_str = parts[1].strip()
                    month_name = MONTH_MAPPING.get(month_abbr, 'Неизвестно')
                    year_num = int(year_str) if year_str.isdigit() else 2026
                    if year_num < 100:
                        year_num = 2000 + year_num
                    return (month_name, year_num)
                return ('Неизвестно', 2026)
            
            parse_udf = F.udf(parse_month_year, StructType([
                StructField("month_name", StringType()),
                StructField("year_num", IntegerType())
            ]))
            
            df = df.withColumn("parsed", parse_udf(F.col("month")))
            df = df.withColumn("month_new", F.col("parsed.month_name"))
            df = df.withColumn("year_new", F.col("parsed.year_num"))
            df = df.drop("parsed", "month")
            df = df.withColumnRenamed("month_new", "month")
            df = df.withColumnRenamed("year_new", "year")
        else:
            df = df.withColumn('month', F.lit('Неизвестно'))
            df = df.withColumn('year', F.lit(y))
        
        # Создаем период (первое число месяца)
        df = df.withColumn('period',
            F.to_date(
                F.concat(
                    F.col('year').cast('string'),
                    F.lit('-'),
                    F.when(F.col('month') == 'Январь', '01')
                     .when(F.col('month') == 'Февраль', '02')
                     .when(F.col('month') == 'Март', '03')
                     .when(F.col('month') == 'Апрель', '04')
                     .when(F.col('month') == 'Май', '05')
                     .when(F.col('month') == 'Июнь', '06')
                     .when(F.col('month') == 'Июль', '07')
                     .when(F.col('month') == 'Август', '08')
                     .when(F.col('month') == 'Сентябрь', '09')
                     .when(F.col('month') == 'Октябрь', '10')
                     .when(F.col('month') == 'Ноябрь', '11')
                     .when(F.col('month') == 'Декабрь', '12')
                     .otherwise('01'),
                    F.lit('-01')
                )
            )
        )
        
        # Очищаем числовые поля от % и пробелов
        if 'numeric_distribution' in df.columns:
            df = df.withColumn('numeric_distribution', 
                F.regexp_replace(F.col('numeric_distribution').cast('string'), '%', '').cast('double'))
        
        # Приводим типы
        if 'rating_sku' in df.columns:
            df = df.withColumn('rating_sku', F.col('rating_sku').cast('int'))
        
        if 'revenue_rub' in df.columns:
            df = df.withColumn('revenue_rub', 
                F.regexp_replace(F.col('revenue_rub').cast('string'), ',', '.').cast('double'))
        
        if 'sales_quantity' in df.columns:
            df = df.withColumn('sales_quantity', F.col('sales_quantity').cast('int'))
        
        if 'cost_price_rub' in df.columns:
            df = df.withColumn('cost_price_rub', 
                F.regexp_replace(F.col('cost_price_rub').cast('string'), ',', '.').cast('double'))
        
        # ============================================================
        # РАСЧЕТ СРЕДНИХ ЗНАЧЕНИЙ
        # ============================================================
        
        # Средняя цена продажи (revenue_rub / sales_quantity)
        df = df.withColumn('average_sell_price',
            F.when(F.col('sales_quantity') > 0, 
                   F.col('revenue_rub') / F.col('sales_quantity'))
             .otherwise(F.lit(None))
        )
        
        # Средняя себестоимость (cost_price_rub / sales_quantity)
        df = df.withColumn('average_cost_price',
            F.when(F.col('sales_quantity') > 0, 
                   F.col('cost_price_rub') / F.col('sales_quantity'))
             .otherwise(F.lit(None))
        )
        
        print(f"✓ Рассчитаны средние цены:")
        print(f"   - average_sell_price = revenue_rub / sales_quantity")
        print(f"   - average_cost_price = cost_price_rub / sales_quantity")
        
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
            res = spark.sql(f"SELECT COUNT(*) FROM iceberg.bristol_silver.sales WHERE file_name = '{file.replace('.csv', '').replace('.xlsx', '')}'").first()
            file_exists = res[0] > 0
        except Exception as e:
            print(f"⚠ Таблица еще не существует или ошибка: {e}")
            file_exists = False
        
        # Записываем
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            
            final_df.writeTo('iceberg.bristol_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()
            
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')
        
        print('=' * 100)
        print()

print('✅ Обработка Бристоль завершена!')
spark.stop()