import boto3
import traceback
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder \
    .appName('Iceberg Magnit NEW Format ETL') \
    .getOrCreate()

print(f"✓ Версия Spark: {spark.version}")

s3 = boto3.client('s3',
                  endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin'
                 )

RAW_COLUMNS = [
    'month_num_raw', 'year_raw', 'store_code', 'store_name', 'format',
    'address', 'category_level_1', 'category_level_2', 'category_level_3',
    'product_id', 'product_name', 'brand', 'vendor', 'barcode',
    'sales_quantity_raw', 'price_cost_raw', 'price_sell_raw'
]

SILVER_COLUMNS = [
    'year', 'month', 'month_num', 'retail_chain',
    'store_code', 'store_name', 'format', 'address',
    'category_level_1', 'category_level_2', 'category_level_3',
    'product_id', 'product_name', 'brand', 'vendor', 'barcode',
    'sales_quantity', 'average_cost_price', 'average_sell_price',
    'sales_amount_rub', 'sales_cost_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

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

TARGET_TABLE = 'iceberg.magnit_new_silver.magnit_new_sales'


def detect_encoding(bucket, key, s3_client):
    """Определяет кодировку файла по первым байтам."""
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=key, Range='bytes=0-2048')
        sample = obj['Body'].read()
        
        if sample[:3] == b'\xef\xbb\xbf':
            return 'UTF-8'
        if sample[:2] == b'\xff\xfe':
            return 'UTF-16LE'
        if sample[:2] == b'\xfe\xff':
            return 'UTF-16BE'
        
        try:
            sample.decode('utf-8')
            return 'UTF-8'
        except UnicodeDecodeError:
            return 'CP1251'
    except Exception as e:
        print(f"  ⚠ Не удалось определить кодировку: {e}, используем UTF-8")
        return 'UTF-8'


def update_product_mapping(final_df, source_table_name):
    """Добавляет уникальные товары в product_mapping в ClickHouse."""
    try:
        import clickhouse_connect

        products = final_df.select(
            F.col('product_name').alias('original_name'),
            F.lit(source_table_name).alias('source_table'),
            F.col('brand').alias('sample_brand'),
            F.col('category_level_1').alias('sample_category')
        ).filter(
            F.col('original_name').isNotNull() & (F.trim(F.col('original_name')) != '')
        ).dropDuplicates(['original_name'])

        n = products.count()
        if n == 0:
            print(f"  ⊘ Нет товаров для добавления в справочник")
            return

        pdf = products.toPandas()

        ch = clickhouse_connect.get_client(
            host='clickhouse', port=8123,
            username='admin', password='123', database='default'
        )
        ch.insert_df('product_mapping', pdf)
        ch.close()

        print(f"  ✓ Справочник product_mapping обновлён: +{n} записей")
    except Exception as e:
        print(f"  ⚠ Не удалось обновить справочник: {e}")


# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket='rawkonditerka', Prefix='magnit_new_')

if 'Contents' not in objects:
    print("⚠ Нет файлов magnit_new_ в бакете rawkonditerka")
else:
    files = objects['Contents']

    year_cr = datetime.now().year
    month_cr = datetime.now().month
    day_cr = datetime.now().day
    date_created_str = f'{year_cr}-{month_cr:02d}-{day_cr:02d}'

    for obj in files:
        file = obj['Key']
        if not file.endswith('.csv'):
            continue

        file_name = f's3a://rawkonditerka/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        
        encoding = detect_encoding('rawkonditerka', file, s3)
        print(f'✓ Кодировка: {encoding}')
        print('=' * 100)

        df = spark.read.option('header', 'false') \
                       .option('encoding', encoding) \
                       .csv(file_name, sep=';')

        actual_cols = df.columns
        print(f'✓ Колонок в файле: {len(actual_cols)}')
        print(f'✓ Строк: {df.count()}')

        for i, new_name in enumerate(RAW_COLUMNS):
            if i < len(actual_cols):
                df = df.withColumnRenamed(actual_cols[i], new_name)

        df = df.select(*[c for c in RAW_COLUMNS if c in df.columns])

        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created_str).cast('date'))
        df = df.withColumn('updated_at', F.lit(date_created_str).cast('date'))

        parts = file.split('_')
        month_str = MONTH_MAPPING.get(parts[2].lower())
        month_int = MONTH_MAPPING_INT.get(month_str)
        parsed_year = parts[3].replace('.csv', '')

        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(int(parsed_year)))
        df = df.withColumn('month_num', F.lit(month_int))
        period_str = f'{parsed_year}-{month_int:02d}-01'
        df = df.withColumn('period', F.lit(period_str).cast('date'))

        df = df.withColumn('retail_chain', F.lit('Магнит'))

        # Запятые → точки, пустые → null
        for col_name in ['sales_quantity_raw', 'price_cost_raw', 'price_sell_raw']:
            if col_name in df.columns:
                df = df.withColumn(col_name,
                    F.regexp_replace(F.col(col_name), ',', '.'))
                df = df.withColumn(col_name,
                    F.when(F.trim(F.col(col_name)) == '', None).otherwise(F.col(col_name)))

        # ============================================================
        # ✅ ИСПРАВЛЕНО: price_*_raw — это уже СУММЫ в рублях
        # ============================================================
        df = df.withColumn('sales_quantity', F.col('sales_quantity_raw').cast('double'))
        df = df.withColumn('sales_cost_price', F.col('price_cost_raw').cast('double'))
        df = df.withColumn('sales_amount_rub', F.col('price_sell_raw').cast('double'))

        # Средние цены = СУММА / КОЛИЧЕСТВО
        df = df.withColumn(
            'average_sell_price',
            F.when(F.col('sales_quantity') > 0,
                   F.col('sales_amount_rub') / F.col('sales_quantity'))
        )
        df = df.withColumn(
            'average_cost_price',
            F.when(F.col('sales_quantity') > 0,
                   F.col('sales_cost_price') / F.col('sales_quantity'))
        )
        # ============================================================

        remaining_cols = set(SILVER_COLUMNS) - set(df.columns)
        for column in remaining_cols:
            df = df.withColumn(column, F.lit(None))

        df = df.withColumn('year', F.col('year').cast('int'))
        df = df.withColumn('month_num', F.col('month_num').cast('int'))
        df = df.withColumn('store_code', F.col('store_code').cast('string'))
        df = df.withColumn('product_id', F.col('product_id').cast('string'))
        df = df.withColumn('barcode', F.col('barcode').cast('string'))
        df = df.withColumn('created_at', F.col('created_at').cast('date'))
        df = df.withColumn('updated_at', F.col('updated_at').cast('date'))
        df = df.withColumn('period', F.col('period').cast('date'))

        final_df = df.select(*SILVER_COLUMNS)

        final_df = final_df.filter(
            (F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0)) &
            (F.col('sales_amount_rub').isNotNull() & (F.col('sales_amount_rub') > 0))
        )

        try:
            res = spark.sql(
                f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE file_name = '{file[:-4]}'"
            ).first()
            file_exists = res[0] > 0
        except:
            file_exists = False

        if not file_exists:
            final_df.cache()
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.magnit_new_silver")
            final_df.writeTo(TARGET_TABLE) \
                    .partitionedBy('retail_chain', 'year', 'month') \
                    .append()
            print(f'✓ {file} → {rows} строк')

            update_product_mapping(final_df, 'magnit_new_sales')

            final_df.unpersist()
        else:
            print(f'⊘ {file} уже есть, пропускаем')

        print('=' * 100)

print('✅ Обработка Magnit NEW format завершена!')
spark.stop()