import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import logging

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName('Iceberg Dixy ETL') \
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
# КОНСТАНТЫ И МАППИНГИ
# ============================================================
SILVER_COLUMNS = [
    'year', 'month', 'retail_chain',
    'region_name', 'city_name', 'address', 'store_code',
    'product_category_3', 'product_category_4', 'product_category_5',
    'product_id', 'product_name', 'brand', 'vendor',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_cost_price', 'average_sell_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'region_name': 'string', 'city_name': 'string', 'address': 'string', 'store_code': 'string',
    'product_category_3': 'string', 'product_category_4': 'string', 'product_category_5': 'string',
    'product_id': 'string', 'product_name': 'string', 'brand': 'string', 'vendor': 'string',
    'sales_quantity': 'int', 'sales_amount_rub': 'float', 'sales_cost_price': 'float',
    'average_cost_price': 'float', 'average_sell_price': 'float',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

SILVER_MAPPING = {
    'Уровень 3': 'product_category_3',
    'Уровень 4': 'product_category_4',
    'Уровень 5': 'product_category_5',
    'ВТМ': 'vendor',
    'Товар': 'product_name',
    'Адрес': 'address',
    'Код товара': 'product_id',
    'МАГАЗИНЫ': 'store_code',
    'Количество_итоги': 'sales_quantity',
    'Себестоимость с НДС_итоги': 'sales_cost_price',
    'Сумма с НДС_итоги': 'sales_amount_rub'
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

# ============================================================
# ОСНОВНОЙ ЦИКЛ
# ============================================================
objects = s3.list_objects_v2(Bucket='raw', Prefix='diksi_')

if 'Contents' not in objects:
    print("⚠ Нет файлов diksi_ в бакете raw")
else:
    files = objects['Contents']

    year_cr = datetime.now().year
    month_cr = datetime.now().month
    day_cr = datetime.now().day
    date_created = f'{year_cr}-{month_cr:02d}-{day_cr:02d}'

    for obj in files:
        
        file = obj['Key']
        file_name = f's3a://raw/{file}'
        
        print('=' * 100)
        print(f'Обработка: {file_name}')
        print('=' * 100)
        
        df = spark.read.csv(file_name, sep=';', header=True, inferSchema=False)
        print(f'✓ Прочитано строк: {df.count()}')
        print(f'Исходные колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 1: Оставляем нужные колонки
        descriptive_cols = ['Уровень 3', 'Уровень 4', 'Уровень 5', 'ВТМ', 'Товар', 'Адрес', 'Код товара', 'МАГАЗИНЫ']
        summary_cols = ['Количество_итоги', 'Себестоимость с НДС_итоги', 'Сумма с НДС_итоги']
        
        cols_to_keep = [c for c in descriptive_cols + summary_cols if c in df.columns]
        df = df.select(*cols_to_keep)
        
        # ШАГ 2: Маппинг
        for column in df.columns:
            new_name = SILVER_MAPPING.get(column)
            if new_name:
                df = df.withColumnRenamed(column, new_name)
        
        # ШАГ 3: Служебные
        df = df.withColumn('retail_chain', F.lit('Дикси'))
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))
        
        # ШАГ 4: Дата из имени файла
        parts = file.split('_')
        month_str = MONTH_MAPPING.get(parts[1].lower())
        month_int = MONTH_MAPPING_INT.get(month_str)
        parsed_year = parts[2].replace('.csv', '')
        
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(int(parsed_year)))
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
        
        # ШАГ 6: brand = vendor
        if 'vendor' in df.columns:
            df = df.withColumn('brand', df['vendor'])
        
        # ШАГ 7: Замена запятых
        for col_name in ['sales_amount_rub', 'sales_cost_price']:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.regexp_replace(df[col_name], ',', '.'))
        
        # ШАГ 8: Недостающие колонки
        remaining_cols = set(SILVER_COLUMNS) - set(df.columns)
        print(f'Оставшиеся колонки (NULL): {remaining_cols}')
        
        for column in remaining_cols:
            df = df.withColumn(column, F.lit(None))
        
        # ШАГ 9: Типы
        for column in df.columns:
            dtype_str = SILVER_TYPES.get(column)
            if dtype_str:
                try:
                    df = df.withColumn(column, df[column].cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {column}: {e}")
        
        print('Финальные колонки:')
        print(df.columns)
        print('=' * 100)
        
        # ШАГ 10: Финальный датафрейм
        final_df = df.select(*SILVER_COLUMNS)
        
        # ШАГ 11: Фильтрация
        final_df = final_df.filter(
            df['sales_quantity'].isNull() | (df['sales_quantity'] > 0)
        )
        final_df = final_df.filter(
            df['sales_amount_rub'].isNull() | (df['sales_amount_rub'] > 0)
        )
        final_df = final_df.filter(
            df['sales_cost_price'].isNull() | (df['sales_cost_price'] > 0)
        )
        
        # ШАГ 12: Расчет средних цен
        final_df = final_df.withColumn(
            'average_cost_price', 
            F.coalesce(
                df['average_cost_price'], 
                F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0), 
                     df['sales_cost_price'] / df['sales_quantity'])
            )
        )
        
        final_df = final_df.withColumn(
            'average_sell_price', 
            F.coalesce(
                df['average_sell_price'], 
                F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0), 
                     df['sales_amount_rub'] / df['sales_quantity'])
            )
        )
        
        # ШАГ 13: Проверка дубликатов и запись
        try:
            res = spark.sql(f'''
                SELECT COUNT(*) 
                FROM iceberg.diksi_silver.sales 
                WHERE file_name = '{file[:-4]}'
            ''').first()
            file_exists = res[0] > 0
        except:
            file_exists = False
        
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            final_df.writeTo('iceberg.diksi_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть в таблице')
        
        print('=' * 100)
        print()

print('✅ Обработка Дикси завершена!')