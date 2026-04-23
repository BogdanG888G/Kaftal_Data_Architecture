import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
from datetime import datetime

# ============================================================
# SPARK SESSION
# ============================================================
spark = SparkSession.builder \
    .appName('Iceberg Dixy ETL') \
    .getOrCreate()

print(f"✓ Версия Spark: {spark.version}")

# ============================================================
# СОЗДАНИЕ НЕЙМСПЕЙСА И ТАБЛИЦЫ
# ============================================================
print("Создаем неймспейс и таблицу...")

spark.sql('CREATE NAMESPACE IF NOT EXISTS iceberg.diksi_silver')

spark.sql('''
    CREATE TABLE IF NOT EXISTS iceberg.diksi_silver.sales (
        year                INT,
        month               STRING,
        retail_chain        STRING,
        region_name         STRING,
        city_name           STRING,
        address             STRING,
        store_code          STRING,
        product_category_3  STRING,
        product_category_4  STRING,
        product_category_5  STRING,
        product_id          STRING,
        product_name        STRING,
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
    LOCATION 's3://warehouse/diksi_silver/sales'
''')

print("✓ Таблица готова\n")

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
SILVER_COLUMNS = {
    'year': 'int',
    'month': 'string',
    'retail_chain': 'string',
    'region_name': 'string',
    'city_name': 'string',
    'address': 'string',
    'store_code': 'string',
    'product_category_3': 'string',
    'product_category_4': 'string',
    'product_category_5': 'string',
    'product_id': 'string',
    'product_name': 'string',
    'brand': 'string',
    'vendor': 'string',
    'sales_quantity': 'int',
    'sales_amount_rub': 'float',
    'sales_cost_price': 'float',
    'average_cost_price': 'float',
    'average_sell_price': 'float',
    'file_name': 'string',
    'created_at': 'date',
    'updated_at': 'date',
    'period': 'date'
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
# ОСНОВНОЙ ЦИКЛ ОБРАБОТКИ
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
        
        # Читаем CSV
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
        
        # ШАГ 3: Служебные колонки
        df = df.withColumn('retail_chain', lit('Дикси'))
        df = df.withColumn('file_name', lit(file[:-4]))
        df = df.withColumn('created_at', lit(date_created))
        df = df.withColumn('updated_at', lit(date_created))
        
        # ШАГ 4: Дата из имени файла
        parts = file.split('_')
        month_str = MONTH_MAPPING.get(parts[1].lower())
        month_int = MONTH_MAPPING_INT.get(month_str)
        parsed_year = parts[2].replace('.csv', '')
        
        df = df.withColumn('month', lit(month_str))
        df = df.withColumn('year', lit(int(parsed_year)))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', lit(period_done))
        
        # ШАГ 5: Извлечение города и региона (Spark SQL, без UDF)
        if 'address' in df.columns:
            # Город
            df = df.withColumn('city_name', 
                coalesce(
                    regexp_extract(col('address'), r'г\.\s*([^,]+)', 1),
                    regexp_extract(col('address'), r'г\s+([^,]+)', 1)
                )
            )
            # Регион
            df = df.withColumn('region_name',
                coalesce(
                    regexp_extract(col('address'), r'([^,]*область[^,]*)', 1),
                    regexp_extract(col('address'), r'([^,]*край[^,]*)', 1),
                    regexp_extract(col('address'), r'([^,]*республика[^,]*)', 1),
                    regexp_extract(col('address'), r'([^,]*АО[^,]*)', 1)
                )
            )
        
        # ШАГ 6: brand = vendor (ВТМ для Дикси)
        if 'vendor' in df.columns:
            df = df.withColumn('brand', col('vendor'))
        
        # ШАГ 7: Замена запятых на точки в числах
        for col_name in ['sales_amount_rub', 'sales_cost_price']:
            if col_name in df.columns:
                df = df.withColumn(col_name, regexp_replace(col(col_name), ',', '.'))
        
        # ШАГ 8: Недостающие колонки
        remaining_cols = set(SILVER_COLUMNS.keys()) - set(df.columns)
        print(f'Оставшиеся колонки (заполним NULL): {remaining_cols}')
        
        for column in remaining_cols:
            df = df.withColumn(column, lit(None))
        
        # ШАГ 9: Приведение типов
        for column in df.columns:
            dtype = SILVER_COLUMNS.get(column)
            if dtype:
                df = df.withColumn(column, col(column).cast(dtype))
        
        print('Финальные колонки:')
        print(df.columns)
        print('=' * 100)
        
        # ШАГ 10: Финальный датафрейм
        final_df = df.select(*SILVER_COLUMNS.keys())
        
        # ШАГ 11: Фильтрация строк
        final_df = final_df.filter(
            (col('sales_quantity').isNull()) | (col('sales_quantity') > 0)
        )
        final_df = final_df.filter(
            (col('sales_amount_rub').isNull()) | (col('sales_amount_rub') > 0)
        )
        final_df = final_df.filter(
            (col('sales_cost_price').isNull()) | (col('sales_cost_price') > 0)
        )
        
        # ШАГ 12: Расчет средних цен
        final_df = final_df.withColumn(
            'average_cost_price', 
            coalesce(
                col('average_cost_price'), 
                when((col('sales_quantity').isNotNull()) & (col('sales_quantity') > 0), 
                     col('sales_cost_price') / col('sales_quantity'))
            )
        )
        
        final_df = final_df.withColumn(
            'average_sell_price', 
            coalesce(
                col('average_sell_price'), 
                when((col('sales_quantity').isNotNull()) & (col('sales_quantity') > 0), 
                     col('sales_amount_rub') / col('sales_quantity'))
            )
        )
        
        # ШАГ 13: Проверка на дубликаты и запись
        try:
            res = spark.sql(f'''
                SELECT COUNT(*) 
                FROM iceberg.diksi_silver.sales 
                WHERE file_name = '{file[:-4]}'
            ''').first()
            file_exists = res[0] > 0
        except Exception as e:
            logging.warning(f'⚠ Ошибка проверки дубликатов: {e}')
            file_exists = False
        
        if not file_exists:
            rows_before = final_df.count()
            print(f'Записываем {rows_before} строк в iceberg.diksi_silver.sales...')
            
            final_df.writeTo('iceberg.diksi_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()
            
            print(f'✓ Файл {file} успешно занесен в таблицу ({rows_before} строк)')
        else:
            print(f'⊘ Данные из файла {file} уже есть в таблице, пропускаем')
        
        print('=' * 100)
        print()

print('✅ Обработка Дикси завершена!')