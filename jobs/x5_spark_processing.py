import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
import pandas as pd
from datetime import *


spark = SparkSession.builder \
    .appName('Iceberg Test') \
    .getOrCreate()

print(f"Версия Spark: {spark.version}")

spark.sql('''
    CREATE NAMESPACE IF NOT EXISTS iceberg.x5_silver
''')

spark.sql('''
    CREATE TABLE IF NOT EXISTS iceberg.x5_silver.sales (
        year                INT,
        month               STRING,
        retail_chain        STRING,
        district_name       STRING,
        region_name         STRING,
        city_name           STRING,
        address             STRING,
        factory_code        STRING,
        factory_name        STRING,
        product_category_2  STRING,
        product_category_3  STRING,
        product_category_4  STRING,
        product_id          INT,
        product_name        STRING,
        brand               STRING,
        vendor              STRING,
        retailer            STRING,
        retailer_rc         STRING,
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
    PARTITIONED BY (retail_chain, year, month)
''')


s3 = boto3.client('s3', 
                  endpoint_url = 'http://213.165.222.200:9000/',
                  aws_access_key_id = 'minioadmin',
                  aws_secret_access_key = 'minioadmin'
                 )

SILVER_COLUMNS = {
    'year': 'int',
    'month': 'string',
    'retail_chain': 'string',
    'district_name': 'string',
    'region_name': 'string',
    'city_name': 'string',
    'address': 'string',
    'factory_code': 'string',
    'factory_name': 'string',
    'product_category_2': 'string',
    'product_category_3': 'string',
    'product_category_4': 'string',
    'product_id': 'int',
    'product_name': 'string',
    'brand': 'string',
    'vendor': 'string',
    'retailer': 'string',
    'retailer_rc': 'string',
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
    'Сеть': 'retail_chain',
    'Филиал': 'district_name',
    'Регион': 'region_name',
    'Город': 'city_name',
    'Адрес': 'address', 
    'Завод': 'factory_code',
    'Завод2': 'factory_name',
    'Тов.иер.ур.2': 'product_category_2',
    'Тов.иер.ур.3': 'product_category_3', 
    'Тов.иер.ур.4': 'product_category_4',
    'Материал': 'product_id',
    'Материал2': 'product_name',
    'Бренд': 'brand',
    'Вендор': 'vendor',
    'Основной поставщик': 'retailer', 
    'Количество (без ед. изм.)': 'sales_quantity',
    'Оборот с НДС (без ед.изм.)': 'sales_amount_rub',
    'Общая себестоимость (с НДС) (без ед. изм.)': 'sales_cost_price', 
    'Средняя цена по себестоимости (с НДС)': 'average_cost_price',
    'Средняя цена продажи (с НДС)': 'average_sell_price',
    'Поставщик склада (РЦ)': 'retailer_rc'
}    

MONTH_MAPPING = {
    1: 'Январь',
    2: 'Февраль',
    3: 'Март',
    4: 'Апрель',
    5: 'Май',
    6: 'Июнь',
    7: 'Июль',
    8: 'Август',
    9: 'Сентябрь',
    10: 'Октябрь',
    11: 'Ноябрь',
    12: 'Декабрь',
    
    'january': 'Январь',
    'february': 'Февраль',
    'march': 'Март',
    'april': 'Апрель',
    'may': 'Май',
    'june': 'Июнь',
    'july': 'Июль',
    'august': 'Август',
    'september': 'Сентябрь',
    'october': 'Октябрь',
    'november': 'Ноябрь',
    'december': 'Декабрь'
}

MONTH_MAPPING_INT = {
    'Январь': 1,
    'Февраль': 2,
    'Март': 3,
    'Апрель': 4,
    'Май': 5,
    'Июнь': 6,
    'Июль': 7,
    'Август': 8,
    'Сентябрь': 9,
    'Октябрь': 10,
    'Ноябрь': 11,
    'Декабрь': 12
}




objects = s3.list_objects_v2(Bucket = 'raw')

files = objects['Contents']

year_cr = datetime.now().year
month_cr = datetime.now().month
day_cr = datetime.now().day

date_created = str(year_cr) + '-' + str(month_cr) + '-' + str(day_cr)

for object in files:
    
    file = object['Key']
    file_name = 's3a://raw/' + file
    
    print('=' * 100)
    print(file_name)
    print('=' * 100)
    
    df = spark.read.csv(file_name, sep=';')
    df = df.offset(1)
    
    first_row = list(df.first())[:-1]

    df = df.offset(1)
    
    input_cols = []
    
    for i in first_row:
        input_cols.append(str(i))
        
    df = df.select(df.columns[:-1])
    
    df = df.toDF(*input_cols)

    print('Исходные данные:')
    print('=' * 100)
    
    print(df.columns)

    # Сделать маппинг столбцов
    columns_name = df.columns
    for column in columns_name:
        new_name = SILVER_MAPPING.get(column)
        df = df.withColumnRenamed(column, new_name)

    print('Обновленные данные:')
    print(df.columns)

    # Создаем сслужебные столбцы
    
    df = df.withColumn('file_name', lit(file[:-4]))
    df = df.withColumn('created_at', lit(date_created))
    df = df.withColumn('updated_at', lit(date_created))

    # Исправляем проблему с запятыми в исходных данных (стоимоть товаров и т.д)
    if 'sales_amount_rub' in df.columns:
        df = df.withColumn('sales_amount_rub', regexp_replace(col('sales_amount_rub'), ',', '.'))
    
    if 'sales_cost_price' in df.columns:
        df = df.withColumn('sales_cost_price', regexp_replace(col('sales_cost_price'), ',', '.'))
    
    if 'average_cost_price' in df.columns:
        df = df.withColumn('average_cost_price', regexp_replace(col('average_cost_price'), ',', '.'))
    
    if 'average_sell_price' in df.columns:
        df = df.withColumn('average_sell_price', regexp_replace(col('average_sell_price'), ',', '.'))
    
    # Найти недостающие столбцы
    
    remaining_cols = set(SILVER_COLUMNS.keys()) - set(df.columns)
    print(remaining_cols)

    parts = file.split('_')

    month_from_file = parts[1]
    month_from_file_str = MONTH_MAPPING.get(month_from_file)
    month_from_file_int = MONTH_MAPPING_INT.get(month_from_file_str)
    
    df = df.withColumn('month', lit(month_from_file_str))

    parsed_year = parts[2]
    if 'csv' in parsed_year:
        parsed_year = parsed_year[:-4]
        
    df = df.withColumn('year', lit(int(parsed_year)))

    string_period = str(parsed_year) + '-' + str(month_from_file_int) + '-01'
    period_done = datetime.strptime(string_period, '%Y-%m-%d')
    
    df = df.withColumn('period', lit(period_done))

    # Замена значений в столбцах

    df = df.withColumn('retail_chain', regexp_replace(col('retail_chain'), 'Пятёрочка', 'Пятерочка'))
    df = df.withColumn('retail_chain', regexp_replace(col('retail_chain'), 'Перекрёсток', 'Перекресток'))
    df = df.withColumn('retail_chain', regexp_replace(col('retail_chain'), 'Перекрёсток-Джем', 'Перекресток'))
    
    remaining_cols = set(SILVER_COLUMNS.keys()) - set(df.columns)
    print(remaining_cols)

    print('=' * 100)
    print('Оставшиеся колонки:', remaining_cols)
    print('=' * 100)
    
    # Вставялем null в ненайденные колонки

    if 'retail_chain' not in df.columns:
        df = df.withColumn('retail_chain', lit(parts[0]))
    
    for column in remaining_cols:
        df = df.withColumn(column, lit(None))
    
    # Приводим колонки к соотвутсвующему типу данных
    for column in df.columns:
        dtype = SILVER_COLUMNS.get(column)
        df = df.withColumn(column, col(column).cast(dtype))
        
    print('=' * 100)
    print('Финальные колонки')
    print('=' * 100)
    print(df.columns)

    final_df = df.select(*SILVER_COLUMNS.keys())

    # Фильтрация корректных строк
    final_df = final_df.filter(final_df.sales_quantity > 0)
    final_df = final_df.filter(final_df.sales_amount_rub > 0)
    final_df = final_df.filter(final_df.sales_cost_price > 0)

    # Расчет недостающих колонок
    
    final_df = final_df.withColumn('average_cost_price', coalesce(col('average_cost_price'), col('sales_cost_price') / col('sales_quantity')))
    final_df = final_df.withColumn('average_sell_price', coalesce(col('average_sell_price'), col('sales_amount_rub') / col('sales_quantity')))


    final_df.filter(final_df.average_cost_price > 0)
    final_df.filter(final_df.average_sell_price > 0)

    
    res = spark.sql(f'''
        select count(*) from iceberg.x5_silver.sales
        where file_name = '{file[:-4]}'
    '''
    ).first()

    if res[0] == 0:
        
        final_df.writeTo('iceberg.x5_silver.sales').partitionedBy('retail_chain', 'year', 'month').append()
        print(f'Файл {file} занесен в таблицу')
    else:
    
        print('Данные уже есть в таблице, не заносим')




    
