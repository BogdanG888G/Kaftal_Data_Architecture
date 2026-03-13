from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import time, datetime
import os
import boto3
import logging


logging.getLogger().setLevel(logging.ERROR)

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 pyspark-shell'

spark = SparkSession.builder \
    .appName('Iceberg Test') \
    .master('local[*]') \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0") \
    .config("spark.sql.extensions", 
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", 
            "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql('CREATE DATABASE IF NOT EXISTS iceberg.silver')


spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.silver.sales (
        -- Основные колонки
        id                   STRING,
        period               STRING,
        retail_chain         STRING,
        category             STRING,
        category_2           STRING,
        supplier             STRING,
        brand                STRING,
        product_name         STRING,
        uni_product_name     STRING,
        grammage             STRING,
        flavor               STRING,
        sales_units          DOUBLE,
        sales_rub            DOUBLE,
        sales_tons           DOUBLE,
        cost_rub             DOUBLE,
        
        -- Служебные колонки
        year                 INT,
        month                INT,
        source_file          STRING,
        
        -- Новые колонки из декабря
        branch               STRING,
        region               STRING,
        city                 STRING,
        address              STRING,
        distribution_center  STRING,
        trade_point          STRING
    )
    USING iceberg
    PARTITIONED BY (retail_chain, year, month)
""")


s3 = boto3.client('s3', 
                  endpoint_url = 'http://minio:9000',
                  aws_access_key_id = 'minioadmin',
                  aws_secret_access_key = 'minioadmin')


SILVER_COLUMNS = {
    "id": "string",
    "period": "string",
    "retail_chain": "string",
    "category": "string",
    "category_2": "string",
    "supplier": "string",
    "brand": "string",
    "product_name": "string",
    "uni_product_name": "string",
    "grammage": "string",
    "flavor": "string",
    "sales_units": "double",
    "sales_rub": "double",
    "sales_tons": "double",
    "cost_rub": "double",
    "year": "int",
    "month": "int",
    "source_file": "string",
    "branch": "string",
    "region": "string",
    "city": "string",
    "address": "string",
    "distribution_center": "string",
    "trade_point": "string",
}

# ============================================================
# МАППИНГ: грязное имя → чистое имя
# ============================================================
COLUMN_MAPPING = {
    # ID
    "_c0": "id",
    
    # Период
    "Период": "period",
    "период": "period",
    "Period": "period",
    
    # Сеть
    "Сеть": "retail_chain",
    "Сеть ": "retail_chain",
    "сеть": "retail_chain",
    "Retail": "retail_chain",
    
    # Категории
    "Категория": "category",
    "категория": "category",
    "Category": "category",
    
    "Категория 2": "category_2",
    "категория 2": "category_2",
    "Category 2": "category_2",
    'Тип основы': 'category_2',
    
    # Поставщик
    "Поставщик": "supplier",
    "поставщик": "supplier",
    "Supplier": "supplier",
    "Поставщики": "supplier",
    
    # Бренд
    "Бренд": "brand",
    "Бренды": "brand",
    "бренд": "brand",
    "Brand": "brand",
    
    # Наименование
    "Наименование": "product_name",
    "наименование": "product_name",
    "Product": "product_name",
    
    # УНИ Наименование
    "УНИ Наименование": "uni_product_name",
    "уни наименование": "uni_product_name",
    "UNI Name": "uni_product_name",
    
    # Граммовка
    "Граммовка": "grammage",
    "граммовка": "grammage",
    "Grammage": "grammage",
    
    # Вкус / Вкусы → единое поле
    "Вкус": "flavor",
    "Вкусы": "flavor",
    "вкус": "flavor",
    "вкусы": "flavor",
    "Flavor": "flavor",
    
    # Продажи (с пробелами и без)
    "Продажи, шт": "sales_units",
    "Продажи, шт ": "sales_units",
    "продажи, шт": "sales_units",
    "Sales Units": "sales_units",
    
    "Продажи, руб": "sales_rub",
    "Продажи, руб ": "sales_rub",
    "продажи, руб": "sales_rub",
    "Sales RUB": "sales_rub",
    
    "Продажи, тонн": "sales_tons",
    "Продажи, тонн ": "sales_tons",
    "продажи, тонн": "sales_tons",
    "Sales Tons": "sales_tons",
    
    # Себестоимость
    "Себест., руб": "cost_rub",
    "Себест., руб ": "cost_rub",
    "себест., руб": "cost_rub",
    "Cost RUB": "cost_rub",
    'Себест. Руб': "cost_rub",

    "Филиал ": "branch",           
    "Регион": "region",            
    "Город ": "city",               
    "Адрес": "address",             
    "РЦ": "distribution_center",    
    "ТТ": "trade_point",            
}

# ============================================================
# МАППИНГ МЕСЯЦЕВ
# ============================================================
MONTH_MAPPING = {
    "january": 1, "february": 2, "march": 3,
    "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9,
    "ceptember": 9,
    "october": 10, "november": 11, "december": 12,
}



files = s3.list_objects(Bucket = 'data')['Contents']

for i in files:
    logging.info(i['Key'])
    
    filename = 's3a://' + 'data/' + i['Key']

    logging.info(f'Обрабатываем файл {filename}')
    
    df = spark.read.csv(filename, header = True)

    logging.info(df.columns)

    for column in df.columns:

        new_col = COLUMN_MAPPING[column]
        new_dtype = SILVER_COLUMNS[new_col]

        df = df.withColumnRenamed(column, new_col)
        df = df.withColumn(new_col, col(new_col).cast(new_dtype))
        
        logging.info(f'Колонка {column} преобразована в {new_col} с типом данных {SILVER_COLUMNS[new_col]}')

    logging.info('Переименованные колонки:')
    logging.info(df.printSchema())

    year_d = month_d = None
    
    if 'period' in df.columns:
        df = df.withColumn('period', to_date(col('period'), 'yyyy-MM-dd'))
        df = df.withColumn('year', year(col('period')).cast('int'))
        df = df.withColumn('month', month(col('period')).cast('int'))
    else:
        for part in filename.split('/'):
            if part.isdigit() and len(part) == 4:
                year_d = int(part)
            elif part.lower() in MONTH_MAPPING:
                month_d = MONTH_MAPPING[part.lower()]
        
        df = df.withColumn('year', lit(year_d).cast('int'))
        df = df.withColumn('month', lit(month_d).cast('int'))
            
        
    df = df.withColumn('source_file', lit(filename))
    
    logging.info('Недостающие колонки')
    for col_rest in SILVER_COLUMNS:
        if col_rest not in df.columns:
            df = df.withColumn(col_rest, lit(None))

    logging.info('Конечный (silver) вариант:')
    logging.info(df.printSchema())
    

    #if spark.sql(f'SELECT COUNT(*) FROM iceberg.silver.sales where source_file = {filename}') != 
    
    df.writeTo('iceberg.silver.sales') \
    .using('iceberg') \
    .partitionedBy(col('retail_chain'), col('year'), col('month')) \
    .append()

    logging.info('Данные занесены в табличку iceberg')