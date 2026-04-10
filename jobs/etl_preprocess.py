from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import time, datetime
import os
import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

spark = SparkSession.builder \
    .appName('ETL Preprocess') \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

spark.sql('CREATE DATABASE IF NOT EXISTS iceberg.silver')

spark.sql("""
CREATE TABLE IF NOT EXISTS iceberg.silver.sales (
    -- Идентификаторы
    id                          INT,
    
    -- Время
    sale_date                   DATE,
    sale_year                   INT,
    sale_month                  INT,
    
    -- Локация
    retail_chain                STRING,
    branch                      STRING,
    region                      STRING,
    city                        STRING,
    address                     STRING,
    store_format                STRING,
    store_name                  STRING,
    
    -- Продукт
    product_name                STRING,
    brand                       STRING,
    flavor                      STRING,
    weight                      DOUBLE,
    product_type                STRING,
    package_type                STRING,
    
    -- Иерархия продукта
    product_level_1             STRING,
    product_level_2             STRING,
    product_level_3             STRING,
    product_level_4             STRING,
    
    -- Коды продукта
    product_family_code         STRING,
    product_family_name         STRING,
    product_article             STRING,
    product_code                STRING,
    barcode                     STRING,
    
    -- Производство
    factory_code                STRING,
    factory_name                STRING,
    material                    STRING,
    
    -- Поставщики
    vendor                      STRING,
    supplier                    STRING,
    warehouse_supplier          STRING,
    
    -- Метрики продаж
    sales_quantity              DOUBLE,
    sales_amount_rub            DOUBLE,
    sales_tons                  DOUBLE,
    sales_weight_kg             DOUBLE,
    
    -- Цены
    avg_cost_price              DOUBLE,
    avg_sell_price              DOUBLE,
    
    -- Суммы с НДС
    sales_amount_with_vat       DOUBLE,
    promo_sales_amount_with_vat DOUBLE,
    
    -- Списания и потери
    writeoff_quantity           DOUBLE,
    writeoff_amount_rub         DOUBLE,
    loss_quantity               DOUBLE,
    loss_amount_rub             DOUBLE,
    
    -- Маржа
    margin_amount_rub           DOUBLE,
    
    -- Технические
    source_file                 STRING,
    load_timestamp              TIMESTAMP
)
USING iceberg
PARTITIONED BY (retail_chain, sale_year, sale_month)"""
          )

spark.sql('truncate table iceberg.silver.sales')


s3 = boto3.client('s3', 
                  endpoint_url = 'http://minio:9000',
                  aws_access_key_id = 'minioadmin',
                  aws_secret_access_key = 'minioadmin')


SILVER_COLUMNS = {
    # === ИДЕНТИФИКАТОРЫ ===
    "id": "int",
    
    # === ВРЕМЯ ===
    "sale_date": "date",
    "sale_year": "int",
    "sale_month": "int",
    
    # === ЛОКАЦИЯ / МАГАЗИН ===
    "retail_chain": "string",
    "branch": "string",
    "region": "string",
    "city": "string",
    "address": "string",
    "store_format": "string",
    "store_name": "string",
    
    # === ПРОДУКТ ===
    "product_name": "string",
    "brand": "string",
    "flavor": "string",
    "weight": "double",
    "product_type": "string",
    "package_type": "string",
    
    # === ИЕРАРХИЯ ПРОДУКТА ===
    "product_level_1": "string",
    "product_level_2": "string",
    "product_level_3": "string",
    "product_level_4": "string",
    
    # === КОДЫ ПРОДУКТА ===
    "product_family_code": "string",
    "product_family_name": "string",
    "product_article": "string",
    "product_code": "string",
    "barcode": "string",
    
    # === ПРОИЗВОДСТВО ===
    "factory_code": "string",
    "factory_name": "string",
    "material": "string",
    
    # === ПОСТАВЩИКИ ===
    "vendor": "string",
    "supplier": "string",
    "warehouse_supplier": "string",
    
    # === МЕТРИКИ ПРОДАЖ ===
    "sales_quantity": "double",
    "sales_amount_rub": "double",
    "sales_tons": "double",
    "sales_weight_kg": "double",
    
    # === ЦЕНЫ ===
    "avg_cost_price": "double",
    "avg_sell_price": "double",
    
    # === СУММЫ С НДС ===
    "sales_amount_with_vat": "double",
    "promo_sales_amount_with_vat": "double",
    
    # === СПИСАНИЯ И ПОТЕРИ ===
    "writeoff_quantity": "double",
    "writeoff_amount_rub": "double",
    "loss_quantity": "double",
    "loss_amount_rub": "double",
    
    # === МАРЖА ===
    "margin_amount_rub": "double",
    
    # === ТЕХНИЧЕСКИЕ ===
    "source_file": "string",
    "load_timestamp": "timestamp",
}

# ============================================================
# МАППИНГ: грязное имя → чистое имя
# ============================================================
COLUMN_MAPPING = {
    # ========================================
    # ИДЕНТИФИКАТОРЫ
    # ========================================
    "_c0": "id",
    "Unnamed: 0": "id",
    "№": "id",
    "ID": "id",
    
    # ========================================
    # ВРЕМЯ / ПЕРИОД
    # ========================================
    "Период": "sale_date",
    "период": "sale_date",
    "Period": "sale_date",
    "Дата": "sale_date",
    "дата": "sale_date",
    "Date": "sale_date",
    "sale_date": "sale_date",
    
    "Год": "sale_year",
    "год": "sale_year",
    "Year": "sale_year",
    "sale_year": "sale_year",
    
    "Месяц": "sale_month",
    "месяц": "sale_month",
    "Month": "sale_month",
    "sale_month": "sale_month",
    
    # ========================================
    # ЛОКАЦИЯ / МАГАЗИН
    # ========================================
    "Сеть": "retail_chain",
    "Сеть ": "retail_chain",
    "сеть": "retail_chain",
    "Retail": "retail_chain",
    "ТС": "retail_chain",
    "Торговая сеть": "retail_chain",
    "retail_chain": "retail_chain",
    
    "Филиал": "branch",
    "Филиал ": "branch",
    "филиал": "branch",
    "Branch": "branch",
    "branch": "branch",
    
    "Регион": "region",
    "регион": "region",
    "Region": "region",
    "region": "region",
    
    "Город": "city",
    "Город ": "city",
    "город": "city",
    "City": "city",
    "city": "city",
    
    "Адрес": "address",
    "адрес": "address",
    "Address": "address",
    "address": "address",
    
    "Формат": "store_format",
    "Формат магазина": "store_format",
    "Store Format": "store_format",
    "store_format": "store_format",
    
    "Магазин": "store_name",
    "Название магазина": "store_name",
    "Store": "store_name",
    "store_name": "store_name",
    
    "РЦ": "warehouse_supplier",
    "Распределительный центр": "warehouse_supplier",
    "DC": "warehouse_supplier",
    
    "ТТ": "store_name",
    "Торговая точка": "store_name",
    
    # ========================================
    # ПРОДУКТ
    # ========================================
    "Наименование": "product_name",
    "наименование": "product_name",
    "Product": "product_name",
    "Товар": "product_name",
    "товар": "product_name",
    "product_name": "product_name",
    
    "Бренд": "brand",
    "Бренды": "brand",
    "бренд": "brand",
    "Brand": "brand",
    "brand": "brand",
    
    "Вкус": "flavor",
    "Вкусы": "flavor",
    "вкус": "flavor",
    "вкусы": "flavor",
    "Flavor": "flavor",
    "flavor": "flavor",
    
    "Граммовка": "weight",
    "граммовка": "weight",
    "Grammage": "weight",
    "Вес": "weight",
    "вес": "weight",
    "Weight": "weight",
    "weight": "weight",
    
    "Тип продукта": "product_type",
    "Тип": "product_type",
    "Product Type": "product_type",
    "product_type": "product_type",
    
    "Тип упаковки": "package_type",
    "Упаковка": "package_type",
    "Package": "package_type",
    "package_type": "package_type",
    
    # ========================================
    # ИЕРАРХИЯ ПРОДУКТА
    # ========================================
    "Категория": "product_level_1",
    "категория": "product_level_1",
    "Category": "product_level_1",
    "Категория 1": "product_level_1",
    "product_level_1": "product_level_1",
    
    "Категория 2": "product_level_2",
    "категория 2": "product_level_2",
    "Category 2": "product_level_2",
    "Подкатегория": "product_level_2",
    "Тип основы": "product_level_2",
    "product_level_2": "product_level_2",
    
    "Категория 3": "product_level_3",
    "Category 3": "product_level_3",
    "product_level_3": "product_level_3",
    
    "Категория 4": "product_level_4",
    "Category 4": "product_level_4",
    "product_level_4": "product_level_4",
    
    # ========================================
    # КОДЫ ПРОДУКТА
    # ========================================
    "Код семейства": "product_family_code",
    "Family Code": "product_family_code",
    "product_family_code": "product_family_code",
    
    "Семейство": "product_family_name",
    "Название семейства": "product_family_name",
    "Family": "product_family_name",
    "product_family_name": "product_family_name",
    
    "Артикул": "product_article",
    "артикул": "product_article",
    "Article": "product_article",
    "SKU": "product_article",
    "product_article": "product_article",
    
    "Код товара": "product_code",
    "Product Code": "product_code",
    "product_code": "product_code",
    
    "Штрихкод": "barcode",
    "штрихкод": "barcode",
    "Barcode": "barcode",
    "ШК": "barcode",
    "EAN": "barcode",
    "barcode": "barcode",
    
    # ========================================
    # ПРОИЗВОДСТВО
    # ========================================
    "Код завода": "factory_code",
    "Factory Code": "factory_code",
    "factory_code": "factory_code",
    
    "Завод": "factory_name",
    "Название завода": "factory_name",
    "Factory": "factory_name",
    "factory_name": "factory_name",
    
    "Материал": "material",
    "материал": "material",
    "Material": "material",
    "material": "material",
    
    # ========================================
    # ПОСТАВЩИКИ
    # ========================================
    "Вендор": "vendor",
    "вендор": "vendor",
    "Vendor": "vendor",
    "vendor": "vendor",
    
    "Поставщик": "supplier",
    "поставщик": "supplier",
    "Supplier": "supplier",
    "Поставщики": "supplier",
    "supplier": "supplier",
    
    "Склад поставщика": "warehouse_supplier",
    "Warehouse": "warehouse_supplier",
    "warehouse_supplier": "warehouse_supplier",
    
    # ========================================
    # МЕТРИКИ ПРОДАЖ
    # ========================================
    "Продажи, шт": "sales_quantity",
    "Продажи, шт ": "sales_quantity",
    "продажи, шт": "sales_quantity",
    "Sales Units": "sales_quantity",
    "Количество": "sales_quantity",
    "Кол-во": "sales_quantity",
    "sales_quantity": "sales_quantity",
    
    "Продажи, руб": "sales_amount_rub",
    "Продажи, руб ": "sales_amount_rub",
    "продажи, руб": "sales_amount_rub",
    "Sales RUB": "sales_amount_rub",
    "Сумма продаж": "sales_amount_rub",
    "sales_amount_rub": "sales_amount_rub",
    
    "Продажи, тонн": "sales_tons",
    "Продажи, тонн ": "sales_tons",
    "продажи, тонн": "sales_tons",
    "Sales Tons": "sales_tons",
    "sales_tons": "sales_tons",
    
    "Продажи, кг": "sales_weight_kg",
    "Sales KG": "sales_weight_kg",
    "sales_weight_kg": "sales_weight_kg",
    
    # ========================================
    # ЦЕНЫ
    # ========================================
    "Себест., руб": "avg_cost_price",
    "Себест., руб ": "avg_cost_price",
    "себест., руб": "avg_cost_price",
    "Cost RUB": "avg_cost_price",
    "Себест. Руб": "avg_cost_price",
    "Себестоимость": "avg_cost_price",
    "avg_cost_price": "avg_cost_price",
    
    "Цена продажи": "avg_sell_price",
    "Средняя цена": "avg_sell_price",
    "Sell Price": "avg_sell_price",
    "avg_sell_price": "avg_sell_price",
    
    # ========================================
    # СУММЫ С НДС
    # ========================================
    "Продажи с НДС": "sales_amount_with_vat",
    "Sales with VAT": "sales_amount_with_vat",
    "sales_amount_with_vat": "sales_amount_with_vat",
    
    "Промо продажи с НДС": "promo_sales_amount_with_vat",
    "Promo Sales": "promo_sales_amount_with_vat",
    "promo_sales_amount_with_vat": "promo_sales_amount_with_vat",
    
    # ========================================
    # СПИСАНИЯ И ПОТЕРИ
    # ========================================
    "Списания, шт": "writeoff_quantity",
    "Writeoff Qty": "writeoff_quantity",
    "writeoff_quantity": "writeoff_quantity",
    
    "Списания, руб": "writeoff_amount_rub",
    "Writeoff RUB": "writeoff_amount_rub",
    "writeoff_amount_rub": "writeoff_amount_rub",
    
    "Потери, шт": "loss_quantity",
    "Loss Qty": "loss_quantity",
    "loss_quantity": "loss_quantity",
    
    "Потери, руб": "loss_amount_rub",
    "Loss RUB": "loss_amount_rub",
    "loss_amount_rub": "loss_amount_rub",
    
    # ========================================
    # МАРЖА
    # ========================================
    "Маржа, руб": "margin_amount_rub",
    "Маржа": "margin_amount_rub",
    "Margin": "margin_amount_rub",
    "margin_amount_rub": "margin_amount_rub",
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