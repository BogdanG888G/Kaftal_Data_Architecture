import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import re as re_m

spark = SparkSession.builder.appName('Iceberg Okey ETL').getOrCreate()
print(f"✓ Spark: {spark.version}")

# ============================================================
# КОНСТАНТЫ
# ============================================================
SILVER_COLUMNS = [
    'year', 'month', 'retail_chain', 'region_name', 'city_name', 'store_code',
    'product_category_2', 'product_category_3', 'product_category_4', 'product_category_5',
    'product_id', 'product_name', 'product_uni_name', 'brand', 'vendor', 'flavor', 'weight_grams',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price', 'sales_tons',
    'average_cost_price', 'average_sell_price', 'margin_rub', 'margin_pct', 'cost_price_rub',
    'max_sell_price', 'max_sell_price_region', 'max_cost_price',
    'stock_qty', 'stock_rub',
    'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'region_name': 'string', 'city_name': 'string', 'store_code': 'string',
    'product_category_2': 'string', 'product_category_3': 'string',
    'product_category_4': 'string', 'product_category_5': 'string',
    'product_id': 'string', 'product_name': 'string', 'product_uni_name': 'string',
    'brand': 'string', 'vendor': 'string', 'flavor': 'string', 'weight_grams': 'string',
    'sales_quantity': 'int', 'sales_amount_rub': 'float', 'sales_cost_price': 'float',
    'sales_tons': 'float', 'average_cost_price': 'float', 'average_sell_price': 'float',
    'margin_rub': 'float', 'margin_pct': 'float', 'cost_price_rub': 'float',
    'max_sell_price': 'float', 'max_sell_price_region': 'float', 'max_cost_price': 'float',
    'stock_qty': 'int', 'stock_rub': 'float',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
}

MAPPING_V1 = {
    'Период': 'period_raw', 'Сеть': 'retail_chain_raw',
    'Категория': 'product_category_3', 'Категория 2': 'product_category_4',
    'Поставщик': 'vendor', 'Бренд': 'brand', 'Наименование': 'product_name',
    'УНИ Наименование': 'product_uni_name', 'Граммовка': 'weight_grams', 'Вкус': 'flavor',
    'Продажи, шт': 'sales_quantity', 'Продажи, руб': 'sales_amount_rub',
    'Продажи, тонн': 'sales_tons', 'Себест., руб': 'sales_cost_price',
}

MAPPING_V2 = {
    'Region': 'region_name', 'Inventlocationid': 'store_code',
    'Категория': 'product_category_2', 'Категория_2': 'product_category_3',
    'Группа': 'product_category_4', 'Подгруппа': 'product_category_5',
    'product_name': 'product_name', 'Configid': 'product_id', 'Бренд': 'brand',
    'Выручка (руб)': 'sales_amount_rub', 'Маржинальный доход (руб)': 'margin_rub',
    'Себестоимость продаж (руб)': 'sales_cost_price',
    'Максимальная продажная цена': 'max_sell_price',
    'Максимальная продажная цена региона': 'max_sell_price_region',
    'Максимальная закупочная цена': 'max_cost_price',
    'Маржа %': 'margin_pct', 'Себестоимость закупки  (руб)': 'cost_price_rub',
    'Продажа (шт)': 'sales_quantity',
    'Остаток на конец периода шт': 'stock_qty', 'Остаток на конец периода руб': 'stock_rub',
}

MONTH_MAPPING = {
    'january': 'Январь', 'february': 'Февраль', 'march': 'Март', 'april': 'Апрель',
    'may': 'Май', 'june': 'Июнь', 'july': 'Июль', 'august': 'Август',
    'september': 'Сентябрь', 'october': 'Октябрь', 'november': 'Ноябрь', 'december': 'Декабрь',
    'ceptember': 'Сентябрь',
}
MONTH_MAPPING_INT = {v: k for k, v in {1: 'Январь', 2: 'Февраль', 3: 'Март', 4: 'Апрель', 5: 'Май', 6: 'Июнь', 7: 'Июль', 8: 'Август', 9: 'Сентябрь', 10: 'Октябрь', 11: 'Ноябрь', 12: 'Декабрь'}.items()}
SHORT_MONTH = {'jan': 'january', 'feb': 'february', 'mar': 'march', 'apr': 'april', 'may': 'may', 'jun': 'june', 'jul': 'july', 'aug': 'august', 'sep': 'september', 'oct': 'october', 'nov': 'november', 'dec': 'december'}

s3 = boto3.client('s3', endpoint_url='http://minio:9000', aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin')
objects = s3.list_objects_v2(Bucket='raw', Prefix='okey_')

if 'Contents' not in objects:
    print("⚠ Нет файлов okey_")
else:
    y, m, d = datetime.now().year, datetime.now().month, datetime.now().day
    date_created = f'{y}-{m:02d}-{d:02d}'

    for obj in objects['Contents']:
        file = obj['Key']
        if not file.endswith('.csv'): continue
        
        file_name = f's3a://raw/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        
        df = spark.read.csv(file_name, sep=';', header=True, inferSchema=False)
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')
        print(f'Колонки: {df.columns}')
        print('=' * 100)
        
        is_v1 = 'Период' in df.columns
        MAPPING = MAPPING_V1 if is_v1 else MAPPING_V2
        
        # ШАГ 1: Переименование
        for old_name in df.columns:
            new_name = MAPPING.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)
        
        # ШАГ 2: Служебные
        df = df.withColumn('retail_chain', F.lit('Окей'))
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))
        
        # ШАГ 3: Месяц и год
        parts = file.replace('.csv', '').split('_')
        month_str, parsed_year = None, None
        for i, part in enumerate(parts):
            mc = MONTH_MAPPING.get(part.lower())
            if mc:
                month_str = mc
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    parsed_year = int(parts[i + 1])
                break
        
        if 'period_raw' in df.columns:
            first = df.select('period_raw').first()
            if first and first[0]:
                p = str(first[0])
                mm = re_m.search(r'-([A-Za-z]{3})', p)
                if mm:
                    m3 = mm.group(1).lower()
                    full = SHORT_MONTH.get(m3, m3)
                    if month_str is None: month_str = MONTH_MAPPING.get(full)
                ym = re_m.search(r'(\d{2})-', p)
                if ym and parsed_year is None: parsed_year = 2000 + int(ym.group(1))
            df = df.drop('period_raw')
        
        if 'retail_chain_raw' in df.columns: df = df.drop('retail_chain_raw')
        
        if month_str is None: month_str = 'Неизвестно'
        month_int = MONTH_MAPPING_INT.get(month_str, 1)
        if parsed_year is None: parsed_year = y
        
        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(parsed_year))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', F.lit(period_done))
        
        # ШАГ 4: Запятые → точки
        for col_name in df.columns:
            if col_name in SILVER_TYPES and SILVER_TYPES[col_name] in ('float', 'int'):
                try:
                    df = df.withColumn(col_name, F.regexp_replace(df[col_name].cast('string'), ',', '.'))
                except: pass
        
        # ШАГ 5: Средние цены
        if 'sales_quantity' in df.columns and 'sales_cost_price' in df.columns:
            df = df.withColumn('average_cost_price',
                F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0),
                     df['sales_cost_price'] / df['sales_quantity']))
        
        if 'sales_quantity' in df.columns and 'sales_amount_rub' in df.columns:
            df = df.withColumn('average_sell_price',
                F.when(df['sales_quantity'].isNotNull() & (df['sales_quantity'] > 0),
                     df['sales_amount_rub'] / df['sales_quantity']))
        
        # ШАГ 6: Недостающие колонки
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        for col in remaining:
            df = df.withColumn(col, F.lit(None))
        
        # ШАГ 7: Типы
        for col_name in df.columns:
            dtype_str = SILVER_TYPES.get(col_name)
            if dtype_str:
                try:
                    df = df.withColumn(col_name, df[col_name].cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {col_name}: {e}")
        
        print(f'Финальные колонки: {df.columns}')
        print('=' * 100)
        
        # ШАГ 8: Запись
        final_df = df.select(*SILVER_COLUMNS)
        
        try:
            res = spark.sql(f"SELECT COUNT(*) FROM iceberg.okey_silver.sales WHERE file_name = '{file[:-4]}'").first()
            file_exists = res[0] > 0
        except: file_exists = False
        
        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            final_df.writeTo('iceberg.okey_silver.sales').partitionedBy('retail_chain', 'year', 'month').append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')
        print('=' * 100)
        print()

print('✅ Обработка Окей завершена!')