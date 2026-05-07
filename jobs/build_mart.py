"""
Сборка единой витрины из 8 сетей + LEFT JOIN со справочником товаров
Параллельная распределенная запись в ClickHouse напрямую с экзекуторов
Фильтрация: Только чипсы (картофельные, кукурузные и т.д.)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import clickhouse_connect
import time

spark = (
    SparkSession.builder
    .appName('Build Sales Mart')
    .config('spark.sql.adaptive.enabled', 'true')
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
    .config('spark.sql.adaptive.skewJoin.enabled', 'true')
    .config('spark.driver.memory', '4g')
    # Фикс Iceberg "Connection pool shut down"
    .config("spark.sql.catalog.iceberg.cache-enabled", "false")
    .getOrCreate()
)

print(f"✓ Spark: {spark.version}")

# ============================================================
# Функции унификации
# ============================================================
def select_x5(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.coalesce(F.col('region_name'), F.lit('')).alias('region_name'),
        F.coalesce(F.col('city_name'), F.lit('')).alias('city_name'),
        F.coalesce(F.col('address'), F.lit('')).alias('address'),
        F.coalesce(F.col('district_name'), F.lit('')).alias('store_code'),
        F.coalesce(F.col('factory_name'), F.lit('')).alias('store_name'),
        F.coalesce(F.col('factory_code'), F.lit('')).alias('store_format'),
        F.coalesce(F.col('product_category_2'), F.lit('')).alias('product_category_2'),
        F.coalesce(F.col('product_category_3'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('product_category_4'), F.lit('')).alias('product_category_4'),
        F.lit('').alias('product_category_5'),
        F.coalesce(F.col('product_id').cast('string'), F.lit('')).alias('product_id'),
        F.col('product_name').alias('product_name_search'),
        F.lit(None).cast('string').alias('product_uni_name'),
        F.coalesce(F.col('brand'), F.lit('')).alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.lit('').alias('flavor'),
        F.lit('').alias('weight_grams'),
        F.lit('').alias('barcode'),
        F.lit('').alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.lit(None).cast('double').alias('sales_kg'),
        F.lit(None).cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.lit(None).cast('double').alias('margin_rub'),
        F.lit(None).cast('double').alias('margin_pct'),
        F.lit(None).cast('double').alias('cost_price_rub'),
        F.lit(None).cast('double').alias('max_sell_price'),
        F.lit(None).cast('double').alias('max_cost_price'),
        F.lit(None).cast('int').alias('stock_qty'),
        F.lit(None).cast('double').alias('stock_rub'),
        F.lit(None).cast('double').alias('promo_sales_rub'),
        F.lit(None).cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

def select_diksi(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.coalesce(F.col('region_name'), F.lit('')).alias('region_name'),
        F.coalesce(F.col('city_name'), F.lit('')).alias('city_name'),
        F.coalesce(F.col('address'), F.lit('')).alias('address'),
        F.coalesce(F.col('store_code'), F.lit('')).alias('store_code'),
        F.lit('').alias('store_name'),
        F.lit('').alias('store_format'),
        F.lit('').alias('product_category_2'),
        F.coalesce(F.col('product_category_3'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('product_category_4'), F.lit('')).alias('product_category_4'),
        F.coalesce(F.col('product_category_5'), F.lit('')).alias('product_category_5'),
        F.coalesce(F.col('product_id'), F.lit('')).alias('product_id'),
        F.col('product_name').alias('product_name_search'),
        F.lit(None).cast('string').alias('product_uni_name'),
        F.coalesce(F.col('brand'), F.lit('')).alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.lit('').alias('flavor'),
        F.lit('').alias('weight_grams'),
        F.lit('').alias('barcode'),
        F.lit('').alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.lit(None).cast('double').alias('sales_kg'),
        F.lit(None).cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.lit(None).cast('double').alias('margin_rub'),
        F.lit(None).cast('double').alias('margin_pct'),
        F.lit(None).cast('double').alias('cost_price_rub'),
        F.lit(None).cast('double').alias('max_sell_price'),
        F.lit(None).cast('double').alias('max_cost_price'),
        F.lit(None).cast('int').alias('stock_qty'),
        F.lit(None).cast('double').alias('stock_rub'),
        F.lit(None).cast('double').alias('promo_sales_rub'),
        F.lit(None).cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

def select_magnit(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.coalesce(F.col('region_name'), F.lit('')).alias('region_name'),
        F.coalesce(F.col('city_name'), F.lit('')).alias('city_name'),
        F.coalesce(F.col('address'), F.lit('')).alias('address'),
        F.coalesce(F.col('store_code'), F.lit('')).alias('store_code'),
        F.coalesce(F.col('store_name'), F.lit('')).alias('store_name'),
        F.coalesce(F.col('store_format'), F.lit('')).alias('store_format'),
        F.coalesce(F.col('product_category_2'), F.lit('')).alias('product_category_2'),
        F.coalesce(F.col('product_category_3'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('product_category_4'), F.lit('')).alias('product_category_4'),
        F.coalesce(F.col('product_category_5'), F.lit('')).alias('product_category_5'),
        F.coalesce(F.col('product_id'), F.lit('')).alias('product_id'),
        F.col('product_name').alias('product_name_search'),
        F.lit(None).cast('string').alias('product_uni_name'),
        F.coalesce(F.col('brand'), F.lit('')).alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.lit('').alias('flavor'),
        F.lit('').alias('weight_grams'),
        F.coalesce(F.col('barcode'), F.lit('')).alias('barcode'),
        F.lit('').alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.lit(None).cast('double').alias('sales_kg'),
        F.lit(None).cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.lit(None).cast('double').alias('margin_rub'),
        F.lit(None).cast('double').alias('margin_pct'),
        F.lit(None).cast('double').alias('cost_price_rub'),
        F.lit(None).cast('double').alias('max_sell_price'),
        F.lit(None).cast('double').alias('max_cost_price'),
        F.lit(None).cast('int').alias('stock_qty'),
        F.lit(None).cast('double').alias('stock_rub'),
        F.lit(None).cast('double').alias('promo_sales_rub'),
        F.lit(None).cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

def select_aushan(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.coalesce(F.col('region_name'), F.lit('')).alias('region_name'),
        F.coalesce(F.col('city_name'), F.lit('')).alias('city_name'),
        F.coalesce(F.col('address'), F.lit('')).alias('address'),
        F.coalesce(F.col('store_code'), F.lit('')).alias('store_code'),
        F.lit('').alias('store_name'),
        F.coalesce(F.col('store_format'), F.lit('')).alias('store_format'),
        F.lit('').alias('product_category_2'),
        F.coalesce(F.col('product_segment'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('family_name'), F.lit('')).alias('product_category_4'),
        F.coalesce(F.col('family_code').cast('string'), F.lit('')).alias('product_category_5'),
        F.coalesce(F.col('product_id'), F.lit('')).alias('product_id'),
        F.col('product_name').alias('product_name_search'),
        F.lit(None).cast('string').alias('product_uni_name'),
        F.lit('').alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.lit('').alias('flavor'),
        F.lit('').alias('weight_grams'),
        F.lit('').alias('barcode'),
        F.lit('').alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.col('sales_kg').cast('double').alias('sales_kg'),
        F.lit(None).cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.col('margin_rub').cast('double').alias('margin_rub'),
        F.lit(None).cast('double').alias('margin_pct'),
        F.lit(None).cast('double').alias('cost_price_rub'),
        F.lit(None).cast('double').alias('max_sell_price'),
        F.lit(None).cast('double').alias('max_cost_price'),
        F.lit(None).cast('int').alias('stock_qty'),
        F.lit(None).cast('double').alias('stock_rub'),
        F.col('promo_sales_rub').cast('double').alias('promo_sales_rub'),
        F.col('week_num').cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

def select_okey(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.coalesce(F.col('region_name'), F.lit('')).alias('region_name'),
        F.coalesce(F.col('city_name'), F.lit('')).alias('city_name'),
        F.lit('').alias('address'),
        F.coalesce(F.col('store_code'), F.lit('')).alias('store_code'),
        F.lit('').alias('store_name'),
        F.lit('').alias('store_format'),
        F.coalesce(F.col('product_category_2'), F.lit('')).alias('product_category_2'),
        F.coalesce(F.col('product_category_3'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('product_category_4'), F.lit('')).alias('product_category_4'),
        F.coalesce(F.col('product_category_5'), F.lit('')).alias('product_category_5'),
        F.coalesce(F.col('product_id'), F.lit('')).alias('product_id'),
        F.when(F.col('product_uni_name').isNotNull(), F.col('product_uni_name'))
         .otherwise(F.col('product_name')).alias('product_name_search'),
        F.col('product_uni_name').alias('product_uni_name'),
        F.coalesce(F.col('brand'), F.lit('')).alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.coalesce(F.col('flavor'), F.lit('')).alias('flavor'),
        F.coalesce(F.col('weight_grams'), F.lit('')).alias('weight_grams'),
        F.lit('').alias('barcode'),
        F.lit('').alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.lit(None).cast('double').alias('sales_kg'),
        F.col('sales_tons').cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.col('margin_rub').cast('double').alias('margin_rub'),
        F.col('margin_pct').cast('double').alias('margin_pct'),
        F.col('cost_price_rub').cast('double').alias('cost_price_rub'),
        F.col('max_sell_price').cast('double').alias('max_sell_price'),
        F.col('max_cost_price').cast('double').alias('max_cost_price'),
        F.col('stock_qty').cast('int').alias('stock_qty'),
        F.col('stock_rub').cast('double').alias('stock_rub'),
        F.lit(None).cast('double').alias('promo_sales_rub'),
        F.lit(None).cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

def select_perekrestok(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.coalesce(F.col('region_name'), F.lit('')).alias('region_name'),
        F.coalesce(F.col('city_name'), F.lit('')).alias('city_name'),
        F.coalesce(F.col('address'), F.lit('')).alias('address'),
        F.coalesce(F.col('address'), F.lit('')).alias('store_code'),
        F.lit('').alias('store_name'),
        F.lit('').alias('store_format'),
        F.lit('').alias('product_category_2'),
        F.coalesce(F.col('product_category_3'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('product_category_4'), F.lit('')).alias('product_category_4'),
        F.coalesce(F.col('base_type'), F.lit('')).alias('product_category_5'),
        F.coalesce(F.col('product_id'), F.lit('')).alias('product_id'),
        F.when(F.col('product_uni_name').isNotNull(), F.col('product_uni_name'))
         .otherwise(F.col('product_name')).alias('product_name_search'),
        F.col('product_uni_name').alias('product_uni_name'),
        F.coalesce(F.col('brand'), F.lit('')).alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.coalesce(F.col('flavor'), F.lit('')).alias('flavor'),
        F.coalesce(F.col('weight_grams'), F.lit('')).alias('weight_grams'),
        F.lit('').alias('barcode'),
        F.lit('').alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.lit(None).cast('double').alias('sales_kg'),
        F.col('sales_tons').cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.lit(None).cast('double').alias('margin_rub'),
        F.lit(None).cast('double').alias('margin_pct'),
        F.lit(None).cast('double').alias('cost_price_rub'),
        F.lit(None).cast('double').alias('max_sell_price'),
        F.lit(None).cast('double').alias('max_cost_price'),  # Исправлено: Передаем Null, так как колонки нет
        F.lit(None).cast('int').alias('stock_qty'),
        F.lit(None).cast('double').alias('stock_rub'),
        F.lit(None).cast('double').alias('promo_sales_rub'),
        F.lit(None).cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

def select_pyaterochka(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.lit('').alias('region_name'),
        F.lit('').alias('city_name'),
        F.lit('').alias('address'),
        F.lit('').alias('store_code'),
        F.lit('').alias('store_name'),
        F.lit('').alias('store_format'),
        F.lit('').alias('product_category_2'),
        F.coalesce(F.col('product_category_3'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('product_category_4'), F.lit('')).alias('product_category_4'),
        F.coalesce(F.col('base_type'), F.lit('')).alias('product_category_5'),
        F.lit('').alias('product_id'),
        F.when(F.col('product_uni_name').isNotNull(), F.col('product_uni_name'))
         .otherwise(F.col('product_name')).alias('product_name_search'),
        F.col('product_uni_name').alias('product_uni_name'),
        F.coalesce(F.col('brand'), F.lit('')).alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.coalesce(F.col('flavor'), F.lit('')).alias('flavor'),
        F.coalesce(F.col('weight_grams'), F.lit('')).alias('weight_grams'),
        F.lit('').alias('barcode'),
        F.lit('').alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.lit(None).cast('double').alias('sales_kg'),
        F.col('sales_tons').cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.lit(None).cast('double').alias('margin_rub'),
        F.lit(None).cast('double').alias('margin_pct'),
        F.lit(None).cast('double').alias('cost_price_rub'),
        F.lit(None).cast('double').alias('max_sell_price'),
        F.lit(None).cast('double').alias('max_cost_price'),
        F.lit(None).cast('int').alias('stock_qty'),
        F.lit(None).cast('double').alias('stock_rub'),
        F.lit(None).cast('double').alias('promo_sales_rub'),
        F.lit(None).cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

def select_vernyi(df):
    return df.select(
        F.col('year').cast('int').alias('year'),
        F.col('month').alias('month'),
        F.col('retail_chain').alias('retail_chain'),
        F.coalesce(F.col('region_name'), F.lit('')).alias('region_name'),
        F.coalesce(F.col('city_name'), F.lit('')).alias('city_name'),
        F.coalesce(F.col('address'), F.lit('')).alias('address'),
        F.coalesce(F.col('store_code'), F.lit('')).alias('store_code'),
        F.lit('').alias('store_name'),
        F.lit('').alias('store_format'),
        F.lit('').alias('product_category_2'),
        F.coalesce(F.col('product_category_3'), F.lit('')).alias('product_category_3'),
        F.coalesce(F.col('product_category_4'), F.lit('')).alias('product_category_4'),
        F.lit('').alias('product_category_5'),
        F.coalesce(F.col('product_id'), F.lit('')).alias('product_id'),
        F.col('product_name').alias('product_name_search'),
        F.lit(None).cast('string').alias('product_uni_name'),
        F.lit('').alias('brand'),
        F.coalesce(F.col('vendor'), F.lit('')).alias('vendor'),
        F.lit('').alias('flavor'),
        F.lit('').alias('weight_grams'),
        F.lit('').alias('barcode'),
        F.coalesce(F.col('manufacturer'), F.lit('')).alias('manufacturer'),
        F.col('sales_quantity').cast('int').alias('sales_quantity'),
        F.col('sales_amount_rub').cast('double').alias('sales_amount_rub'),
        F.col('sales_cost_price').cast('double').alias('sales_cost_price'),
        F.lit(None).cast('double').alias('sales_kg'),
        F.lit(None).cast('double').alias('sales_tons'),
        F.col('average_cost_price').cast('double').alias('average_cost_price'),
        F.col('average_sell_price').cast('double').alias('average_sell_price'),
        F.lit(None).cast('double').alias('margin_rub'),
        F.lit(None).cast('double').alias('margin_pct'),
        F.lit(None).cast('double').alias('cost_price_rub'),
        F.lit(None).cast('double').alias('max_sell_price'),
        F.lit(None).cast('double').alias('max_cost_price'),
        F.lit(None).cast('int').alias('stock_qty'),
        F.lit(None).cast('double').alias('stock_rub'),
        F.lit(None).cast('double').alias('promo_sales_rub'),
        F.col('week_num').cast('int').alias('week_num'),
        F.col('file_name').alias('file_name'),
        F.col('period').cast('string').alias('date'),
    )

# ============================================================
# Свойства метаданных ClickHouse
# ============================================================
def get_ch_client():
    return clickhouse_connect.get_client(
        host='clickhouse', port=8123, username='admin', password='123', database='default'
    )

# ============================================================
# Распределенный параллельный писатель (работает на экзекуторах)
# ============================================================
def insert_partition_to_clickhouse(iterator):
    import clickhouse_connect
    import math
    from datetime import date, datetime

    # Вспомогательные функции сериализуются внутри экзекутора
    def normalize_type(ch_type: str):
        nullable = False
        t = ch_type
        while True:
            if t.startswith('Nullable('):
                nullable = True
                t = t[len('Nullable('):-1]
                continue
            if t.startswith('LowCardinality('):
                t = t[len('LowCardinality('):-1]
                continue
            break
        return t, nullable

    def cast_val(col_name, value, ch_type):
        base_type, nullable = normalize_type(ch_type)

        if value == '' and base_type in (
            'Int8', 'Int16', 'Int32', 'Int64',
            'UInt8', 'UInt16', 'UInt32', 'UInt64',
            'Float32', 'Float64', 'Date'
        ):
            value = None

        # Защита от NULL в не-nullable полях ClickHouse
        if value is None:
            if nullable:
                return None
            if base_type == 'String':
                return ''
            if base_type == 'Date':
                return date.today()
            if base_type.startswith('Int') or base_type.startswith('UInt'):
                return 0
            if base_type.startswith('Float'):
                return 0.0
            raise ValueError(f'NULL in non-nullable `{col_name}` ({ch_type})')

        try:
            if base_type == 'String':
                return str(value)
            if base_type.startswith('Int') or base_type.startswith('UInt'):
                return int(value)
            if base_type.startswith('Float'):
                val = float(value)
                if math.isnan(val) or math.isinf(val):
                    return None if nullable else 0.0
                return val
            if base_type == 'Date':
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, date):
                    return value
                return datetime.strptime(str(value)[:10], '%Y-%m-%d').date()
            return value
        except Exception as e:
            raise ValueError(f'Cast error `{col_name}` to `{ch_type}`: {value!r}') from e

    # Получаем структуру и типы колонок ClickHouse из broadcast-переменных
    cols = ch_columns_b.value
    types = ch_types_b.value

    # Создаем соединение с Clickhouse непосредственно с экзекутора
    client = clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='admin',
        password='123',
        database='default',
        connect_timeout=30,
        send_receive_timeout=300
    )

    batch = []
    batch_size = 50000

    for row in iterator:
        record = [
            cast_val(col_name, row[col_name], types[col_name])
            for col_name in cols
        ]
        batch.append(tuple(record))

        if len(batch) >= batch_size:
            client.insert('sales_mart', batch, column_names=cols)
            batch = []

    if batch:
        client.insert('sales_mart', batch, column_names=cols)

    client.close()

# ============================================================
# Основной пайплайн
# ============================================================
try:
    chains = {
        'x5':          ('iceberg.x5_silver.sales',          select_x5),
        'diksi':       ('iceberg.diksi_silver.sales',       select_diksi),
        'magnit':      ('iceberg.magnit_silver.sales',      select_magnit),
        'aushan':      ('iceberg.aushan_silver.sales',      select_aushan),
        'okey':        ('iceberg.okey_silver.sales',        select_okey),
        'perekrestok': ('iceberg.perekrestok_silver.sales', select_perekrestok),
        'pyaterochka': ('iceberg.pyaterochka_silver.sales', select_pyaterochka),
        'vernyi':      ('iceberg.vernyi_silver.sales',      select_vernyi),
    }

    dfs = []
    for chain_name, (table_name, select_func) in chains.items():
        try:
            df = spark.table(table_name)
            unified = select_func(df)
            dfs.append(unified)
            print(f"  ✓ {chain_name}")
        except Exception as e:
            print(f"  ⚠ {chain_name}: {e}")

    if not dfs:
        print("⚠ Нет данных для обработки")
    else:
        # ----------------------------------------------------------
        # 1. UNION
        # ----------------------------------------------------------
        all_data = dfs[0]
        for df in dfs[1:]:
            all_data = all_data.unionByName(df, allowMissingColumns=True)
        print("\n✓ UNION готов")

        # ----------------------------------------------------------
        # 2. Получение схемы из ClickHouse на Драйвере
        # ----------------------------------------------------------
        client = get_ch_client()

        mapping_pd = client.query_df("""
            SELECT
                original_name,
                brand_manual,
                chip_type_manual,
                package_manual,
                flavor_manual,
                weight_manual
            FROM product_mapping
        """)

        if len(mapping_pd) > 0:
            mapping_pd = (
                mapping_pd
                .dropna(subset=['original_name'])
                .drop_duplicates(subset=['original_name'], keep='last')
            )

        print(f"✓ Справочник: {len(mapping_pd)} записей")

        desc_rows  = client.query("DESCRIBE TABLE default.sales_mart").result_rows
        ch_columns = [row[0] for row in desc_rows]
        ch_types   = {row[0]: row[1] for row in desc_rows}
        client.close()

        if len(mapping_pd) > 0:
            mapping = spark.createDataFrame(mapping_pd)
        else:
            mapping = spark.createDataFrame(
                [],
                "original_name string, brand_manual string, "
                "chip_type_manual string, package_manual string, "
                "flavor_manual string, weight_manual string"
            )

        # ----------------------------------------------------------
        # 3. LEFT JOIN + Обогащение
        # ----------------------------------------------------------
        all_data = all_data.join(
            F.broadcast(mapping),
            all_data['product_name_search'] == mapping['original_name'],
            'left'
        )

        all_data = (
            all_data
            .withColumn('brand',
                F.coalesce(F.col('brand_manual'), F.col('brand')))
            .withColumn('flavor',
                F.coalesce(F.col('flavor_manual'), F.col('flavor')))
            .withColumn('weight_grams',
                F.when(F.col('weight_manual').isNotNull(),
                       F.col('weight_manual').cast('string'))
                 .otherwise(F.col('weight_grams')))
            .withColumn('chip_type',
                F.coalesce(F.col('chip_type_manual'), F.lit('')))
            .withColumn('package_type',
                F.coalesce(F.col('package_manual'), F.lit('')))
            .withColumn('product_name',
                F.when(F.col('product_uni_name').isNotNull(),
                       F.col('product_uni_name'))
                 .otherwise(F.col('product_name_search')))
            .withColumn('created_at', F.current_date())
        )

        # ----------------------------------------------------------
        # ИЗМЕНЕНИЕ: ФИЛЬТРАЦИЯ ТОЛЬКО ЧИПСОВ
        # ----------------------------------------------------------

        all_data = all_data.drop(
            'brand_manual', 'chip_type_manual', 'package_manual',
            'flavor_manual', 'weight_manual', 'original_name',
            'product_name_search', 'product_uni_name'
        )

        # Добавляем недостающие в датафрейме CH колонки
        for col_name in ch_columns:
            if col_name not in all_data.columns:
                all_data = all_data.withColumn(col_name, F.lit(None))

        # Выравниваем порядок колонок
        all_data = all_data.select(*ch_columns)

        print("✓ JOIN, обогащение и фильтрация чипсов выполнены")
        print(f"✓ Колонки: {ch_columns}")

        # ----------------------------------------------------------
        # 4. Очищаем целевую таблицу ClickHouse перед записью
        # ----------------------------------------------------------
        print("\nОчищаем старые данные в ClickHouse...")
        client = get_ch_client()
        client.command('TRUNCATE TABLE IF EXISTS default.sales_mart')
        client.close()
        print("✓ Таблица очищена")

        # ----------------------------------------------------------
        # 5. Инициализация Broadcast-переменных метаданных
        # ----------------------------------------------------------
        ch_columns_b = spark.sparkContext.broadcast(ch_columns)
        ch_types_b = spark.sparkContext.broadcast(ch_types)

        # ----------------------------------------------------------
        # 6. Распределенная запись в ClickHouse (foreachPartition)
        # ----------------------------------------------------------
        print("\nЗаписываем данные в ClickHouse из экзекуторов в 4 параллельных потока...")
        start_time = time.time()
        
        # repartition вместо coalesce равномерно балансирует объем партиций,
        # исключая Data Skew и падение экзекуторов по OOM
        all_data.repartition(4).foreachPartition(insert_partition_to_clickhouse)
        
        elapsed = time.time() - start_time
        print(f"✓ Запись завершена за {elapsed:.2f}с")

        # ----------------------------------------------------------
        # 7. Контрольная проверка результатов на Драйвере
        # ----------------------------------------------------------
        client = get_ch_client()
        final_count = client.query('SELECT count() FROM default.sales_mart').result_set[0][0]
        client.close()

        print(f"\n✅ Витрина успешно обновлена!")
        print(f"   Записано в ClickHouse: {final_count:,} строк чипсов")
        print(f"   Общее время работы:    {elapsed:.1f}с")
        if elapsed > 0:
            print(f"   Средняя скорость:      {final_count / elapsed:.0f} стр/сек")

except Exception as e:
    print(f"\n⚠ Ошибка пайплайна: {e}")
    import traceback
    traceback.print_exc()

finally:
    try:
        spark.stop()
        print("✓ Spark Session остановлена.")
    except Exception:
        pass