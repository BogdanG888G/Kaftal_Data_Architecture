"""
Сборка единой витрины из 13 сетей + LEFT JOIN со справочником товаров
🔄 ИНКРЕМЕНТАЛЬНОЕ ОБНОВЛЕНИЕ по file_name
Параллельная распределённая запись в ClickHouse напрямую с экзекуторов
Фильтрация: только чипсы (исключаем товары с brand_manual = 'Не чипсы')
"""

import traceback
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import clickhouse_connect

# ============================================================
# SPARK SESSION
# ============================================================
spark = (
    SparkSession.builder
    .appName("Build Sales Mart Incremental")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.catalog.iceberg.cache-enabled", "false")
    .getOrCreate()
)

print(f"✓ Spark: {spark.version}")

# ============================================================
# КОНСТАНТЫ
# ============================================================
CLICKHOUSE_HOST = "clickhouse"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "admin"
CLICKHOUSE_PASSWORD = "123"
CLICKHOUSE_DB = "default"
CLICKHOUSE_TABLE = "sales_mart"

BATCH_SIZE = 50_000
NUM_PARTITIONS = 4

INCREMENTAL_MODE = True


def get_ch_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
        connect_timeout=30,
        send_receive_timeout=300,
    )


def _col(name, cast_type=None, default=""):
    c = F.coalesce(F.col(name), F.lit(default))
    if cast_type:
        c = c.cast(cast_type)
    return c


def _lit_null(cast_type):
    return F.lit(None).cast(cast_type)


MART_COLUMNS = [
    "year", "month", "retail_chain",
    "region_name", "city_name", "address",
    "store_code", "store_name", "store_format",
    "product_category_2", "product_category_3",
    "product_category_4", "product_category_5",
    "product_id", "product_name_search", "product_uni_name",
    "brand", "vendor", "flavor", "weight_grams",
    "barcode", "manufacturer",
    "sales_quantity", "sales_amount_rub", "sales_cost_price",
    "sales_kg", "sales_tons",
    "average_cost_price", "average_sell_price",
    "margin_rub", "margin_pct", "cost_price_rub",
    "max_sell_price", "max_cost_price",
    "stock_qty", "stock_rub",
    "promo_sales_rub", "week_num",
    "file_name", "date",
]


# ============================================================
# ФУНКЦИИ УНИФИКАЦИИ
# ============================================================
def select_x5(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        _col("city_name").alias("city_name"),
        _col("address").alias("address"),
        _col("district_name").alias("store_code"),
        _col("factory_name").alias("store_name"),
        _col("factory_code").alias("store_format"),
        _col("product_category_2").alias("product_category_2"),
        _col("product_category_3").alias("product_category_3"),
        _col("product_category_4").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        _col("product_id", "string").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        _lit_null("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )

def select_svetofor(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        F.lit("").alias("city_name"),
        _col("address").alias("address"),
        F.lit("").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        _col("category_level_1").alias("product_category_3"),
        _col("category_level_2").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        _col("product_id", "string").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        F.lit("").alias("brand"),
        F.lit("").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        _col("product_id", "string").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        (F.col("sales_amount_rub") - F.col("sales_cost_price")).cast("double").alias("margin_rub"),
        F.when(F.col("sales_amount_rub") > 0,
               (F.col("sales_amount_rub") - F.col("sales_cost_price"))
               / F.col("sales_amount_rub") * 100
              ).cast("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )

def select_magnit_new(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _lit_null("string").alias("region_name"),
        _lit_null("string").alias("city_name"),
        _col("address").alias("address"),
        _col("store_code").alias("store_code"),
        _col("store_name").alias("store_name"),
        _col("format").alias("store_format"),
        _col("category_level_1").alias("product_category_2"),
        _col("category_level_2").alias("product_category_3"),
        _col("category_level_3").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        _col("product_id", "string").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        _col("barcode").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        (F.col("sales_amount_rub") - F.col("sales_cost_price")).cast("double").alias("margin_rub"),
        F.when(F.col("sales_amount_rub") > 0,
               (F.col("sales_amount_rub") - F.col("sales_cost_price"))
               / F.col("sales_amount_rub") * 100
              ).cast("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_samokat(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        F.lit("").alias("region_name"),
        _col("city_name").alias("city_name"),
        F.lit("").alias("address"),
        F.lit("").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        _col("category_level_1").alias("product_category_2"),
        _col("category_level_2").alias("product_category_3"),
        _col("category_level_3").alias("product_category_4"),
        _col("category_level_4").alias("product_category_5"),
        F.lit("").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        (F.col("sales_amount_rub") - F.col("sales_cost_price")).cast("double").alias("margin_rub"),
        F.when(F.col("sales_amount_rub") > 0,
               (F.col("sales_amount_rub") - F.col("sales_cost_price"))
               / F.col("sales_amount_rub") * 100
              ).cast("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_diksi(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        _col("city_name").alias("city_name"),
        _col("address").alias("address"),
        _col("store_code").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        _col("product_category_3").alias("product_category_3"),
        _col("product_category_4").alias("product_category_4"),
        _col("product_category_5").alias("product_category_5"),
        _col("product_id").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        _lit_null("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_magnit(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        _col("city_name").alias("city_name"),
        _col("address").alias("address"),
        _col("store_code").alias("store_code"),
        _col("store_name").alias("store_name"),
        _col("store_format").alias("store_format"),
        _col("product_category_2").alias("product_category_2"),
        _col("product_category_3").alias("product_category_3"),
        _col("product_category_4").alias("product_category_4"),
        _col("product_category_5").alias("product_category_5"),
        _col("product_id").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        _col("barcode").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        _lit_null("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_aushan(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        _col("city_name").alias("city_name"),
        _col("address").alias("address"),
        _col("store_code").alias("store_code"),
        F.lit("").alias("store_name"),
        _col("store_format").alias("store_format"),
        F.lit("").alias("product_category_2"),
        _col("product_segment").alias("product_category_3"),
        _col("family_name").alias("product_category_4"),
        _col("family_code", "string").alias("product_category_5"),
        _col("product_id").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        F.lit("").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        F.col("sales_kg").cast("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        F.col("margin_rub").cast("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        F.col("promo_sales_rub").cast("double").alias("promo_sales_rub"),
        F.col("week_num").cast("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_lenta(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        F.lit("").alias("region_name"),
        _col("city_name").alias("city_name"),
        F.lit("").alias("address"),
        _col("store_code").alias("store_code"),
        F.lit("").alias("store_name"),
        _col("store_format").alias("store_format"),
        F.lit("").alias("product_category_2"),
        _col("product_segment").alias("product_category_3"),
        F.lit("").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        _col("product_id").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        F.lit("").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("cost_price_rub").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        F.col("margin_rub").cast("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_okey(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        _col("city_name").alias("city_name"),
        F.lit("").alias("address"),
        _col("store_code").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        _col("product_category_2").alias("product_category_2"),
        _col("product_category_3").alias("product_category_3"),
        _col("product_category_4").alias("product_category_4"),
        _col("product_category_5").alias("product_category_5"),
        _col("product_id").alias("product_id"),
        F.when(F.col("product_uni_name").isNotNull(), F.col("product_uni_name"))
         .otherwise(F.col("product_name")).alias("product_name_search"),
        F.col("product_uni_name").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        _col("flavor").alias("flavor"),
        _col("weight_grams").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        F.col("sales_tons").cast("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        F.col("margin_rub").cast("double").alias("margin_rub"),
        F.col("margin_pct").cast("double").alias("margin_pct"),
        F.col("cost_price_rub").cast("double").alias("cost_price_rub"),
        F.col("max_sell_price").cast("double").alias("max_sell_price"),
        F.col("max_cost_price").cast("double").alias("max_cost_price"),
        F.col("stock_qty").cast("int").alias("stock_qty"),
        F.col("stock_rub").cast("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_perekrestok(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        _col("city_name").alias("city_name"),
        _col("address").alias("address"),
        _col("address").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        _col("product_category_3").alias("product_category_3"),
        _col("product_category_4").alias("product_category_4"),
        _col("base_type").alias("product_category_5"),
        _col("product_id").alias("product_id"),
        F.when(F.col("product_uni_name").isNotNull(), F.col("product_uni_name"))
         .otherwise(F.col("product_name")).alias("product_name_search"),
        F.col("product_uni_name").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        _col("flavor").alias("flavor"),
        _col("weight_grams").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        F.col("sales_tons").cast("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        _lit_null("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_pyaterochka(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        F.lit("").alias("region_name"),
        F.lit("").alias("city_name"),
        F.lit("").alias("address"),
        F.lit("").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        _col("product_category_3").alias("product_category_3"),
        _col("product_category_4").alias("product_category_4"),
        _col("base_type").alias("product_category_5"),
        F.lit("").alias("product_id"),
        F.when(F.col("product_uni_name").isNotNull(), F.col("product_uni_name"))
         .otherwise(F.col("product_name")).alias("product_name_search"),
        F.col("product_uni_name").alias("product_uni_name"),
        _col("brand").alias("brand"),
        _col("vendor").alias("vendor"),
        _col("flavor").alias("flavor"),
        _col("weight_grams").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        F.col("sales_tons").cast("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        _lit_null("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_vernyi(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        _col("region_name").alias("region_name"),
        _col("city_name").alias("city_name"),
        _col("address").alias("address"),
        _col("store_code").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        _col("product_category_3").alias("product_category_3"),
        _col("product_category_4").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        _col("product_id").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        F.lit("").alias("brand"),
        _col("vendor").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        _col("manufacturer").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        _lit_null("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        F.col("week_num").cast("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


def select_bristol(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        F.lit("").alias("region_name"),
        F.lit("").alias("city_name"),
        F.lit("").alias("address"),
        F.lit("").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        F.col("product_category").alias("product_category_3"),
        F.lit("").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        F.lit("").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        F.lit("").alias("brand"),
        F.lit("").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("revenue_rub").cast("double").alias("sales_amount_rub"),
        F.col("cost_price_rub").cast("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        _lit_null("double").alias("margin_rub"),
        _lit_null("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )

def select_globus(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        F.lit("").alias("region_name"),
        F.lit("").alias("city_name"),
        F.lit("").alias("address"),
        F.col("store_code").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        F.lit("").alias("product_category_3"),
        F.lit("").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        F.col("product_id").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        F.lit(None).cast("string").alias("product_uni_name"),
        F.lit("").alias("brand"),
        F.lit("").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("sales_quantity").cast("int").alias("sales_quantity"),
        F.col("sales_amount_rub").cast("double").alias("sales_amount_rub"),
        F.col("sales_cost_price").cast("double").alias("sales_cost_price"),
        F.lit(None).cast("double").alias("sales_kg"),
        F.lit(None).cast("double").alias("sales_tons"),
        F.col("average_cost_price").cast("double").alias("average_cost_price"),
        F.col("average_sell_price").cast("double").alias("average_sell_price"),
        F.col("margin_rub").cast("double").alias("margin_rub"),
        F.lit(None).cast("double").alias("margin_pct"),
        F.lit(None).cast("double").alias("cost_price_rub"),
        F.lit(None).cast("double").alias("max_sell_price"),
        F.lit(None).cast("double").alias("max_cost_price"),
        F.lit(None).cast("int").alias("stock_qty"),
        F.lit(None).cast("double").alias("stock_rub"),
        F.lit(None).cast("double").alias("promo_sales_rub"),
        F.lit(None).cast("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )

def select_redwhite(df):
    return df.select(
        F.col("year").cast("int").alias("year"),
        F.col("month").alias("month"),
        F.col("retail_chain").alias("retail_chain"),
        F.lit("").alias("region_name"),
        F.lit("").alias("city_name"),
        F.lit("").alias("address"),
        F.lit("").alias("store_code"),
        F.lit("").alias("store_name"),
        F.lit("").alias("store_format"),
        F.lit("").alias("product_category_2"),
        F.lit("").alias("product_category_3"),
        F.lit("").alias("product_category_4"),
        F.lit("").alias("product_category_5"),
        F.lit("").alias("product_id"),
        F.col("product_name").alias("product_name_search"),
        _lit_null("string").alias("product_uni_name"),
        F.lit("").alias("brand"),
        F.lit("").alias("vendor"),
        F.lit("").alias("flavor"),
        F.lit("").alias("weight_grams"),
        F.lit("").alias("barcode"),
        F.lit("").alias("manufacturer"),
        F.col("quantity_shipped").cast("int").alias("sales_quantity"),
        F.col("revenue_rub").cast("double").alias("sales_amount_rub"),
        _lit_null("double").alias("sales_cost_price"),
        _lit_null("double").alias("sales_kg"),
        _lit_null("double").alias("sales_tons"),
        _lit_null("double").alias("average_cost_price"),
        _lit_null("double").alias("average_sell_price"),
        F.col("markup_rub").cast("double").alias("margin_rub"),
        F.col("profitability_percent").cast("double").alias("margin_pct"),
        _lit_null("double").alias("cost_price_rub"),
        _lit_null("double").alias("max_sell_price"),
        _lit_null("double").alias("max_cost_price"),
        _lit_null("int").alias("stock_qty"),
        _lit_null("double").alias("stock_rub"),
        _lit_null("double").alias("promo_sales_rub"),
        _lit_null("int").alias("week_num"),
        F.col("file_name").alias("file_name"),
        F.col("period").cast("string").alias("date"),
    )


# ============================================================
# РАСПРЕДЕЛЁННЫЙ ПИСАТЕЛЬ В CLICKHOUSE
# ============================================================
def insert_partition_to_clickhouse(iterator):
    import clickhouse_connect
    import math
    from datetime import date, datetime

    def normalize_type(ch_type):
        nullable = False
        t = ch_type
        while True:
            if t.startswith("Nullable("):
                nullable = True
                t = t[len("Nullable("):-1]
                continue
            if t.startswith("LowCardinality("):
                t = t[len("LowCardinality("):-1]
                continue
            break
        return t, nullable

    def cast_val(col_name, value, ch_type):
        base_type, nullable = normalize_type(ch_type)

        if value == "" and base_type in (
            "Int8", "Int16", "Int32", "Int64",
            "UInt8", "UInt16", "UInt32", "UInt64",
            "Float32", "Float64", "Date",
        ):
            value = None

        if value is None:
            if nullable:
                return None
            if base_type == "String":
                return ""
            if base_type == "Date":
                return date.today()
            if base_type.startswith("Int") or base_type.startswith("UInt"):
                return 0
            if base_type.startswith("Float"):
                return 0.0
            raise ValueError(f"NULL in non-nullable `{col_name}` ({ch_type})")

        try:
            if base_type == "String":
                return str(value)
            if base_type.startswith("Int") or base_type.startswith("UInt"):
                return int(value)
            if base_type.startswith("Float"):
                val = float(value)
                if math.isnan(val) or math.isinf(val):
                    return None if nullable else 0.0
                return val
            if base_type == "Date":
                if isinstance(value, datetime):
                    return value.date()
                if isinstance(value, date):
                    return value
                return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
            return value
        except Exception as e:
            raise ValueError(
                f"Cast error `{col_name}` to `{ch_type}`: {value!r}"
            ) from e

    cols = ch_columns_b.value
    types = ch_types_b.value

    client = clickhouse_connect.get_client(
        host="clickhouse", port=8123,
        username="admin", password="123", database="default",
        connect_timeout=30, send_receive_timeout=300,
    )

    batch = []
    for row in iterator:
        record = tuple(cast_val(c, row[c], types[c]) for c in cols)
        batch.append(record)

        if len(batch) >= BATCH_SIZE:
            client.insert(CLICKHOUSE_TABLE, batch, column_names=cols)
            batch = []

    if batch:
        client.insert(CLICKHOUSE_TABLE, batch, column_names=cols)

    client.close()


# ============================================================
# ОСНОВНОЙ ПАЙПЛАЙН
# ============================================================
try:
    chains = {
        "x5":          ("iceberg.x5_silver.sales",                    select_x5),
        "diksi":       ("iceberg.diksi_silver.sales",                 select_diksi),
        "magnit":      ("iceberg.magnit_silver.sales",                select_magnit),
        "aushan":      ("iceberg.aushan_silver.sales",                select_aushan),
        "lenta":       ("iceberg.lenta_silver.sales",                 select_lenta),
        "okey":        ("iceberg.okey_silver.sales",                  select_okey),
        "perekrestok": ("iceberg.perekrestok_silver.sales",           select_perekrestok),
        "pyaterochka": ("iceberg.pyaterochka_silver.sales",           select_pyaterochka),
        "vernyi":      ("iceberg.vernyi_silver.sales",                select_vernyi),
        "bristol":     ("iceberg.bristol_silver.sales",               select_bristol),
        "redwhite":    ("iceberg.redwhite_silver.sales",              select_redwhite),
        "magnit_new":  ("iceberg.magnit_new_silver.magnit_new_sales", select_magnit_new),
        "samokat":     ("iceberg.samokat_silver.samokat_sales",       select_samokat),
        "globus":      ("iceberg.globus_silver.sales",                select_globus),
        "svetofor":    ("iceberg.svetofor_silver.sales",              select_svetofor),
    }

    # ============================================================
    # ШАГ 0: получаем уже загруженные file_name из ClickHouse
    # ============================================================
    loaded_files = set()
    table_exists = False

    if INCREMENTAL_MODE:
        print("\n" + "=" * 80)
        print("🔍 ИНКРЕМЕНТ: проверяем, какие file_name уже в ClickHouse")
        print("=" * 80)

        try:
            client = get_ch_client()
            tables = client.query(
                f"SELECT name FROM system.tables "
                f"WHERE database='{CLICKHOUSE_DB}' AND name='{CLICKHOUSE_TABLE}'"
            ).result_rows

            if tables:
                table_exists = True
                rows = client.query(
                    f"SELECT DISTINCT file_name "
                    f"FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE} "
                    f"WHERE file_name IS NOT NULL AND file_name != ''"
                ).result_rows
                loaded_files = {r[0] for r in rows}
                print(f"✓ Уже в ClickHouse: {len(loaded_files):,} уникальных file_name")
            else:
                print(f"⚠ Таблица {CLICKHOUSE_TABLE} не существует — первичная загрузка")

            client.close()
        except Exception as e:
            print(f"⚠ Не удалось получить список загруженных файлов: {e}")
            loaded_files = set()

    # ============================================================
    # ШАГ 1: загрузка и унификация по сетям
    # ============================================================
    print("\n" + "=" * 80)
    print("ЗАГРУЗКА И УНИФИКАЦИЯ ДАННЫХ ИЗ СЕТЕЙ")
    print("=" * 80)

    dfs = []
    stats_new = {}
    stats_skipped = {}

    for chain_name, (table_name, select_func) in chains.items():
        try:
            df = spark.table(table_name)
            unified = select_func(df)

            if INCREMENTAL_MODE and loaded_files:
                chain_files = {
                    r[0] for r in unified
                        .select("file_name").distinct().collect()
                    if r[0]
                }
                new_files = chain_files - loaded_files
                skipped = chain_files & loaded_files

                stats_new[chain_name] = len(new_files)
                stats_skipped[chain_name] = len(skipped)

                if not new_files:
                    print(f"  ⊘ {chain_name}: все {len(chain_files)} файлов уже загружены")
                    continue

                unified = unified.filter(F.col("file_name").isin(list(new_files)))
                print(f"  ✓ {chain_name}: +{len(new_files)} новых файлов "
                      f"(пропущено {len(skipped)})")
            else:
                stats_new[chain_name] = "ALL"
                stats_skipped[chain_name] = 0
                print(f"  ✓ {chain_name}: полная загрузка")

            dfs.append(unified)

        except Exception as e:
            print(f"  ⚠ {chain_name}: {str(e)[:120]}")

    total_new = sum(v for v in stats_new.values() if isinstance(v, int))
    total_skipped = sum(stats_skipped.values())

    if not dfs:
        print(f"\n✅ Новых файлов нет — витрина актуальна!")
        print(f"   Пропущено файлов: {total_skipped}")
        spark.stop()
        raise SystemExit(0)

    print(f"\n📊 Итого: новых файлов = {total_new}, пропущено = {total_skipped}")

    # ============================================================
    # ШАГ 2: UNION
    # ============================================================
    all_data = dfs[0]
    for df in dfs[1:]:
        all_data = all_data.unionByName(df, allowMissingColumns=True)
    print(f"\n✓ UNION готов: {len(dfs)} сетей")

    # ============================================================
    # ШАГ 3: справочник
    # ============================================================
    print("\n" + "=" * 80)
    print("СПРАВОЧНИК И ОБОГАЩЕНИЕ")
    print("=" * 80)

    client = get_ch_client()

    mapping_pd = client.query_df("""
        SELECT original_name, brand_manual, chip_type_manual,
               package_manual, flavor_manual, weight_manual
        FROM product_mapping
    """)

    if len(mapping_pd) > 0:
        mapping_pd = (
            mapping_pd
            .dropna(subset=["original_name"])
            .drop_duplicates(subset=["original_name"], keep="last")
        )

    print(f"✓ Справочник загружен: {len(mapping_pd):,} записей")

    desc_rows = client.query(
        f"DESCRIBE TABLE {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    ).result_rows
    ch_columns = [row[0] for row in desc_rows]
    ch_types = {row[0]: row[1] for row in desc_rows}
    client.close()

    if len(mapping_pd) > 0:
        mapping = spark.createDataFrame(mapping_pd)
    else:
        mapping = spark.createDataFrame(
            [],
            "original_name string, brand_manual string, "
            "chip_type_manual string, package_manual string, "
            "flavor_manual string, weight_manual string",
        )

    # ============================================================
    # ШАГ 4: LEFT JOIN + обогащение + фильтрация
    # ============================================================
    print("Выполняем LEFT JOIN с обогащением...")

    all_data = all_data.join(
        F.broadcast(mapping),
        all_data["product_name_search"] == mapping["original_name"],
        "left",
    )

    all_data = (
        all_data
        .withColumn("brand", F.coalesce(F.col("brand_manual"), F.col("brand")))
        .withColumn("flavor", F.coalesce(F.col("flavor_manual"), F.col("flavor")))
        .withColumn("weight_grams",
            F.when(F.col("weight_manual").isNotNull(),
                   F.col("weight_manual").cast("string"))
             .otherwise(F.col("weight_grams")))
        .withColumn("chip_type", F.coalesce(F.col("chip_type_manual"), F.lit("")))
        .withColumn("package_type", F.coalesce(F.col("package_manual"), F.lit("")))
        .withColumn("product_name",
            F.when(F.col("product_uni_name").isNotNull(),
                   F.col("product_uni_name"))
             .otherwise(F.col("product_name_search")))
        .withColumn("created_at", F.current_date())
    )

    print("Применяем фильтрацию: оставляем только чипсы...")
    all_data = all_data.filter(
        (F.col("brand_manual") != "Не чипсы") | F.col("brand_manual").isNull()
    )

    all_data = all_data.drop(
        "brand_manual", "chip_type_manual", "package_manual",
        "flavor_manual", "weight_manual", "original_name",
        "product_name_search", "product_uni_name",
    )

    for col_name in ch_columns:
        if col_name not in all_data.columns:
            all_data = all_data.withColumn(col_name, F.lit(None))

    all_data = all_data.select(*ch_columns)
    print("✓ Обогащение и фильтрация выполнены")

    # ============================================================
    # ШАГ 5: TRUNCATE ТОЛЬКО ПРИ FULL RELOAD
    # ============================================================
    if not INCREMENTAL_MODE:
        print("\n🗑️ FULL RELOAD: очищаем старые данные...")
        client = get_ch_client()
        client.command(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}")
        client.close()
        print("✓ Таблица очищена")
    else:
        print("\n🔄 ИНКРЕМЕНТ: дописываем БЕЗ TRUNCATE")

    # ============================================================
    # ШАГ 6: broadcast
    # ============================================================
    ch_columns_b = spark.sparkContext.broadcast(ch_columns)
    ch_types_b = spark.sparkContext.broadcast(ch_types)

    # ============================================================
    # ШАГ 7: запись
    # ============================================================
    print("\nЗаписываем данные в ClickHouse...")
    start_time = time.time()

    all_data.repartition(NUM_PARTITIONS).foreachPartition(
        insert_partition_to_clickhouse
    )

    elapsed = time.time() - start_time
    print(f"✓ Запись завершена за {elapsed:.2f}с")

    # ============================================================
    # ШАГ 8: итоговая проверка
    # ============================================================
    client = get_ch_client()
    final_count = client.query(
        f"SELECT count() FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}"
    ).result_set[0][0]

    chain_stats = client.query(f"""
        SELECT retail_chain, count() AS rows
        FROM {CLICKHOUSE_DB}.{CLICKHOUSE_TABLE}
        GROUP BY retail_chain
        ORDER BY rows DESC
    """).result_rows
    client.close()

    print(f"\n{'=' * 80}")
    print(f"✅ Витрина успешно обновлена!")
    print(f"   Режим:                  {'ИНКРЕМЕНТ' if INCREMENTAL_MODE else 'FULL RELOAD'}")
    print(f"   Загружено новых файлов: {total_new}")
    print(f"   Пропущено старых:        {total_skipped}")
    print(f"   Всего в ClickHouse:      {final_count:,} строк")
    print(f"   Время:                  {elapsed:.1f}с")
    if elapsed > 0 and final_count > 0:
        print(f"   Скорость:               {final_count / elapsed:,.0f} стр/сек")

    print(f"\n📈 По сетям:")
    for row in chain_stats:
        print(f"   {row[0]:<15} | {row[1]:>15,} строк")
    print(f"{'=' * 80}")

except SystemExit:
    pass
except Exception:
    print("\n⚠ Ошибка пайплайна:")
    traceback.print_exc()
    raise

finally:
    try:
        spark.stop()
        print("✓ Spark Session остановлена")
    except Exception:
        pass