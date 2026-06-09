import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime
import re as re_m

spark = SparkSession.builder.appName("Iceberg Aushan ETL").getOrCreate()
print(f"✓ Spark: {spark.version}")

# ============================================================
# КОНСТАНТЫ
# ============================================================
SILVER_COLUMNS = [
    'year', 'month', 'retail_chain', 'store_format', 'region_name', 'city_name',
    'address', 'store_code', 'product_segment', 'family_code', 'family_name',
    'product_id', 'product_name', 'vendor_code', 'vendor',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price', 'sales_kg',
    'average_cost_price', 'average_sell_price', 'margin_rub',
    'write_off_rub', 'write_off_qty', 'loss_rub', 'loss_qty', 'promo_sales_rub',
    'week_num', 'file_name', 'created_at', 'updated_at', 'period'
]

SILVER_TYPES = {
    'year': 'int',
    'month': 'string',
    'retail_chain': 'string',
    'store_format': 'string',
    'region_name': 'string',
    'city_name': 'string',
    'address': 'string',
    'store_code': 'string',
    'product_segment': 'string',
    'family_code': 'string',
    'family_name': 'string',
    'product_id': 'string',
    'product_name': 'string',
    'vendor_code': 'string',
    'vendor': 'string',
    'sales_quantity': 'int',
    'sales_amount_rub': 'float',
    'sales_cost_price': 'float',
    'sales_kg': 'float',
    'average_cost_price': 'float',
    'average_sell_price': 'float',
    'margin_rub': 'float',
    'write_off_rub': 'float',
    'write_off_qty': 'int',
    'loss_rub': 'float',
    'loss_qty': 'int',
    'promo_sales_rub': 'float',
    'week_num': 'int',
    'file_name': 'string',
    'created_at': 'date',
    'updated_at': 'date',
    'period': 'date'
}

COLUMN_RENAME_MAP = {
    'Дата': 'date_raw',
    'Сегмент': 'product_segment',
    'СЕМЬЯ': 'family_code',
    'НАЗВАНИЕ СЕМЬИ': 'family_name',
    'АРТИКУЛ': 'product_id',
    'НАИМЕНОВАНИЕ': 'product_name',
    'ПОСТАВЩИК': 'vendor_code',
    'НАИМЕНОВАНИЕ ПОСТАВЩИКА': 'vendor',
    'Магазин': 'store_code',
    'Город': 'city_raw',
    'Адрес': 'address',
    'Формат': 'store_format',
    'Месяц': 'month_raw',
    'Ср.цена продажи': 'average_sell_price',
    'Списания, руб.': 'write_off_rub',
    'Списания, шт.': 'write_off_qty',
    'Продажи, c НДС': 'sales_amount_rub_extra',
    'Продажи, кг': 'sales_kg',
    'Продажи, шт': 'sales_quantity',
    'Ср.цена покупки': 'average_cost_price',
    'Маржа, руб.': 'margin_rub',
    'Потери, руб.': 'loss_rub',
    'Потери,шт': 'loss_qty',
    'Промо Продажи, c НДС': 'promo_sales_rub',
}

MONTH_MAPPING = {
    'январь': 'Январь',
    'февраль': 'Февраль',
    'март': 'Март',
    'апрель': 'Апрель',
    'май': 'Май',
    'июнь': 'Июнь',
    'июль': 'Июль',
    'август': 'Август',
    'сентябрь': 'Сентябрь',
    'октябрь': 'Октябрь',
    'ноябрь': 'Ноябрь',
    'декабрь': 'Декабрь',
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
    'december': 'Декабрь',
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

NUMERIC_COLUMNS = {
    'average_sell_price': 'float',
    'write_off_rub': 'float',
    'write_off_qty': 'int',
    'sales_amount_rub_extra': 'float',
    'sales_kg': 'float',
    'sales_quantity': 'int',
    'average_cost_price': 'float',
    'margin_rub': 'float',
    'loss_rub': 'float',
    'loss_qty': 'int',
    'promo_sales_rub': 'float',
}

# ============================================================
# ХЕЛПЕРЫ
# ============================================================
def read_aushan_csv(path: str):
    """
    Автоопределение разделителя: сначала таб, потом ;
    Берём тот вариант, где колонок больше.
    """
    best_df = None
    best_cols = 0

    for sep in ['\t', ';']:
        tmp = (
            spark.read
            .option("header", True)
            .option("sep", sep)
            .option("quote", '"')
            .option("escape", '"')
            .csv(path)
        )
        if len(tmp.columns) > best_cols:
            best_df = tmp
            best_cols = len(tmp.columns)

    return best_df


def to_number(col_name: str, dtype: str):
    return F.when(
        F.trim(F.col(col_name).cast("string")) == "",
        None
    ).otherwise(
        F.regexp_replace(F.col(col_name).cast("string"), ",", ".").cast(dtype)
    )


def empty_to_null(expr):
    return F.when(F.length(F.trim(expr)) == 0, None).otherwise(F.trim(expr))


# ============================================================
# ЧТЕНИЕ ФАЙЛОВ ИЗ S3
# ============================================================
s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin'
)

objects = s3.list_objects_v2(Bucket='raw', Prefix='aushan_')

if 'Contents' not in objects:
    print("⚠ Нет файлов aushan_")
else:
    date_created = datetime.now().strftime('%Y-%m-%d')

    for obj in objects['Contents']:
        file = obj['Key']
        if not file.endswith('.csv'):
            continue

        file_path = f's3a://raw/{file}'
        print('=' * 100)
        print(f'Обработка: {file_path}')

        df = read_aushan_csv(file_path)
        print(f'✓ Строк: {df.count()}, Колонок: {len(df.columns)}')

        # ============================================================
        # ШАГ 1: Переименование колонок
        # ============================================================
        for old_name in df.columns:
            new_name = COLUMN_RENAME_MAP.get(old_name)
            if new_name and new_name != old_name:
                df = df.withColumnRenamed(old_name, new_name)

        # ============================================================
        # ШАГ 2: Служебные поля
        # ============================================================
        df = df.withColumn('retail_chain', F.lit('Ашан'))
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created).cast('date'))
        df = df.withColumn('updated_at', F.lit(date_created).cast('date'))

        # ============================================================
        # ШАГ 3: Месяц / год
        # ============================================================
        parts = re_m.split(r'[_\-.]+', file.replace('.csv', ''))
        month_str = None
        parsed_year = None

        for i, part in enumerate(parts):
            mc = MONTH_MAPPING.get(part.lower())
            if mc:
                month_str = mc
                if i + 1 < len(parts) and parts[i + 1].isdigit():
                    parsed_year = int(parts[i + 1])
                break

        if 'month_raw' in df.columns:
            first_month = (
                df.where(F.col('month_raw').isNotNull() & (F.trim(F.col('month_raw')) != ''))
                  .select('month_raw')
                  .limit(1)
                  .collect()
            )

            if first_month:
                mrv = str(first_month[0][0])

                for mn in MONTH_MAPPING_INT.keys():
                    if mn.lower() in mrv.lower():
                        if month_str is None:
                            month_str = mn

                        ym = re_m.search(r'(\d{4})', mrv)
                        if ym and parsed_year is None:
                            parsed_year = int(ym.group(1))
                        break

            df = df.drop('month_raw')

        if month_str is None:
            month_str = 'Неизвестно'

        month_int = MONTH_MAPPING_INT.get(month_str, 1)

        if parsed_year is None:
            parsed_year = datetime.now().year

        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(parsed_year))
        df = df.withColumn(
            'period',
            F.lit(f'{parsed_year}-{month_int:02d}-01').cast('date')
        )

        # ============================================================
        # ШАГ 4: Номер недели
        # Пример: 14.2026 -> 14
        # ============================================================
        if 'date_raw' in df.columns:
            df = df.withColumn(
                'week_num',
                F.regexp_extract(F.col('date_raw').cast('string'), r'^(\d+)', 1).cast('int')
            )
            df = df.drop('date_raw')

        # ============================================================
        # ШАГ 5: Регион / город / код магазина
        # ============================================================
        if 'city_raw' in df.columns:
            df = df.withColumn(
                'region_name',
                F.when(F.col('city_raw') == 'МО', F.lit('Московская область'))
                 .otherwise(empty_to_null(F.col('city_raw').cast('string')))
            )
            df = df.drop('city_raw')

        if 'address' in df.columns:
            city_from_address_1 = F.regexp_extract(
                F.col('address').cast('string'),
                r'г\.\s*([^,]+)',
                1
            )
            city_from_address_2 = F.regexp_extract(
                F.col('address').cast('string'),
                r'г\s*([^,]+)',
                1
            )

            df = df.withColumn(
                'city_name',
                F.coalesce(
                    empty_to_null(city_from_address_1),
                    empty_to_null(city_from_address_2),
                    F.col('region_name')
                )
            )

        if 'store_code' in df.columns:
            extracted_store_code = F.regexp_extract(
                F.col('store_code').cast('string'),
                r'\((\d+)\)',
                1
            )

            df = df.withColumn(
                'store_code',
                F.when(F.length(F.trim(extracted_store_code)) > 0, extracted_store_code)
                 .otherwise(F.trim(F.col('store_code').cast('string')))
            )

        # ============================================================
        # ШАГ 6: Нормализация числовых полей
        # ============================================================
        for col_name, dtype in NUMERIC_COLUMNS.items():
            if col_name in df.columns:
                df = df.withColumn(col_name, to_number(col_name, dtype))

        # ============================================================
        # ШАГ 7: Суммы продаж и себестоимость
        # ВАЖНО: sales_amount_rub берём из файла, а не только пересчитываем
        # ============================================================
        if 'sales_amount_rub_extra' in df.columns:
            df = df.withColumn(
                'sales_amount_rub',
                F.coalesce(
                    F.col('sales_amount_rub_extra'),
                    F.when(
                        F.col('average_sell_price').isNotNull() & F.col('sales_quantity').isNotNull(),
                        F.round(F.col('average_sell_price') * F.col('sales_quantity'), 2)
                    )
                )
            )
            df = df.drop('sales_amount_rub_extra')
        else:
            df = df.withColumn(
                'sales_amount_rub',
                F.when(
                    F.col('average_sell_price').isNotNull() & F.col('sales_quantity').isNotNull(),
                    F.round(F.col('average_sell_price') * F.col('sales_quantity'), 2)
                )
            )

        if 'average_cost_price' in df.columns and 'sales_quantity' in df.columns:
            df = df.withColumn(
                'sales_cost_price',
                F.when(
                    F.col('average_cost_price').isNotNull() & F.col('sales_quantity').isNotNull(),
                    F.round(F.col('average_cost_price') * F.col('sales_quantity'), 2)
                )
            )

        # ============================================================
        # ШАГ 8: Недостающие колонки
        # ============================================================
        remaining = set(SILVER_COLUMNS) - set(df.columns)
        for col in remaining:
            df = df.withColumn(col, F.lit(None).cast(SILVER_TYPES[col]))

        # ============================================================
        # ШАГ 9: Финальная типизация
        # ============================================================
        for col_name in SILVER_COLUMNS:
            df = df.withColumn(col_name, F.col(col_name).cast(SILVER_TYPES[col_name]))

        final_df = df.select(*SILVER_COLUMNS)

        print(f'Финальные колонки: {final_df.columns}')
        print('=' * 100)

        # ============================================================
        # ШАГ 10: Проверка на повторную загрузку файла
        # ============================================================
        try:
            res = spark.sql(
                f"""
                SELECT COUNT(*)
                FROM iceberg.aushan_silver.sales
                WHERE file_name = '{file[:-4]}'
                """
            ).first()
            file_exists = res[0] > 0
        except Exception:
            file_exists = False

        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')

            final_df.writeTo('iceberg.aushan_silver.sales') \
                .partitionedBy('retail_chain', 'year', 'month') \
                .append()

            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')

        print('=' * 100)
        print()

print('✅ Обработка Ашан завершена!')