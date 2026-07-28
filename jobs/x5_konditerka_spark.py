import boto3
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from datetime import datetime

spark = SparkSession.builder \
    .appName('Iceberg X5 Konditerka ETL') \
    .getOrCreate()

print(f"✓ Версия Spark: {spark.version}")

s3 = boto3.client('s3',
                  endpoint_url='http://minio:9000',
                  aws_access_key_id='minioadmin',
                  aws_secret_access_key='minioadmin')

SILVER_COLUMNS = [
    'year', 'month', 'retail_chain',
    'district_name', 'region_name', 'city_name', 'address',
    'factory_code', 'factory_name',
    'product_category_2', 'product_category_3', 'product_category_4',
    'product_id', 'product_name', 'brand', 'vendor',
    'retailer', 'retailer_rc',
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_cost_price', 'average_sell_price',
    'file_name', 'created_at', 'updated_at', 'period'
]

# ✅ ВСЕ количественные и денежные поля → double
SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'district_name': 'string', 'region_name': 'string', 'city_name': 'string', 'address': 'string',
    'factory_code': 'string', 'factory_name': 'string',
    'product_category_2': 'string', 'product_category_3': 'string', 'product_category_4': 'string',
    'product_id': 'string', 'product_name': 'string', 'brand': 'string', 'vendor': 'string',
    'retailer': 'string', 'retailer_rc': 'string',
    'sales_quantity': 'double',        # ✅ было int — теряли дробные кг
    'sales_amount_rub': 'double',
    'sales_cost_price': 'double',
    'average_cost_price': 'double',
    'average_sell_price': 'double',
    'file_name': 'string', 'created_at': 'date', 'updated_at': 'date', 'period': 'date'
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

NUMERIC_COLS = [
    'sales_quantity', 'sales_amount_rub', 'sales_cost_price',
    'average_cost_price', 'average_sell_price'
]

objects = s3.list_objects_v2(Bucket='rawkonditerka', Prefix='x5_')

if 'Contents' not in objects:
    print("⚠ Нет файлов x5_ в бакете rawkonditerka")
else:
    files = objects['Contents']

    now = datetime.now()
    date_created = f'{now.year}-{now.month:02d}-{now.day:02d}'

    for obj in files:
        file = obj['Key']
        if not file.endswith('.csv'):
            continue

        file_name = f's3a://rawkonditerka/{file}'
        print('=' * 100)
        print(f'Обработка: {file_name}')
        print('=' * 100)

        df = spark.read.csv(file_name, sep=';')
        df = df.offset(1)

        first_row = list(df.first())[:-1]
        df = df.offset(1)

        input_cols = [str(i) for i in first_row]
        df = df.select(df.columns[:-1])
        df = df.toDF(*input_cols)

        print(f'✓ Строк прочитано: {df.count()}')

        # Маппинг
        for column in df.columns:
            new_name = SILVER_MAPPING.get(column)
            if new_name:
                df = df.withColumnRenamed(column, new_name)

        # Служебные
        df = df.withColumn('file_name', F.lit(file[:-4]))
        df = df.withColumn('created_at', F.lit(date_created))
        df = df.withColumn('updated_at', F.lit(date_created))

        # Год/месяц из имени
        parts = file.split('_')
        month_str = MONTH_MAPPING.get(parts[2].lower())
        month_int = MONTH_MAPPING_INT.get(month_str)
        parsed_year = parts[3].replace('.csv', '')

        df = df.withColumn('month', F.lit(month_str))
        df = df.withColumn('year', F.lit(int(parsed_year)))
        period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
        df = df.withColumn('period', F.lit(period_done))

        # Нормализация сети
        if 'retail_chain' in df.columns:
            df = df.withColumn('retail_chain', F.regexp_replace(df['retail_chain'], 'Пятёрочка', 'Пятерочка'))
            df = df.withColumn('retail_chain', F.regexp_replace(df['retail_chain'], 'Перекрёсток-Джем', 'Перекресток'))
            df = df.withColumn('retail_chain', F.regexp_replace(df['retail_chain'], 'Перекрёсток', 'Перекресток'))

        # ✅ Запятые → точки + пустые в NULL для ВСЕХ числовых
        for col_name in NUMERIC_COLS:
            if col_name in df.columns:
                df = df.withColumn(col_name, F.regexp_replace(F.col(col_name), ',', '.'))
                df = df.withColumn(
                    col_name,
                    F.when(F.trim(F.col(col_name)) == '', None).otherwise(F.col(col_name))
                )

        # Недостающие
        remaining_cols = set(SILVER_COLUMNS) - set(df.columns)
        if 'retail_chain' not in df.columns:
            df = df.withColumn('retail_chain', F.lit(parts[0]))
        for column in remaining_cols:
            df = df.withColumn(column, F.lit(None))

        # Типы
        for column in df.columns:
            dtype_str = SILVER_TYPES.get(column)
            if dtype_str:
                try:
                    df = df.withColumn(column, F.col(column).cast(dtype_str))
                except Exception as e:
                    print(f"  ⚠ {column}: {e}")

        final_df = df.select(*SILVER_COLUMNS)

        # ✅ МЯГКАЯ фильтрация — как у Ашана и Магнита
        # Оставляем строку если есть ХОТЬ ЧТО-ТО: qty ИЛИ выручка
        final_df = final_df.filter(
            (F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0)) |
            (F.col('sales_amount_rub').isNotNull() & (F.col('sales_amount_rub') > 0))
        )

        # Средние цены докидываем, если их нет
        final_df = final_df.withColumn(
            'average_cost_price',
            F.coalesce(
                F.col('average_cost_price'),
                F.when(F.col('sales_quantity') > 0,
                       F.col('sales_cost_price') / F.col('sales_quantity'))
            )
        ).withColumn(
            'average_sell_price',
            F.coalesce(
                F.col('average_sell_price'),
                F.when(F.col('sales_quantity') > 0,
                       F.col('sales_amount_rub') / F.col('sales_quantity'))
            )
        )

        TARGET_TABLE = 'iceberg.konditerka_silver.x5_sales'

        try:
            res = spark.sql(f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE file_name = '{file[:-4]}'").first()
            file_exists = res[0] > 0
        except:
            file_exists = False

        if not file_exists:
            rows = final_df.count()
            print(f'Записываем {rows} строк...')
            spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.konditerka_silver")
            final_df.writeTo(TARGET_TABLE).append()
            print(f'✓ {file} → {rows} строк')
        else:
            print(f'⊘ {file} уже есть')

        print('=' * 100)

print('✅ Обработка X5 Кондитерка завершена!')
spark.stop()