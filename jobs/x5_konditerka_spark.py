import boto3
import traceback
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

SILVER_TYPES = {
    'year': 'int', 'month': 'string', 'retail_chain': 'string',
    'district_name': 'string', 'region_name': 'string', 'city_name': 'string', 'address': 'string',
    'factory_code': 'string', 'factory_name': 'string',
    'product_category_2': 'string', 'product_category_3': 'string', 'product_category_4': 'string',
    'product_id': 'string', 'product_name': 'string', 'brand': 'string', 'vendor': 'string',
    'retailer': 'string', 'retailer_rc': 'string',
    'sales_quantity': 'double',
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
    'Завод_': 'factory_name',
    'Тов.иер.ур.2': 'product_category_2',
    'Тов.иер.ур.3': 'product_category_3',
    'Тов.иер.ур.4': 'product_category_4',
    'Материал': 'product_id',
    'Материал2': 'product_name',
    'Материал_': 'product_name',
    'Бренд': 'brand',
    'Вендор': 'vendor',
    'Основной поставщик': 'retailer',
    'Поставщик склада (РЦ)': 'retailer_rc',
    'Количество (без ед. изм.)': 'sales_quantity',
    'Оборот с НДС (без ед.изм.)': 'sales_amount_rub',
    'Оборот (с НДС)(без ед.изм.)': 'sales_amount_rub',
    'Общая себестоимость (с НДС) (без ед. изм.)': 'sales_cost_price',
    'Средняя цена по себестоимости (с НДС)': 'average_cost_price',
    'Средняя цена продажи (с НДС)': 'average_sell_price',
}


def normalize_col(name):
    """Нормализация имён колонок: убираем пробелы, скобки, приводим к нижнему регистру.
    Это защищает от разных вариантов написания у X5."""
    if name is None:
        return None
    return (str(name)
            .replace(' ', '')
            .replace('\u00A0', '')  # неразрывный пробел
            .replace('(', '')
            .replace(')', '')
            .lower())


# Строим нормализованный маппинг один раз
SILVER_MAPPING_NORM = {normalize_col(k): v for k, v in SILVER_MAPPING.items()}


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

TARGET_TABLE = 'iceberg.konditerka_silver.x5_sales'

# Создаём namespace один раз до цикла
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.konditerka_silver")

# ✅ Пагинация — на случай если файлов больше 1000
paginator = s3.get_paginator('list_objects_v2')
files = []
for page in paginator.paginate(Bucket='rawkonditerka', Prefix='x5_'):
    files.extend(page.get('Contents', []))

if not files:
    print("⚠ Нет файлов x5_ в бакете rawkonditerka")
else:
    now = datetime.now()
    date_created = f'{now.year}-{now.month:02d}-{now.day:02d}'

    total = len([f for f in files if f['Key'].endswith('.csv')])
    processed = 0
    written = 0
    skipped_exists = 0
    skipped_empty = 0
    failed = 0

    for obj in files:
        file = obj['Key']
        if not file.endswith('.csv'):
            continue

        processed += 1
        file_name = f's3a://rawkonditerka/{file}'
        print('=' * 100)
        print(f'[{processed}/{total}] Обработка: {file_name}')
        print('=' * 100)

        try:
            # === 1. Быстрая проверка: файл уже загружен? ===
            try:
                res = spark.sql(
                    f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE file_name = '{file[:-4]}'"
                ).first()
                if res[0] > 0:
                    print(f'⊘ {file} уже есть в таблице ({res[0]} строк) — пропуск')
                    skipped_exists += 1
                    continue
            except Exception:
                # Таблицы ещё нет — это норм, продолжаем
                pass

            # === 2. Чтение CSV ===
            df = spark.read.csv(file_name, sep=';')

            # Читаем первую строку целиком (это заголовки)
            first_row = list(df.first())

            # Проверяем — если последняя колонка пустая (None/''), отрезаем её.
            # Иначе оставляем как есть.
            if first_row[-1] is None or str(first_row[-1]).strip() == '':
                first_row = first_row[:-1]
                df = df.select(df.columns[:-1])
                print(f'  ⚠ Отрезали пустую последнюю колонку')

            input_cols = [str(i) for i in first_row]
            df = df.toDF(*input_cols)

            # Удаляем строку с заголовками из данных
            # (она сейчас первая строка df)
            header_values = input_cols
            df = df.filter(~(F.col(input_cols[0]) == F.lit(header_values[0])))

            print(f'✓ Строк прочитано: {df.count()}')
            print(f'  Колонки в файле: {df.columns}')

            # === 3. Маппинг колонок через нормализацию ===
            for column in df.columns:
                new_name = SILVER_MAPPING_NORM.get(normalize_col(column))
                if new_name:
                    df = df.withColumnRenamed(column, new_name)
                else:
                    print(f"  ⚠ Не смаплена колонка: '{column}' (norm: '{normalize_col(column)}')")

            # === 4. Служебные поля ===
            df = df.withColumn('file_name', F.lit(file[:-4]))
            df = df.withColumn('created_at', F.lit(date_created))
            df = df.withColumn('updated_at', F.lit(date_created))

            # === 5. Год/месяц из имени файла ===
            parts = file.split('_')
            month_str = MONTH_MAPPING.get(parts[2].lower())
            month_int = MONTH_MAPPING_INT.get(month_str)
            parsed_year = parts[3].replace('.csv', '')

            df = df.withColumn('month', F.lit(month_str))
            df = df.withColumn('year', F.lit(int(parsed_year)))
            period_done = datetime.strptime(f'{parsed_year}-{month_int}-01', '%Y-%m-%d')
            df = df.withColumn('period', F.lit(period_done))

            # === 6. Нормализация сети ===
            if 'retail_chain' in df.columns:
                df = df.withColumn('retail_chain',
                    F.regexp_replace(df['retail_chain'], 'Пятёрочка', 'Пятерочка'))
                df = df.withColumn('retail_chain',
                    F.regexp_replace(df['retail_chain'], 'Перекрёсток-Джем', 'Перекресток'))
                df = df.withColumn('retail_chain',
                    F.regexp_replace(df['retail_chain'], 'Перекрёсток', 'Перекресток'))

            # === 7. Чистка чисел: пробелы, кавычки, запятая → точка ===
            for col_name in NUMERIC_COLS:
                if col_name in df.columns:
                    # убираем пробелы (обычные, неразрывные, табы) и кавычки
                    df = df.withColumn(col_name,
                        F.regexp_replace(F.col(col_name), '[\\s\\u00A0"\']', ''))
                    # запятая → точка
                    df = df.withColumn(col_name,
                        F.regexp_replace(F.col(col_name), ',', '.'))
                    # пустые → null
                    df = df.withColumn(col_name,
                        F.when(F.trim(F.col(col_name)) == '', None).otherwise(F.col(col_name)))

            # === 8. Добавляем недостающие колонки ===
            remaining_cols = set(SILVER_COLUMNS) - set(df.columns)
            if 'retail_chain' not in df.columns:
                df = df.withColumn('retail_chain', F.lit(parts[0]))
                remaining_cols.discard('retail_chain')
            for column in remaining_cols:
                df = df.withColumn(column, F.lit(None))

            # === 9. Приведение типов ===
            for column in df.columns:
                dtype_str = SILVER_TYPES.get(column)
                if dtype_str:
                    try:
                        df = df.withColumn(column, F.col(column).cast(dtype_str))
                    except Exception as e:
                        print(f"  ⚠ Cast {column} → {dtype_str}: {e}")

            final_df = df.select(*SILVER_COLUMNS)

            # === 10. Мягкая фильтрация ===
            final_df = final_df.filter(
                (F.col('sales_quantity').isNotNull() & (F.col('sales_quantity') > 0)) |
                (F.col('sales_amount_rub').isNotNull() & (F.col('sales_amount_rub') > 0))
            )

            # === 11. Пересчёт средних цен если их нет ===
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

            # === 12. Запись (только если есть данные!) ===
            rows = final_df.count()
            if rows == 0:
                print(f'⚠ {file}: 0 строк после фильтрации — ПРОПУСК записи '
                      f'(проверь маппинг колонок выше!)')
                skipped_empty += 1
                continue

            print(f'Записываем {rows} строк...')
            final_df.writeTo(TARGET_TABLE).append()
            print(f'✓ {file} → {rows} строк записано')
            written += 1

        except Exception as e:
            print(f'❌ ОШИБКА на файле {file}: {e}')
            traceback.print_exc()
            failed += 1
            continue

        print('=' * 100)

    print()
    print('=' * 100)
    print(f'📊 ИТОГИ:')
    print(f'   Всего файлов:              {total}')
    print(f'   Записано:                  {written}')
    print(f'   Пропущено (уже в таблице): {skipped_exists}')
    print(f'   Пропущено (0 строк):       {skipped_empty}')
    print(f'   Ошибок:                    {failed}')
    print('=' * 100)

print('✅ Обработка X5 Кондитерка завершена!')
spark.stop()