"""
Обновление справочника товаров
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
import clickhouse_connect
from datetime import datetime

# Создаем Spark сессию с Iceberg поддержкой
spark = SparkSession.builder \
    .appName('Update Product Mapping') \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "rest") \
    .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181") \
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000") \
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "minioadmin") \
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true") \
    .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse") \
    .getOrCreate()

print(f"✓ Spark: {spark.version}")
print(f"✓ Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Список всех таблиц с продажами
tables = [
    ('x5_silver', 'iceberg.x5_silver.sales'),
    ('diksi_silver', 'iceberg.diksi_silver.sales'),
    ('magnit_silver', 'iceberg.magnit_silver.sales'),
    ('aushan_silver', 'iceberg.aushan_silver.sales'),
    ('okey_silver', 'iceberg.okey_silver.sales'),
    ('perekrestok_silver', 'iceberg.perekrestok_silver.sales'),
    ('pyaterochka_silver', 'iceberg.pyaterochka_silver.sales'),
    ('vernyi_silver', 'iceberg.vernyi_silver.sales'),
    ('bristol_silver', 'iceberg.bristol_silver.sales'),
    ('redwhite_silver', 'iceberg.redwhite_silver.sales'),
]

print("\n" + "=" * 80)
print("СБОР УНИКАЛЬНЫХ ТОВАРОВ ИЗ ВСЕХ СЕТЕЙ")
print("=" * 80)

all_products = None
stats = {}

for name, table in tables:
    try:
        # Пытаемся получить колонку product_name (может называться по-разному)
        try:
            df = spark.sql(f"""
                SELECT DISTINCT product_name as product_name
                FROM {table}
                WHERE product_name IS NOT NULL AND product_name != ''
            """)
            col_name = 'product_name'
        except:
            try:
                df = spark.sql(f"""
                    SELECT DISTINCT product_name as product_name
                    FROM {table}
                    WHERE product_name IS NOT NULL AND product_name != ''
                """)
                col_name = 'product_name'
            except:
                try:
                    df = spark.sql(f"""
                        SELECT DISTINCT Товар as product_name
                        FROM {table}
                        WHERE Товар IS NOT NULL AND Товар != ''
                    """)
                    col_name = 'Товар'
                except:
                    print(f"  ⚠ {name}: нет колонки с названием товара")
                    continue
        
        count = df.count()
        stats[name] = count
        print(f"  ✓ {name}: {count} уникальных товаров")
        
        if all_products is None:
            all_products = df
        else:
            all_products = all_products.unionByName(df)
            
    except Exception as e:
        print(f"  ⚠ {name}: ошибка - {str(e)[:50]}")
        stats[name] = 0

print("\n" + "=" * 80)
print("ОБРАБОТКА ДАННЫХ")
print("=" * 80)

if all_products is not None:
    # Очистка данных
    all_products = all_products.filter(F.col('product_name').isNotNull())
    all_products = all_products.filter(F.col('product_name') != '')
    all_products = all_products.filter(F.length(F.col('product_name')) > 2)
    
    # Удаляем дубликаты
    all_products = all_products.dropDuplicates(['product_name'])
    
    # Очищаем от мусора
    all_products = all_products.filter(~F.lower(F.col('product_name')).like('%тест%'))
    all_products = all_products.filter(~F.lower(F.col('product_name')).like('%test%'))
    all_products = all_products.filter(~F.lower(F.col('product_name')).like('%удалить%'))
    
    total_unique = all_products.count()
    print(f"✓ Уникальных товаров после очистки: {total_unique}")
    
    # Показываем статистику по сетям
    print("\nСтатистика по сетям:")
    for name, cnt in stats.items():
        if cnt > 0:
            print(f"  • {name}: {cnt:,} товаров")
    
    # Подготавливаем данные для вставки
    new_names_pd = all_products.select('product_name').toPandas()
    new_names_pd = new_names_pd.dropna(subset=['product_name'])
    new_names_pd['original_name'] = new_names_pd['product_name'].astype(str).str.strip()
    new_names_pd = new_names_pd.drop(columns=['product_name'])
    new_names_pd = new_names_pd[~new_names_pd['original_name'].isin(['nan', 'None', '', 'NULL'])]
    
    # Удаляем совсем короткие названия
    new_names_pd = new_names_pd[new_names_pd['original_name'].str.len() > 3]
    
    print(f"\n✓ После финальной очистки: {len(new_names_pd)} товаров")
    
    # Подключаемся к ClickHouse
    try:
        client = clickhouse_connect.get_client(
            host='clickhouse', port=8123,
            username='admin', password='123', database='default',
            connect_timeout=30, send_receive_timeout=30
        )
        print("✓ Подключение к ClickHouse установлено")
        
        # Проверяем существование таблицы
        try:
            client.command("""
                CREATE TABLE IF NOT EXISTS product_mapping (
                    original_name String,
                    brand_manual String,
                    chip_type_manual String,
                    package_manual String,
                    flavor_manual String,
                    weight_manual Float64,
                    created_at DateTime DEFAULT now(),
                    updated_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()
                ORDER BY original_name
            """)
            print("✓ Таблица product_mapping проверена/создана")
        except Exception as e:
            print(f"⚠ Ошибка при создании таблицы: {e}")
        
        # Получаем существующие названия
        existing = client.query_df('SELECT DISTINCT original_name FROM product_mapping')
        existing_names = set(existing['original_name'].tolist()) if not existing.empty else set()
        print(f"✓ Существующих товаров в справочнике: {len(existing_names)}")
        
        # Находим новые
        new_ones = new_names_pd[~new_names_pd['original_name'].isin(existing_names)]
        cnt = len(new_ones)
        print(f"✓ Новых товаров для добавления: {cnt}")
        
        if cnt > 0:
            # Добавляем колонки для ручного заполнения
            new_ones['brand_manual'] = ''
            new_ones['chip_type_manual'] = ''
            new_ones['package_manual'] = ''
            new_ones['flavor_manual'] = ''
            new_ones['weight_manual'] = 0.0
            
            # Вставляем данные
            client.insert_df('product_mapping', new_ones)
            print(f"✅ Добавлено {cnt} новых товаров в справочник!")
            
            # Показываем примеры новых товаров
            print("\nПримеры новых товаров (первые 10):")
            for idx, row in new_ones.head(10).iterrows():
                print(f"  • {row['original_name'][:80]}")
        else:
            print("✅ Новых товаров нет! Справочник актуален.")
        
        # Общая статистика
        total_after = client.query_df('SELECT COUNT(*) as cnt FROM product_mapping')
        print(f"\n📊 Итоговая статистика:")
        print(f"  • Всего товаров в справочнике: {total_after['cnt'].iloc[0]:,}")
        print(f"  • Добавлено сегодня: {cnt}")
        
        client.close()
        print("✓ Соединение с ClickHouse закрыто")
        
    except Exception as e:
        print(f"❌ Ошибка при работе с ClickHouse: {e}")
        import traceback
        traceback.print_exc()

else:
    print("⚠ Нет данных для обработки!")

print("\n" + "=" * 80)
print(f"✅ ОБНОВЛЕНИЕ СПРАВОЧНИКА ЗАВЕРШЕНО")
print(f"   Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 80)

spark.stop()