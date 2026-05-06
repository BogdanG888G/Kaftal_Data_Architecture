"""
Обновление справочника товаров — только ЧИПСЫ
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import clickhouse_connect

spark = SparkSession.builder \
    .appName('Update Product Mapping') \
    .getOrCreate()

print(f"✓ Spark: {spark.version}")

tables = [
    'iceberg.x5_silver.sales',
    'iceberg.diksi_silver.sales',
    'iceberg.magnit_silver.sales',
    'iceberg.aushan_silver.sales',
    'iceberg.okey_silver.sales',
    'iceberg.perekrestok_silver.sales',
    'iceberg.pyaterochka_silver.sales',
    'iceberg.vernyi_silver.sales',
]

all_products = None
for table in tables:
    try:
        df = spark.sql(f"""
            SELECT DISTINCT product_name 
            FROM {table} 
            WHERE lower(product_name) LIKE '%чипсы%'
        """)
        if all_products is None:
            all_products = df
        else:
            all_products = all_products.unionByName(df)
        print(f"  ✓ {table.split('.')[1]}: OK")
    except Exception as e:
        print(f"  ⚠ {table}: {e}")

if all_products is not None:
    all_products = all_products.filter(F.col('product_name').isNotNull())
    all_products = all_products.dropDuplicates(['product_name'])
    total_unique = all_products.count()
    print(f"✓ Всего уникальных чипсов: {total_unique}")
    
    new_names_pd = all_products.select('product_name').toPandas()
    new_names_pd = new_names_pd.dropna(subset=['product_name'])
    new_names_pd['original_name'] = new_names_pd['product_name'].astype(str)
    new_names_pd = new_names_pd.drop(columns=['product_name'])
    new_names_pd = new_names_pd[~new_names_pd['original_name'].isin(['nan', 'None', ''])]
    
    print(f"✓ После очистки: {len(new_names_pd)} чипсов")
    
    try:
        client = clickhouse_connect.get_client(
            host='clickhouse', port=8123,
            username='admin', password='123', database='default'
        )
        
        existing = client.query_df('SELECT DISTINCT original_name FROM product_mapping')
        existing_names = set(existing['original_name'].tolist()) if not existing.empty else set()
        
        print(f"✓ Существующих: {len(existing_names)}")
        
        new_ones = new_names_pd[~new_names_pd['original_name'].isin(existing_names)]
        cnt = len(new_ones)
        print(f"✓ Новых чипсов: {cnt}")
        
        if cnt > 0:
            new_ones['brand_manual'] = ''
            new_ones['chip_type_manual'] = ''
            new_ones['package_manual'] = ''
            new_ones['flavor_manual'] = ''
            new_ones['weight_manual'] = 0
            
            client.insert_df('product_mapping', new_ones)
            print(f"✅ Добавлено {cnt} новых чипсов!")
        else:
            print("✅ Новых чипсов нет!")
        
        client.close()
        
    except Exception as e:
        print(f"⚠ Ошибка: {e}")
        import traceback
        traceback.print_exc()

else:
    print("⚠ Нет данных")