# generate_map.py — финальная версия с регионами, Магнитом и /data/ путями

import re
import time
import json
import math
from pathlib import Path
from itertools import cycle

import numpy as np
import pandas as pd
import requests
import clickhouse_connect
from tqdm import tqdm

# ============================================================
# НАСТРОЙКИ
# ============================================================
CH_HOST = "213.165.222.200"
CH_PORT = 8123
CH_USER = "admin"
CH_PASSWORD = "123"
CH_DATABASE = "default"

YANDEX_KEYS = [
    '18ffa901-3ca3-4490-9222-ed66046d64d7',
    '4eafaf6f-51c9-47d0-be01-cddf8e94f4a7',
    '27b61e45-ccdd-4c16-b6c7-e9c6e38c01f7',
    '694470aa-33bb-49c8-a0ba-1be0e99ec787',
    '54bf3eb1-a2d7-400b-9928-acc90a2a5780',
    '22706d49-4f15-41d6-892b-cde7473200de',
    '2056b23c-648c-4952-ac7a-d5952575e7db',
    '4f0efc9d-e486-4952-983d-dd4847d599a8',
    '413dcd39-ba92-43a2-92e1-51cec7aa26cd',
    '57bbd123-1ee5-48e8-95d3-9207318b7450',
    'c81804b3-3b27-400e-8c8e-3c2d688d9d43',
    '08fc2bb0-4759-40ff-b507-48005ba26947',
    '7b730765-17f9-4eec-822b-839c92ad7cad'
]

REQUESTS_PER_KEY = 1600

DATA_DIR = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAP_PATH = DATA_DIR / "x5_stores_map_final.html"
GEO_CACHE_PATH = DATA_DIR / "dim_stores_geo.xlsx"
CITIES_GEO_CACHE_PATH = DATA_DIR / "cities_geo.xlsx"
CITIES_POP_XLSX = DATA_DIR / "Города.xlsx"   # файл с населением

print(f"DATA_DIR: {DATA_DIR}")

# ============================================================
# РОТАТОР КЛЮЧЕЙ
# ============================================================
class YandexKeyRotator:
    def __init__(self, keys, per_key_limit=900):
        if not keys:
            raise ValueError("Нужен хотя бы один API-ключ!")
        self.keys = keys
        self.per_key_limit = per_key_limit
        self.usage = {k: 0 for k in keys}
        self._cycle = cycle(keys)
        self._current = next(self._cycle)
        self.exhausted_keys = set()

    @property
    def current_key(self):
        return self._current

    def use(self):
        self.usage[self._current] += 1
        if self.usage[self._current] >= self.per_key_limit:
            self.exhausted_keys.add(self._current)
            self._rotate()

    def _rotate(self):
        for _ in range(len(self.keys)):
            candidate = next(self._cycle)
            if candidate not in self.exhausted_keys:
                self._current = candidate
                print(f"🔄 Переключение на ключ: ...{candidate[-8:]}")
                return
        print("⚠️ Все ключи исчерпаны!")

    @property
    def has_capacity(self):
        return len(self.exhausted_keys) < len(self.keys)

    def stats(self):
        lines = []
        for k in self.keys:
            status = "🔴 исчерпан" if k in self.exhausted_keys else "🟢 активен"
            lines.append(f"  ...{k[-8:]}: {self.usage[k]}/{self.per_key_limit} {status}")
        return "\n".join(lines)


key_rotator = YandexKeyRotator(YANDEX_KEYS, REQUESTS_PER_KEY)
print(f"Загружено ключей: {len(YANDEX_KEYS)}")

# ============================================================
# ПОДКЛЮЧЕНИЕ К CLICKHOUSE
# ============================================================
client = clickhouse_connect.get_client(
    host=CH_HOST,
    port=CH_PORT,
    username=CH_USER,
    password=CH_PASSWORD,
    database=CH_DATABASE,
)
print(f"Подключились к ClickHouse: {CH_HOST}:{CH_PORT} ✅")

# ============================================================
# ЗАГРУЗКА ДАННЫХ ИЗ CLICKHOUSE
# ============================================================
SQL_FACT_PEREKRESTOK = """
SELECT
    toStartOfMonth(toDate(parseDateTimeBestEffortOrNull(date))) AS period_month,
    retail_chain,
    region_name,
    city_name,
    address,
    store_code,
    store_name,
    store_format,
    brand,
    sum(ifNull(sales_quantity, 0))    AS sales_quantity,
    sum(ifNull(sales_amount_rub, 0))  AS sales_amount_rub,
    sum(ifNull(sales_cost_price, 0))  AS cost_amount_rub
FROM sales_mart
WHERE
    (
        positionCaseInsensitiveUTF8(retail_chain, 'перекресток') > 0
        OR positionCaseInsensitiveUTF8(retail_chain, 'x5 united') > 0
    )
    AND parseDateTimeBestEffortOrNull(date) IS NOT NULL
    AND chip_type = 'Картофельные чипсы'
    AND address IS NOT NULL
    AND year = 2026
GROUP BY
    period_month, retail_chain, region_name, city_name,
    address, store_code, store_name, store_format, brand
ORDER BY period_month
"""

SQL_FACT_PYATEROCHKA = """
SELECT
    toStartOfMonth(toDate(parseDateTimeBestEffortOrNull(date))) AS period_month,
    retail_chain,
    region_name,
    city_name,
    address,
    store_code,
    store_name,
    store_format,
    brand,
    sum(ifNull(sales_quantity, 0))    AS sales_quantity,
    sum(ifNull(sales_amount_rub, 0))  AS sales_amount_rub,
    sum(ifNull(sales_cost_price, 0))  AS cost_amount_rub
FROM sales_mart
WHERE
    retail_chain = 'Пятерочка'
    AND parseDateTimeBestEffortOrNull(date) IS NOT NULL
    AND chip_type = 'Картофельные чипсы'
    AND address IS NOT NULL
    AND year = 2026
GROUP BY
    period_month, retail_chain, region_name, city_name,
    address, store_code, store_name, store_format, brand
ORDER BY period_month
"""

# --- НОВЫЙ ЗАПРОС ДЛЯ МАГНИТА ---
SQL_FACT_MAGNIT = """
SELECT
    toStartOfMonth(toDate(parseDateTimeBestEffortOrNull(date))) AS period_month,
    retail_chain,
    region_name,
    city_name,
    address,
    store_code,
    store_name,
    store_format,
    brand,
    sum(ifNull(sales_quantity, 0))    AS sales_quantity,
    sum(ifNull(sales_amount_rub, 0))  AS sales_amount_rub,
    sum(ifNull(sales_cost_price, 0))  AS cost_amount_rub
FROM sales_mart
WHERE
    positionCaseInsensitiveUTF8(retail_chain, 'магнит') > 0
    AND parseDateTimeBestEffortOrNull(date) IS NOT NULL
    AND chip_type = 'Картофельные чипсы'
    AND address IS NOT NULL
    AND year = 2026
GROUP BY
    period_month, retail_chain, region_name, city_name,
    address, store_code, store_name, store_format, brand
ORDER BY period_month
"""

SQL_STORES_PEREKRESTOK = """
SELECT DISTINCT
    retail_chain, region_name, city_name, address,
    store_code, store_name, store_format
FROM sales_mart
WHERE
    (
        positionCaseInsensitiveUTF8(retail_chain, 'перекресток') > 0
        OR positionCaseInsensitiveUTF8(retail_chain, 'x5 united') > 0
    )
    AND year = 2026
    AND address IS NOT NULL
"""

SQL_STORES_PYATEROCHKA = """
SELECT DISTINCT
    retail_chain, region_name, city_name, address,
    store_code, store_name, store_format
FROM sales_mart
WHERE
    retail_chain = 'Пятерочка'
    AND year = 2026
    AND address IS NOT NULL
"""

# --- НОВЫЙ ЗАПРОС ДЛЯ МАГНИТА (stores) ---
SQL_STORES_MAGNIT = """
SELECT DISTINCT
    retail_chain, region_name, city_name, address,
    store_code, store_name, store_format
FROM sales_mart
WHERE
    positionCaseInsensitiveUTF8(retail_chain, 'магнит') > 0
    AND year = 2026
    AND address IS NOT NULL
"""

print("Загружаем fact Перекрёсток...")
fact_p = client.query_df(SQL_FACT_PEREKRESTOK)
print(f"  → {len(fact_p):,} строк")

print("Загружаем fact Пятёрочка...")
fact_5 = client.query_df(SQL_FACT_PYATEROCHKA)
print(f"  → {len(fact_5):,} строк")

print("Загружаем fact Магнит...")
fact_m = client.query_df(SQL_FACT_MAGNIT)
print(f"  → {len(fact_m):,} строк")

print("Загружаем stores Перекрёсток...")
stores_p = client.query_df(SQL_STORES_PEREKRESTOK)
print(f"  → {len(stores_p):,} строк")

print("Загружаем stores Пятёрочка...")
stores_5 = client.query_df(SQL_STORES_PYATEROCHKA)
print(f"  → {len(stores_5):,} строк")

print("Загружаем stores Магнит...")
stores_m = client.query_df(SQL_STORES_MAGNIT)
print(f"  → {len(stores_m):,} строк")

# Объединяем все факты и магазины
fact = pd.concat([fact_p, fact_5, fact_m], ignore_index=True)
stores = pd.concat([stores_p, stores_5, stores_m], ignore_index=True)

print(f"\n📦 Итого fact: {len(fact):,} строк")
print(f"📦 Итого stores: {len(stores):,} строк")

# ============================================================
# ХЕЛПЕРЫ (нормализация, геокодирование)
# ============================================================
def norm(x) -> str:
    if pd.isna(x):
        return ""
    return re.sub(r"\s+", " ", str(x).strip())

def clean_address(addr) -> str:
    addr = norm(addr)
    addr = re.sub(r"^\d{6},\s*", "", addr)
    addr = re.sub(r",\s*,", ",", addr)
    return addr.strip()

def mode_or_first(s):
    s = s.dropna().astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        return None
    return s.value_counts().index[0]

def normalize_chain_name(x: str) -> str:
    x_n = norm(x).lower()
    if "пятер" in x_n or "пятёр" in x_n:
        return "Пятёрочка"
    if "джем" in x_n:
        return "Перекресток-Джем"
    if "перекресток" in x_n or "перекрёсток" in x_n:
        return "Перекресток"
    if "x5 united" in x_n:
        return "X5 United"
    if "магнит" in x_n:
        return "Магнит"
    return norm(x) or "Другое"

def make_full_address(row) -> str:
    address = clean_address(row.get("address", ""))
    return address

def normalize_city_for_join(s):
    s = str(s).lower().strip()
    s = re.sub(r'^г\.?\s*', '', s)
    s = re.sub(r'^город\s+', '', s)
    s = re.sub(r'^(сп\.|д\.|с\.|пос\.|рп\.|пгт\.?)\s*', '', s)
    if ',' in s:
        s = s.split(',')[0].strip()
    return s.strip()

def geocode_yandex(query: str, api_key: str):
    url = "https://geocode-maps.yandex.ru/1.x/"
    params = {
        "apikey": api_key,
        "geocode": query,
        "format": "json",
        "lang": "ru_RU",
        "results": 1,
    }
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    members = (
        data.get("response", {})
        .get("GeoObjectCollection", {})
        .get("featureMember", [])
    )
    if not members:
        return None, None, None, None
    geoobj = members[0]["GeoObject"]
    lon_s, lat_s = geoobj["Point"]["pos"].split()
    meta = geoobj.get("metaDataProperty", {}).get("GeocoderMetaData", {})
    return float(lat_s), float(lon_s), meta.get("precision"), meta.get("text")

# ============================================================
# ЗАГРУЗКА ДАННЫХ НАСЕЛЕНИЯ (после определения normalize_city_for_join)
# ============================================================
print("\nЗагружаем данные о населении из Excel...")
if CITIES_POP_XLSX.exists():
    cities_pop_df = pd.read_excel(CITIES_POP_XLSX)
    # Приводим колонки к нижнему регистру
    cities_pop_df.columns = cities_pop_df.columns.str.strip().str.lower()
    # Ищем нужные колонки
    col_map = {}
    for c in cities_pop_df.columns:
        if 'город' in c and 'федерал' not in c:
            col_map[c] = 'city'
        elif 'регион' in c:
            col_map[c] = 'region'
        elif 'федерал' in c:
            col_map[c] = 'federal_district'
        elif 'насел' in c:
            col_map[c] = 'population'
    cities_pop_df = cities_pop_df.rename(columns=col_map)
    # Оставляем нужные колонки
    keep_cols = [c for c in ['city', 'region', 'federal_district', 'population'] if c in cities_pop_df.columns]
    cities_pop_df = cities_pop_df[keep_cols].copy()
    cities_pop_df['city'] = cities_pop_df['city'].astype(str).str.strip()
    cities_pop_df['population'] = pd.to_numeric(cities_pop_df['population'], errors='coerce')
    cities_pop_df = cities_pop_df.dropna(subset=['population']).copy()
    cities_pop_df['population'] = cities_pop_df['population'].astype(int)
    # Нормализуем названия городов для join
    cities_pop_df['city_key'] = cities_pop_df['city'].apply(normalize_city_for_join)
    print(f"Городов с населением: {len(cities_pop_df)}")
else:
    cities_pop_df = pd.DataFrame(columns=['city_key', 'city', 'region', 'federal_district', 'population'])
    print("⚠️ Файл Города.xlsx не найден, данные о населении недоступны.")

# ============================================================
# НОРМАЛИЗАЦИЯ ДАННЫХ
# ============================================================
fact["period_month"] = pd.to_datetime(fact["period_month"], errors="coerce")
fact["period_str"] = fact["period_month"].dt.strftime("%Y-%m")
fact["sales_quantity"] = pd.to_numeric(fact["sales_quantity"], errors="coerce").fillna(0)
fact["sales_amount_rub"] = pd.to_numeric(fact["sales_amount_rub"], errors="coerce").fillna(0)
fact["cost_amount_rub"] = pd.to_numeric(fact["cost_amount_rub"], errors="coerce").fillna(0)
fact["brand"] = fact["brand"].fillna("Без бренда").astype(str).str.strip()
fact.loc[fact["brand"] == "", "brand"] = "Без бренда"

for df in [fact, stores]:
    df["retail_chain"] = df["retail_chain"].map(normalize_chain_name)
    df["address"] = df["address"].map(clean_address)
    df["store_name"] = df["store_name"].map(norm)
    df["store_format"] = df["store_format"].map(norm)
    df["region_name"] = df["region_name"].map(norm)
    df["city_name"] = df["city_name"].map(norm)
    df["store_code"] = df["store_code"].map(norm)

fact["store_key"] = fact["address"].str.lower().str.strip()
stores["store_key"] = stores["address"].str.lower().str.strip()

fact = fact[fact["store_key"].fillna("").str.strip().ne("")].copy()
stores = stores[stores["store_key"].fillna("").str.strip().ne("")].copy()

fact_keys = set(fact["store_key"].unique())
stores_keys = set(stores["store_key"].unique())

print(f"\nfact:   {len(fact):,} строк | {fact['store_key'].nunique():,} уникальных магазинов")
print(f"stores: {len(stores):,} строк | {stores['store_key'].nunique():,} уникальных магазинов")

# ============================================================
# ГЕОКОДИНГ МАГАЗИНОВ
# ============================================================
store_ref = (
    stores
    .groupby("store_key", as_index=False)
    .agg(
        retail_chain=("retail_chain", mode_or_first),
        region_name=("region_name", mode_or_first),
        city_name=("city_name", mode_or_first),
        address=("address", mode_or_first),
        store_code=("store_code", mode_or_first),
        store_name=("store_name", mode_or_first),
        store_format=("store_format", mode_or_first),
    )
    .copy()
)

store_ref["store_name"] = store_ref["store_name"].fillna("")
mask = store_ref["store_name"].str.strip() == ""
store_ref.loc[mask, "store_name"] = store_ref.loc[mask, "address"].fillna("Магазин")
store_ref["full_address"] = store_ref.apply(make_full_address, axis=1)

only_fact_keys = fact_keys - stores_keys
if only_fact_keys:
    extra = (
        fact[fact["store_key"].isin(only_fact_keys)]
        .groupby("store_key", as_index=False)
        .agg(
            retail_chain=("retail_chain", mode_or_first),
            region_name=("region_name", mode_or_first),
            city_name=("city_name", mode_or_first),
            address=("address", mode_or_first),
            store_code=("store_code", mode_or_first),
            store_name=("store_name", mode_or_first),
            store_format=("store_format", mode_or_first),
        )
    )
    extra["full_address"] = extra.apply(make_full_address, axis=1)
    store_ref = pd.concat([store_ref, extra], ignore_index=True)
    print(f"Добавлено {len(extra)} магазинов из fact")

print(f"Магазинов в store_ref: {len(store_ref):,}")

# --- Загрузка кэша магазинов ---
if GEO_CACHE_PATH.exists():
    geo_cache = pd.read_excel(GEO_CACHE_PATH)
    geo_cache.columns = geo_cache.columns.str.strip().str.lower()
    required = ["full_address", "lat", "lon", "geo_precision", "geo_found_address"]
    for col in required:
        if col not in geo_cache.columns:
            geo_cache[col] = None
    geo_cache = geo_cache.drop_duplicates("full_address", keep="last").copy()
    print(f"Кэш магазинов загружен: {len(geo_cache):,} записей")
else:
    geo_cache = pd.DataFrame(columns=required)
    print("Кэш магазинов не найден, создаём новый")

address_ref = store_ref[["full_address"]].drop_duplicates().copy()
address_ref = address_ref.merge(
    geo_cache[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]],
    on="full_address", how="left"
)

need_geo = address_ref[
    address_ref["full_address"].fillna("").str.strip().ne("") &
    (address_ref["lat"].isna() | address_ref["lon"].isna())
].copy()

print(f"\nВсего адресов: {len(address_ref):,}")
print(f"Уже в кэше:   {len(address_ref) - len(need_geo):,}")
print(f"Нужно геокодить: {len(need_geo):,}")

if len(need_geo) > 0:
    ok, fail, skipped = 0, 0, 0
    save_every = 100

    for i, (idx, row) in enumerate(
        tqdm(need_geo.iterrows(), total=len(need_geo), desc="Geocoding stores")
    ):
        if not key_rotator.has_capacity:
            print(f"\n⚠️ Все ключи исчерпаны! Остановка. Обработано: {ok + fail}")
            skipped = len(need_geo) - (ok + fail)
            break

        if not row["full_address"] or not row["full_address"].strip():
            for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                address_ref.at[idx, col] = None
            fail += 1
            continue

        try:
            lat, lon, prec, found = geocode_yandex(
                row["full_address"], key_rotator.current_key
            )
            key_rotator.use()

            address_ref.at[idx, "lat"] = lat
            address_ref.at[idx, "lon"] = lon
            address_ref.at[idx, "geo_precision"] = prec
            address_ref.at[idx, "geo_found_address"] = found
            ok += int(lat is not None)
            fail += int(lat is None)

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                print(f"\n🔑 Ключ ...{key_rotator.current_key[-8:]} вернул 403, переключаем...")
                key_rotator.exhausted_keys.add(key_rotator.current_key)
                key_rotator._rotate()
                if key_rotator.has_capacity:
                    try:
                        lat, lon, prec, found = geocode_yandex(
                            row["full_address"], key_rotator.current_key
                        )
                        key_rotator.use()
                        address_ref.at[idx, "lat"] = lat
                        address_ref.at[idx, "lon"] = lon
                        address_ref.at[idx, "geo_precision"] = prec
                        address_ref.at[idx, "geo_found_address"] = found
                        ok += int(lat is not None)
                        fail += int(lat is None)
                    except Exception as e2:
                        print(f"Ошибка повтора: {e2}")
                        for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                            address_ref.at[idx, col] = None
                        fail += 1
                else:
                    for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                        address_ref.at[idx, col] = None
                    fail += 1
        except Exception as e:
            print(f"Ошибка: {row['full_address'][:60]} → {e}")
            for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                address_ref.at[idx, col] = None
            fail += 1

        if (i + 1) % save_every == 0:
            _tmp = pd.concat([
                geo_cache,
                address_ref[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]]
            ]).drop_duplicates("full_address", keep="last")
            _tmp.to_excel(GEO_CACHE_PATH, index=False)

        time.sleep(0.15)

    print(f"\n✅ Успешно: {ok} | ❌ Ошибок: {fail} | ⏭️ Пропущено: {skipped}")

    updated = pd.concat([
        geo_cache,
        address_ref[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]]
    ]).drop_duplicates("full_address", keep="last")
    updated.to_excel(GEO_CACHE_PATH, index=False)
    print(f"Кэш магазинов обновлён: {GEO_CACHE_PATH} ({len(updated):,} записей)")

store_ref = store_ref.merge(
    address_ref[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]],
    on="full_address", how="left"
)

# ============================================================
# АГРЕГАЦИЯ ПО МАГАЗИНАМ
# ============================================================
store_sales = (
    fact
    .groupby("store_key", as_index=False)
    .agg(
        revenue_rub=("sales_amount_rub", "sum"),
        cost_rub=("cost_amount_rub", "sum"),
        sales_qty=("sales_quantity", "sum"),
        periods_count=("period_str", "nunique"),
        brands_count=("brand", "nunique"),
    )
    .copy()
)

store_sales["avg_monthly_revenue"] = (
    store_sales["revenue_rub"] / store_sales["periods_count"].clip(lower=1)
).round(0)

store_sales["avg_sale_price"] = np.where(
    store_sales["sales_qty"] > 0,
    store_sales["revenue_rub"] / store_sales["sales_qty"],
    np.nan
)

store_sales["avg_cost"] = np.where(
    store_sales["sales_qty"] > 0,
    store_sales["cost_rub"] / store_sales["sales_qty"],
    np.nan
)

store_brand_rev = (
    fact
    .groupby(["store_key", "brand"], as_index=False)
    .agg(brand_rev=("sales_amount_rub", "sum"))
    .sort_values(["store_key", "brand_rev"], ascending=[True, False])
)

top_brand_df = (
    store_brand_rev
    .groupby("store_key", as_index=False)
    .first()[["store_key", "brand"]]
    .rename(columns={"brand": "top_brand"})
)

all_brands_dict = {}
for sk, grp in store_brand_rev.groupby("store_key"):
    all_brands_dict[sk] = [
        {"brand": str(r["brand"]), "brand_rev": float(r["brand_rev"])}
        for _, r in grp.iterrows()
    ]

top5_dict = {sk: brands[:5] for sk, brands in all_brands_dict.items()}

store_monthly = (
    fact
    .groupby(["store_key", "period_str"], as_index=False)
    .agg(
        month_rev=("sales_amount_rub", "sum"),
        month_qty=("sales_quantity", "sum"),
    )
    .sort_values(["store_key", "period_str"])
)

dyn_dict = {
    sk: {
        "periods": grp["period_str"].tolist(),
        "rev": grp["month_rev"].astype(float).tolist(),
        "qty": grp["month_qty"].astype(float).tolist(),
    }
    for sk, grp in store_monthly.groupby("store_key")
}

store_sales = store_sales.merge(top_brand_df, on="store_key", how="left")
store_sales = store_sales.merge(
    store_ref[[
        "store_key", "retail_chain", "region_name", "city_name",
        "address", "store_code", "store_name", "store_format",
        "full_address", "lat", "lon", "geo_precision", "geo_found_address"
    ]],
    on="store_key", how="left"
)

store_sales["store_name"] = store_sales["store_name"].fillna("")
mask = store_sales["store_name"].str.strip() == ""
store_sales.loc[mask, "store_name"] = store_sales.loc[mask, "address"].fillna("Магазин")

geo = store_sales[store_sales["lat"].notna() & store_sales["lon"].notna()].copy()

total_stores_all = store_sales["store_key"].nunique()
total_stores_geo = geo["store_key"].nunique()
total_rev_all = store_sales["revenue_rub"].sum()
total_qty_all = store_sales["sales_qty"].sum()

print(f"\nstore_sales строк: {len(store_sales)}")
print(f"geo строк: {len(geo)}")
print(f"Всего магазинов: {total_stores_all}")
print(f"На карте: {total_stores_geo}")
print(f"Выручка: {total_rev_all:,.0f} ₽")
print(f"Продажи: {total_qty_all:,.0f} шт")

# ============================================================
# ГЕОКОДИНГ ГОРОДОВ
# ============================================================
print("\nПодготавливаем геокодирование городов...")

city_list = fact[["city_name", "region_name"]].drop_duplicates("city_name").copy()
city_list = city_list[city_list["city_name"].notna() & city_list["city_name"].str.strip().ne("")]
city_list["city_key"] = city_list["city_name"].apply(normalize_city_for_join)

print(f"Уникальных городов в данных: {len(city_list)}")

required_city_cols = ["city_key", "lat", "lon", "geo_precision", "geo_found_address"]

if CITIES_GEO_CACHE_PATH.exists():
    city_geo_cache = pd.read_excel(CITIES_GEO_CACHE_PATH)
    city_geo_cache.columns = city_geo_cache.columns.str.strip().str.lower()
    for col in required_city_cols:
        if col not in city_geo_cache.columns:
            city_geo_cache[col] = None
    city_geo_cache = city_geo_cache.drop_duplicates("city_key", keep="last").copy()
    print(f"Кэш городов загружен: {len(city_geo_cache):,} записей")
else:
    city_geo_cache = pd.DataFrame(columns=required_city_cols)
    print("Кэш городов не найден, создаём новый")

cities_merged = city_list.merge(
    city_geo_cache[["city_key", "lat", "lon", "geo_precision", "geo_found_address"]],
    on="city_key",
    how="left"
)

need_geo_cities = cities_merged[
    cities_merged["city_name"].notna() &
    (cities_merged["lat"].isna() | cities_merged["lon"].isna())
].copy()

print(f"Всего городов: {len(cities_merged):,}")
print(f"Уже в кэше:   {len(cities_merged) - len(need_geo_cities):,}")
print(f"Нужно геокодить: {len(need_geo_cities):,}")

if len(need_geo_cities) > 0:
    ok_city, fail_city, skipped_city = 0, 0, 0
    save_every_city = 50

    for i, (idx, row) in enumerate(
        tqdm(need_geo_cities.iterrows(), total=len(need_geo_cities), desc="Geocoding cities")
    ):
        if not key_rotator.has_capacity:
            print(f"\n⚠️ Все ключи исчерпаны! Остановка геокодирования городов. Обработано: {ok_city + fail_city}")
            skipped_city = len(need_geo_cities) - (ok_city + fail_city)
            break

        city = row["city_name"]
        region = row.get("region_name", "")
        if not city or not city.strip():
            for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                cities_merged.at[idx, col] = None
            fail_city += 1
            continue

        query = f"{city}, {region}, Россия"
        try:
            lat, lon, prec, found = geocode_yandex(query, key_rotator.current_key)
            key_rotator.use()

            cities_merged.at[idx, "lat"] = lat
            cities_merged.at[idx, "lon"] = lon
            cities_merged.at[idx, "geo_precision"] = prec
            cities_merged.at[idx, "geo_found_address"] = found
            ok_city += int(lat is not None)
            fail_city += int(lat is None)

        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 403:
                print(f"\n🔑 Ключ ...{key_rotator.current_key[-8:]} вернул 403, переключаем...")
                key_rotator.exhausted_keys.add(key_rotator.current_key)
                key_rotator._rotate()
                if key_rotator.has_capacity:
                    try:
                        lat, lon, prec, found = geocode_yandex(query, key_rotator.current_key)
                        key_rotator.use()
                        cities_merged.at[idx, "lat"] = lat
                        cities_merged.at[idx, "lon"] = lon
                        cities_merged.at[idx, "geo_precision"] = prec
                        cities_merged.at[idx, "geo_found_address"] = found
                        ok_city += int(lat is not None)
                        fail_city += int(lat is None)
                    except Exception as e2:
                        print(f"Ошибка повтора: {e2}")
                        for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                            cities_merged.at[idx, col] = None
                        fail_city += 1
                else:
                    for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                        cities_merged.at[idx, col] = None
                    fail_city += 1
        except Exception as e:
            print(f"Ошибка для города {city}: {e}")
            for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                cities_merged.at[idx, col] = None
            fail_city += 1

        if (i + 1) % save_every_city == 0:
            _tmp_city = cities_merged[["city_key", "lat", "lon", "geo_precision", "geo_found_address"]].drop_duplicates("city_key", keep="last")
            _tmp_city.to_excel(CITIES_GEO_CACHE_PATH, index=False)
            print(f"💾 Кэш городов сохранён (шаг {i+1})")

        time.sleep(0.15)

    print(f"\n✅ Городов: успешно {ok_city}, ошибок {fail_city}, пропущено {skipped_city}")

    city_geo_final = cities_merged[["city_key", "lat", "lon", "geo_precision", "geo_found_address"]].drop_duplicates("city_key", keep="last")
    city_geo_final.to_excel(CITIES_GEO_CACHE_PATH, index=False)
    print(f"Кэш городов сохранён: {CITIES_GEO_CACHE_PATH} ({len(city_geo_final):,} записей)")

# ============================================================
# АГРЕГАЦИЯ ПО ГОРОДАМ + ПРИСОЕДИНЕНИЕ НАСЕЛЕНИЯ
# ============================================================
print("\nАгрегируем продажи по городам...")

city_sales = (
    fact
    .groupby("city_name", as_index=False)
    .agg(
        city_qty=("sales_quantity", "sum"),
        city_rev=("sales_amount_rub", "sum"),
        city_stores=("store_key", "nunique"),
        region_name=("region_name", "first")
    )
)
city_sales["city_key"] = city_sales["city_name"].apply(normalize_city_for_join)

# --- Загружаем координаты городов из кэша ---
if CITIES_GEO_CACHE_PATH.exists():
    city_coords = pd.read_excel(CITIES_GEO_CACHE_PATH)
    city_coords.columns = city_coords.columns.str.strip().str.lower()
    city_coords = city_coords.dropna(subset=["lat", "lon"])
    city_coords = city_coords.rename(columns={"lat": "city_lat", "lon": "city_lon"})
    print(f"Загружены координаты городов из кэша: {len(city_coords)} городов")
else:
    # fallback – средние по магазинам
    print("Кэш городов не найден, вычисляем средние по магазинам...")
    city_coords = (
        geo
        .groupby("city_name", as_index=False)
        .agg(
            city_lat=("lat", "mean"),
            city_lon=("lon", "mean"),
        )
    )
    city_coords["city_key"] = city_coords["city_name"].apply(normalize_city_for_join)

# Объединяем продажи с координатами
city_merged = city_sales.merge(
    city_coords[["city_key", "city_lat", "city_lon"]],
    on="city_key",
    how="left"
)

# --- Присоединяем население из cities_pop_df ---
if not cities_pop_df.empty:
    city_merged = city_merged.merge(
        cities_pop_df[["city_key", "population"]],
        on="city_key",
        how="left"
    )
else:
    city_merged["population"] = np.nan

# Рассчитываем продажи на 1000 жителей
city_merged["qty_per_1000"] = np.where(
    city_merged["population"].notna() & (city_merged["population"] > 0),
    city_merged["city_qty"] / city_merged["population"] * 1000,
    np.nan
)

# Удаляем строки без координат
city_layer = city_merged.dropna(subset=["city_lat", "city_lon", "city_qty"]).copy()

# Ранг по продажам на душу
city_layer["rank_per_capita"] = city_layer["qty_per_1000"].rank(
    ascending=False, method="min", na_option='keep'
).astype('Int64')

# Ранг по абсолютным продажам
city_layer["rank_abs"] = city_layer["city_qty"].rank(ascending=False, method="min").astype('Int64')

print(f"\nГородов для слоя: {len(city_layer)}")
print(f"Из них с населением: {city_layer['population'].notna().sum()}")

# (Отладочные print'ы можно оставить или убрать – они не влияют)
print("------")
print("Всего продаж fact:", fact["sales_amount_rub"].sum())
print("Всего продаж city:", city_layer["city_rev"].sum())
print("------")
print(city_layer["city_key"].value_counts().head(20))
print(len(city_sales))
print(len(city_merged))
print(len(city_layer))

# ============================================================
# АГРЕГАЦИЯ ПО РЕГИОНАМ (вместо федеральных округов)
# ============================================================
print("\nАгрегируем продажи по регионам...")

region_sales = (
    geo
    .groupby("region_name", as_index=False)
    .agg(
        region_qty=("sales_qty", "sum"),
        region_rev=("revenue_rub", "sum"),
        region_stores=("store_key", "nunique"),
        region_cities=("city_name", "nunique"),
    )
)

# Удаляем регионы без названия
region_sales = region_sales[region_sales["region_name"].notna() & region_sales["region_name"].str.strip().ne("")]

# Координаты регионов – средние по магазинам в регионе
region_coords = (
    geo
    .groupby("region_name", as_index=False)
    .agg(
        region_lat=("lat", "mean"),
        region_lon=("lon", "mean"),
    )
)

region_sales = region_sales.merge(region_coords, on="region_name", how="inner")
region_sales = region_sales.dropna(subset=["region_lat", "region_lon"])

print(f"Регионов с данными: {len(region_sales)}")
for _, row in region_sales.iterrows():
    print(f"  {row['region_name']}: {row['region_qty']:,.0f} шт, {row['region_rev']:,.0f} ₽")

# ============================================================
# ПОДГОТОВКА JSON
# ============================================================
def safe_float(x):
    try:
        v = float(x)
        return None if math.isnan(v) else v
    except Exception:
        return None

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return None

color_map = {
    "Пятёрочка":       "#2ECC71",
    "Перекресток":     "#E63946",
    "Перекресток-Джем": "#F4A261",
    "X5 United":       "#457B9D",
    "Магнит":          "#FFA500",   # оранжевый
}

if len(geo) == 0:
    print("ВНИМАНИЕ: Нет данных для отображения на карте!")
    exit()

center_lat = float(geo["lat"].mean())
center_lon = float(geo["lon"].mean())
max_rev = max(float(geo["revenue_rub"].max()), 1.0)

# --- Слой 1: магазины ---
stores_full = []
for _, row in geo.iterrows():
    sk = row["store_key"]
    rev = float(row["revenue_rub"])
    r = max(5.0, min(23.0, 5 + 18 * math.sqrt(rev / max_rev)))

    stores_full.append({
        "store_key": str(sk),
        "store_code": norm(row.get("store_code")),
        "name": norm(row.get("store_name")) or "Магазин",
        "address": norm(row.get("address")),
        "full_address": norm(row.get("full_address")),
        "city": norm(row.get("city_name")),
        "region": norm(row.get("region_name")),
        "chain": norm(row.get("retail_chain")),
        "format": norm(row.get("store_format")) or "—",
        "lat": float(row["lat"]),
        "lon": float(row["lon"]),
        "rev": rev,
        "qty": float(row["sales_qty"]),
        "avg_price": safe_float(row.get("avg_sale_price")),
        "avg_cost": safe_float(row.get("avg_cost")),
        "avg_month_rev": safe_float(row.get("avg_monthly_revenue")),
        "periods_count": safe_int(row.get("periods_count")),
        "brands_count": safe_int(row.get("brands_count")),
        "top_brand": norm(row.get("top_brand")) or "—",
        "geo_precision": norm(row.get("geo_precision")) or "—",
        "top5": top5_dict.get(sk, []),
        "all_brands": all_brands_dict.get(sk, []),
        "dyn": dyn_dict.get(sk, {"periods": [], "rev": [], "qty": []}),
        "radius": round(r, 2),
    })

stores_light = []
for s in stores_full:
    stores_light.append({
        "lat": s["lat"],
        "lon": s["lon"],
        "r": s["radius"],
        "c": s["chain"],
        "n": s["name"],
        "rev": s["rev"],
        "qty": s["qty"],
        "id": s["store_key"]
    })

details_dict = {}
for s in stores_full:
    details_dict[s["store_key"]] = {
        "store_code": s["store_code"],
        "address": s["address"],
        "full_address": s["full_address"],
        "city": s["city"],
        "region": s["region"],
        "format": s["format"],
        "avg_price": s["avg_price"],
        "avg_cost": s["avg_cost"],
        "avg_month_rev": s["avg_month_rev"],
        "periods_count": s["periods_count"],
        "brands_count": s["brands_count"],
        "top_brand": s["top_brand"],
        "geo_precision": s["geo_precision"],
        "top5": s["top5"],
        "all_brands": s["all_brands"],
        "dyn": s["dyn"]
    }

# --- Слой 2: города ---
cities_full = []
max_qty_city = float(city_layer["city_qty"].max()) if len(city_layer) > 0 else 1.0

for _, row in city_layer.iterrows():
    qty = float(row["city_qty"])
    rev = float(row["city_rev"])
    r = max(6.0, min(40.0, 6 + 34 * math.sqrt(qty / max_qty_city)))
    pop = safe_int(row.get("population"))
    qty_per_1000 = safe_float(row.get("qty_per_1000"))
    rank_abs = safe_int(row.get("rank_abs"))
    rank_per_capita = safe_int(row.get("rank_per_capita"))

    cities_full.append({
        "city": norm(row.get("city_name")),
        "region": norm(row.get("region_name")),
        "lat": float(row["city_lat"]),
        "lon": float(row["city_lon"]),
        "qty": qty,
        "rev": rev,
        "stores": safe_int(row.get("city_stores")),
        "population": pop,
        "qty_per_1000": qty_per_1000,
        "radius": round(r, 2),
        "rank_abs": rank_abs,
        "rank_per_capita": rank_per_capita,
    })

cities_light = []
for c in cities_full:
    cities_light.append({
        "lat": c["lat"],
        "lon": c["lon"],
        "r": c["radius"],
        "city": c["city"],
        "region": c["region"],
        "qty": c["qty"],
        "rev": c["rev"],
        "stores": c["stores"],
        "population": c["population"],
        "qty_per_1000": c["qty_per_1000"],
        "rank_abs": c["rank_abs"],
        "rank_per_capita": c["rank_per_capita"],
    })

# --- Слой 3: регионы ---
regions_full = []
max_region_qty = float(region_sales["region_qty"].max()) if len(region_sales) > 0 else 1.0

for _, row in region_sales.iterrows():
    qty = float(row["region_qty"])
    r = max(15.0, min(60.0, 15 + 45 * math.sqrt(qty / max_region_qty)))
    regions_full.append({
        "name": norm(row["region_name"]),
        "lat": float(row["region_lat"]),
        "lon": float(row["region_lon"]),
        "qty": qty,
        "rev": float(row["region_rev"]),
        "stores": safe_int(row.get("region_stores")),
        "cities": safe_int(row.get("region_cities")),
        "radius": round(r, 2),
    })

regions_light = []
for r in regions_full:
    regions_light.append({
        "lat": r["lat"],
        "lon": r["lon"],
        "r": r["radius"],
        "name": r["name"],
        "qty": r["qty"],
        "rev": r["rev"],
        "stores": r["stores"],
        "cities": r["cities"],
    })

# ============================================================
# СОХРАНЕНИЕ JSON
# ============================================================
stores_json_path = DATA_DIR / "stores.json"
cities_json_path = DATA_DIR / "cities.json"
regions_json_path = DATA_DIR / "regions.json"
details_json_path = DATA_DIR / "details.json"

with open(stores_json_path, "w", encoding="utf-8") as f:
    json.dump(stores_light, f, ensure_ascii=False, separators=(',', ':'))
with open(cities_json_path, "w", encoding="utf-8") as f:
    json.dump(cities_light, f, ensure_ascii=False, separators=(',', ':'))
with open(regions_json_path, "w", encoding="utf-8") as f:
    json.dump(regions_light, f, ensure_ascii=False, separators=(',', ':'))
with open(details_json_path, "w", encoding="utf-8") as f:
    json.dump(details_dict, f, ensure_ascii=False, separators=(',', ':'))

print(f"\nJSON-файлы сохранены:\n  {stores_json_path}\n  {cities_json_path}\n  {regions_json_path}\n  {details_json_path}")

# ============================================================
# HTML ТЕМПЛЕЙТ (с /data/ путями, как у вас работало)
# ============================================================
HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <title>Торговые точки — картофельные чипсы</title>

    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>

    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        html, body, #map { height:100%; width:100%; font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif; }
        #map { background:#f0f4f9; }

        .top-panel {
            position:fixed; top:16px; left:50%; transform:translateX(-50%); z-index:10000;
            background:rgba(255,255,255,0.92); backdrop-filter:blur(16px) saturate(180%);
            box-shadow:0 8px 40px rgba(0,0,0,0.08); border:1px solid rgba(255,255,255,0.6);
            border-radius:20px; padding:14px 28px; display:flex; align-items:center;
            gap:32px; min-width:600px; max-width:90vw; justify-content:center; flex-wrap:wrap;
        }
        .top-panel .brand-icon { font-size:28px; }
        .top-panel .brand-title { font-size:18px; font-weight:800; color:#1a2634; letter-spacing:-0.3px; }
        .top-panel .brand-sub { font-size:12px; font-weight:400; color:#6b7a8d; margin-top:2px; }
        .top-panel .stats { display:flex; gap:20px; align-items:center; }
        .stat-item { display:flex; flex-direction:column; align-items:center; }
        .stat-value { font-size:16px; font-weight:700; color:#1a2634; }
        .stat-label { font-size:10px; font-weight:600; color:#8a99aa; text-transform:uppercase; letter-spacing:0.5px; margin-top:2px; }
        .stat-divider { width:1px; height:32px; background:#e4e9f0; }

        #layer-switcher {
            position:fixed; top:16px; left:24px; z-index:10001;
            background:rgba(255,255,255,0.95); backdrop-filter:blur(16px);
            border-radius:18px; padding:14px 18px;
            box-shadow:0 8px 32px rgba(0,0,0,0.10); border:1px solid rgba(255,255,255,0.6);
            min-width:200px;
        }
        #layer-switcher h4 {
            font-size:11px; font-weight:700; color:#6b7a8d; text-transform:uppercase;
            letter-spacing:0.6px; margin-bottom:10px;
        }
        .layer-option {
            display:flex; align-items:center; gap:10px; padding:8px 10px;
            border-radius:12px; cursor:pointer; transition:all 0.2s ease;
            margin-bottom:4px; border:2px solid transparent;
        }
        .layer-option:hover { background:#f0f4f9; }
        .layer-option.active { background:#EEF3FF; border-color:#4f7cff; }
        .layer-option input[type="radio"] { display:none; }
        .layer-icon { font-size:18px; }
        .layer-label { font-size:13px; font-weight:600; color:#1a2634; line-height:1.3; }
        .layer-sub { font-size:10px; color:#8a99aa; margin-top:1px; }

        #filter-toggle {
            position:fixed; top:20px; right:24px; z-index:10001;
            border:none; border-radius:14px; padding:10px 18px;
            background:rgba(255,255,255,0.92); backdrop-filter:blur(12px);
            color:#1a2634; font-weight:600; font-size:13px; cursor:pointer;
            box-shadow:0 4px 20px rgba(0,0,0,0.08); border:1px solid rgba(255,255,255,0.6);
            transition:all 0.2s ease; display:flex; align-items:center; gap:8px;
        }
        #filter-toggle:hover { background:rgba(255,255,255,0.98); transform:translateY(-1px); }

        #filters {
            position:fixed; top:76px; right:24px; width:320px; z-index:10001;
            background:rgba(255,255,255,0.95); backdrop-filter:blur(20px);
            border-radius:20px; padding:20px 22px;
            box-shadow:0 12px 48px rgba(0,0,0,0.12); border:1px solid rgba(255,255,255,0.6);
            transition:transform 0.3s cubic-bezier(0.4,0,0.2,1),opacity 0.3s ease;
            max-height:calc(100vh - 100px); overflow-y:auto;
        }
        #filters.collapsed { transform:translateX(360px); opacity:0; pointer-events:none; }
        #filters h3 { margin:0 0 16px 0; color:#1a2634; font-size:17px; font-weight:700; }

        .filter-group { margin-bottom:14px; }
        .filter-group label { display:block; font-size:11px; font-weight:600; color:#6b7a8d; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px; }
        .filter-group select, .filter-group input {
            width:100%; border:1.5px solid #e4e9f0; border-radius:12px; padding:10px 14px;
            font-size:13px; outline:none; background:#fff; color:#1a2634;
            transition:border-color 0.2s ease; font-family:inherit;
        }
        .filter-group select:focus, .filter-group input:focus { border-color:#4f7cff; }

        .btn-row { display:flex; gap:10px; margin-top:16px; }
        .btn { flex:1; border:none; border-radius:12px; padding:11px 16px; font-weight:600; font-size:13px; cursor:pointer; transition:all 0.2s ease; font-family:inherit; }
        .btn-apply { background:#4f7cff; color:#fff; }
        .btn-apply:hover { background:#3d6ae6; transform:translateY(-1px); }
        .btn-reset { background:#f0f4f9; color:#324155; }
        .btn-reset:hover { background:#e4e9f0; }

        /* ===== СТИЛЬНЫЙ ФИЛЬТР СЕТЕЙ ===== */
        .chain-filter-group { margin-bottom:14px; }
        .chain-filter-group .group-label {
            display:block; font-size:11px; font-weight:600; color:#6b7a8d;
            text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;
        }
        .chain-checkbox-wrap {
            display:flex; flex-wrap:wrap; gap:8px;
        }
        .chain-checkbox-btn {
            display:flex; align-items:center; gap:6px;
            padding:6px 14px; border-radius:20px;
            border:2px solid #e4e9f0; background:#fff;
            font-size:13px; font-weight:600; color:#1a2634;
            cursor:pointer; transition:all 0.2s ease;
            user-select:none; font-family:inherit;
        }
        .chain-checkbox-btn:hover {
            border-color:#b0c4d8; background:#f8fafc;
        }
        .chain-checkbox-btn.active {
            border-color:#4f7cff; background:#EEF3FF;
            box-shadow:0 2px 8px rgba(79,124,255,0.15);
        }
        .chain-checkbox-btn .dot {
            width:12px; height:12px; border-radius:50%; flex-shrink:0;
            border:1px solid rgba(0,0,0,0.06);
        }
        .chain-checkbox-btn input[type="checkbox"] { display:none; }
        .chain-checkbox-btn .count {
            font-weight:400; color:#8a99aa; font-size:11px; margin-left:2px;
        }

        /* ===== ДОП. ФИЛЬТР ДЛЯ ГОРОДОВ ===== */
        .city-filter-group { margin-bottom:14px; display:none; }
        .city-filter-group label {
            display:block; font-size:11px; font-weight:600; color:#6b7a8d;
            text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px;
        }
        .city-filter-group select {
            width:100%; border:1.5px solid #e4e9f0; border-radius:12px;
            padding:10px 14px; font-size:13px; outline:none; background:#fff;
            color:#1a2634; transition:border-color 0.2s ease; font-family:inherit;
        }
        .city-filter-group select:focus { border-color:#4f7cff; }

        .filters-note {
            margin-top:16px; padding-top:14px; border-top:1px solid #eef2f7;
            font-size:12px; color:#6b7a8d; line-height:1.6;
        }

        /* ===== ЛЕГЕНДА ===== */
        #legend {
            position:fixed; left:20px; bottom:24px; z-index:10000;
            background:rgba(255,255,255,0.92); backdrop-filter:blur(12px);
            border-radius:16px; padding:16px 20px;
            box-shadow:0 4px 24px rgba(0,0,0,0.08);
            border:1px solid rgba(255,255,255,0.6);
            min-width:180px; max-width:260px;
            transition:all 0.3s ease;
        }
        .legend-title {
            font-weight:700; font-size:13px; color:#1a2634; margin-bottom:10px;
            display:flex; align-items:center; gap:8px;
        }
        .legend-title .icon { font-size:16px; }
        .legend-item {
            display:flex; align-items:center; gap:10px; margin:6px 0;
            font-size:12px; color:#324155; padding:4px 6px;
            border-radius:8px; transition:background 0.2s;
        }
        .legend-item:hover { background:rgba(0,0,0,0.04); }
        .legend-dot {
            width:14px; height:14px; border-radius:50%; flex-shrink:0;
            border:1px solid rgba(0,0,0,0.06); box-shadow:0 1px 3px rgba(0,0,0,0.1);
        }
        .legend-label { flex:1; }
        .legend-count {
            font-weight:600; color:#8a99aa; font-size:11px;
            background:#f0f4f9; padding:0 8px; border-radius:12px; line-height:20px;
        }
        .legend-divider { border:none; border-top:1px solid #eef2f7; margin:10px 0; }
        .legend-hint { font-size:11px; color:#8a99aa; line-height:1.5; }

        .colorbar-wrap { margin-top:8px; }
        .colorbar { height:10px; border-radius:8px; margin:4px 0; }
        .colorbar-labels { display:flex; justify-content:space-between; font-size:10px; color:#8a99aa; }

        /* POPUPS */
        .store-popup { width:400px; font-family:inherit; color:#1a2634; }
        .city-popup { width:340px; font-family:inherit; color:#1a2634; }
        .region-popup { width:320px; font-family:inherit; color:#1a2634; }

        .popup-header { padding:16px 20px; border-radius:16px 16px 0 0; }
        .popup-header .badge {
            display:inline-block; background:rgba(255,255,255,0.2);
            padding:2px 10px; border-radius:20px; font-size:10px;
            font-weight:600; color:#fff; text-transform:uppercase;
            letter-spacing:0.5px; margin-bottom:6px;
        }
        .popup-title { font-size:17px; font-weight:800; line-height:1.3; color:#fff; }
        .popup-subtitle { margin-top:4px; font-size:12px; opacity:0.9; color:#fff; }

        .popup-body {
            background:#fafcfe; border:1px solid #eef2f7; border-top:none;
            border-radius:0 0 16px 16px; padding:16px 20px 20px;
        }
        .popup-meta {
            display:flex; flex-wrap:wrap; gap:4px 16px; font-size:12px;
            color:#6b7a8d; margin-bottom:14px; padding-bottom:12px;
            border-bottom:1px solid #eef2f7;
        }

        .kpi-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:14px; }
        .kpi-grid-3 { display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-bottom:14px; }
        .mini-kpi {
            background:#fff; border:1px solid #eef2f7; border-radius:12px;
            padding:10px 12px;
        }
        .mini-kpi .val { font-size:15px; font-weight:700; color:#1a2634; }
        .mini-kpi .lbl {
            margin-top:3px; font-size:10px; font-weight:600; color:#8a99aa;
            text-transform:uppercase; letter-spacing:0.3px;
        }

        .section-title {
            margin:14px 0 8px; font-size:11px; font-weight:700; color:#6b7a8d;
            text-transform:uppercase; letter-spacing:0.5px;
        }
        .spark-box {
            background:#fff; border:1px solid #eef2f7; border-radius:12px;
            padding:6px 8px; margin-bottom:6px;
        }

        .brand-list {
            background:#fff; border:1px solid #eef2f7; border-radius:12px;
            padding:10px 12px; max-height:300px; overflow-y:auto;
        }
        .brand-row {
            display:grid; grid-template-columns:1fr auto; gap:10px;
            align-items:center; margin-bottom:8px;
        }
        .brand-row:last-child { margin-bottom:0; }
        .brand-name {
            font-size:12px; font-weight:600; color:#1a2634;
            white-space:nowrap; overflow:hidden; text-overflow:ellipsis;
        }
        .brand-bar-wrap {
            height:6px; background:#eef2f7; border-radius:999px;
            overflow:hidden; margin-top:3px;
        }
        .brand-bar { height:6px; border-radius:999px; }
        .brand-val {
            font-size:12px; font-weight:600; color:#6b7a8d; white-space:nowrap;
        }
        .show-more-btn {
            margin-top:10px; padding:8px 16px; background:#f0f4f9;
            border:1px solid #e4e9f0; border-radius:10px; font-size:12px;
            font-weight:600; color:#4f7cff; cursor:pointer; width:100%;
            font-family:inherit; display:flex; align-items:center;
            justify-content:center; gap:8px;
        }
        .popup-footer {
            margin-top:12px; padding-top:10px; border-top:1px solid #eef2f7;
            font-size:11px; color:#8a99aa;
        }

        .leaflet-popup-content-wrapper {
            border-radius:16px !important; padding:0 !important;
            overflow:hidden; box-shadow:0 16px 48px rgba(0,0,0,0.18) !important;
        }
        .leaflet-popup-content { margin:0 !important; min-width:300px; }
        .leaflet-popup-close-button {
            top:12px !important; right:12px !important;
            color:rgba(255,255,255,0.7) !important; font-size:20px !important;
            font-weight:300 !important;
        }
        .leaflet-popup-close-button:hover { color:#fff !important; }

        .layer-notice {
            background:#FFF8E1; border:1px solid #FFE082; border-radius:10px;
            padding:8px 12px; font-size:12px; color:#795548; margin-bottom:12px;
            display:none;
        }

        #loading {
            position:fixed; top:0;left:0;width:100%;height:100%;
            background:rgba(255,255,255,0.8); z-index:99999;
            display:flex; flex-direction:column; align-items:center;
            justify-content:center; font-size:20px; color:#1a2634; font-weight:600;
        }
        #loading .spinner {
            width:50px; height:50px; border:5px solid #eef2f7;
            border-top-color:#4f7cff; border-radius:50%;
            animation:spin 0.9s linear infinite; margin-bottom:20px;
        }
        @keyframes spin { to { transform:rotate(360deg); } }

        .city-label {
            pointer-events: none;
            font-size: 13px;
            font-weight: 600;
            color: #1a2634;
            text-shadow: 0 1px 4px rgba(255,255,255,0.9);
            background: rgba(255,255,255,0.7);
            padding: 2px 8px;
            border-radius: 10px;
            border: 1px solid rgba(0,0,0,0.08);
            white-space: nowrap;
            backdrop-filter: blur(2px);
        }
    </style>
</head>
<body>
    <div id="loading"><div class="spinner"></div>Загрузка данных...</div>
    <div id="map"></div>

    <!-- СЛОИ -->
    <div id="layer-switcher">
        <h4>📍 Режим карты</h4>
        <label class="layer-option active" id="lopt-stores" onclick="switchLayer('stores')">
            <input type="radio" name="layer" value="stores" checked/>
            <span class="layer-icon">🏪</span>
            <div>
                <div class="layer-label">Магазины</div>
                <div class="layer-sub">Точки продаж, выручка</div>
            </div>
        </label>
        <label class="layer-option" id="lopt-cities" onclick="switchLayer('cities')">
            <input type="radio" name="layer" value="cities"/>
            <span class="layer-icon">🏙️</span>
            <div>
                <div class="layer-label">Города</div>
                <div class="layer-sub">Продажи на душу населения</div>
            </div>
        </label>
        <label class="layer-option" id="lopt-regions" onclick="switchLayer('regions')">
            <input type="radio" name="layer" value="regions"/>
            <span class="layer-icon">🗺️</span>
            <div>
                <div class="layer-label">Регионы</div>
                <div class="layer-sub">Сводка по регионам</div>
            </div>
        </label>
    </div>

    <!-- TOP PANEL -->
    <div class="top-panel">
        <div>
            <span class="brand-icon">🥔</span>
            <span class="brand-title" id="panel-title">Федеральные сети</span>
            <div class="brand-sub" id="panel-sub">Картофельные чипсы на карте</div>
        </div>
        <div class="stats">
            <div class="stat-item">
                <span class="stat-value" id="kpi-stores">0</span>
                <span class="stat-label" id="kpi-stores-lbl">Магазинов</span>
            </div>
            <div class="stat-divider"></div>
            <div class="stat-item">
                <span class="stat-value" id="kpi-rev">0 ₽</span>
                <span class="stat-label">Выручка</span>
            </div>
            <div class="stat-divider"></div>
            <div class="stat-item">
                <span class="stat-value" id="kpi-qty">0 шт</span>
                <span class="stat-label" id="kpi-qty-lbl">Продажи</span>
            </div>
        </div>
    </div>

    <!-- FILTER TOGGLE -->
    <button id="filter-toggle">⚙️ Фильтры</button>

    <!-- FILTERS -->
    <div id="filters">
        <h3>🔍 Фильтры</h3>
        <div class="layer-notice" id="layer-notice">
            Фильтры по сети/формату работают только в режиме «Магазины»
        </div>

        <div class="chain-filter-group" id="chain-filter-block">
            <label class="group-label">Сеть</label>
            <div id="chain-checkboxes" class="chain-checkbox-wrap"></div>
        </div>

        <div class="filter-group">
            <label>Регион</label>
            <select id="f-region"><option value="">Все регионы</option></select>
        </div>
        <div class="filter-group">
            <label>Город</label>
            <select id="f-city"><option value="">Все города</option></select>
        </div>
        <!-- Фильтр формата магазина УДАЛЁН -->

        <!-- Дополнительный фильтр для городов -->
        <div class="city-filter-group" id="city-filter-block">
            <label>Население</label>
            <select id="sel-population-filter">
                <option value="all" selected>Все города</option>
                <option value="only_with_pop">Только с населением</option>
                <option value="only_without_pop">Только без населения</option>
            </select>
        </div>

        <div class="filter-group">
            <label>Поиск</label>
            <input id="f-search" type="text" placeholder="Адрес, город, название..."/>
        </div>
        <div class="btn-row">
            <button class="btn btn-apply" id="btn-apply">Применить</button>
            <button class="btn btn-reset" id="btn-reset">Сбросить</button>
        </div>
        <div class="filters-note" id="filters-note"></div>
    </div>

    <!-- LEGEND -->
    <div id="legend"></div>

    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>

    <script>
    // ─── ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ──────────────────────────────────────────
    let STORES = [];
    let CITIES = [];
    let REGIONS_DATA = [];
    let DETAILS = {};
    const COLOR_MAP = __COLOR_MAP__;
    const DEFAULT_CENTER = [__CENTER_LAT__, __CENTER_LON__];
    const TOTAL_ALL_STORES = __TOTAL_ALL_STORES__;
    const TOTAL_GEO_STORES = __TOTAL_GEO_STORES__;

    let map, cluster, cityLayerGroup, regionsLayerGroup;
    let currentLayer = 'stores';

    // ─── УТИЛИТЫ ──────────────────────────────────────────────────────────
    const uniq = arr => [...new Set(arr.filter(x => x && String(x).trim() !== '' && x !== '—'))].sort((a,b) => String(a).localeCompare(String(b),'ru'));
    const esc  = s => String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#039;');
    const fmtNum   = n => (n===null||n===undefined||Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('ru-RU',{maximumFractionDigits:0});
    const fmtMoney = n => (n===null||n===undefined||Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('ru-RU',{maximumFractionDigits:0})+' ₽';
    const fmtPrice = n => (n===null||n===undefined||Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('ru-RU',{minimumFractionDigits:2,maximumFractionDigits:2})+' ₽';
    const fmtPc    = n => (n===null||n===undefined||Number.isNaN(Number(n))) ? '—' : Number(n).toLocaleString('ru-RU',{minimumFractionDigits:2,maximumFractionDigits:2});

    function fillSelect(el, items, firstText) {
        el.innerHTML = '';
        const first = document.createElement('option');
        first.value = ''; first.textContent = firstText;
        el.appendChild(first);
        items.forEach(item => {
            const opt = document.createElement('option');
            opt.value = item; opt.textContent = item;
            el.appendChild(opt);
        });
    }

    function perCapitaColor(value, min, max) {
        if (value === null || value === undefined) return '#aaaaaa';
        const t = Math.max(0, Math.min(1, (value - min) / Math.max(max - min, 0.001)));
        const stops = [
            [0,   [99,  179, 237]],
            [0.25,[72,  187, 120]],
            [0.5, [246, 224,  94]],
            [0.75,[237, 137,  54]],
            [1.0, [197,  48,  48]],
        ];
        let r=stops[0][1][0], g=stops[0][1][1], b=stops[0][1][2];
        for (let i=0; i<stops.length-1; i++) {
            const [t0, c0] = stops[i];
            const [t1, c1] = stops[i+1];
            if (t >= t0 && t <= t1) {
                const f = (t - t0) / (t1 - t0);
                r = Math.round(c0[0] + f*(c1[0]-c0[0]));
                g = Math.round(c0[1] + f*(c1[1]-c0[1]));
                b = Math.round(c0[2] + f*(c1[2]-c0[2]));
                break;
            }
        }
        return `rgb(${r},${g},${b})`;
    }

    const REGION_COLORS = ['#E63946','#457B9D','#2A9D8F','#E9C46A','#F4A261','#264653','#A8DADC','#6D6875','#8E5CF6','#E67E22','#2ECC71','#3498DB','#9B59B6','#1ABC9C','#F39C12','#D35400','#C0392B','#2980B9','#7F8C8D','#27AE60'];

    // ─── ПОПАПЫ ──────────────────────────────────────────────────────────
    function sparkline(values, color) {
        if (!values || values.length < 2) return '<div style="padding:8px 4px;color:#b0c0d0;font-size:12px;">Нет данных</div>';
        const w=340, h=40;
        const maxV=Math.max(...values), minV=Math.min(...values), range=(maxV-minV)||1;
        const step=w/(values.length-1);
        const pts=values.map((v,i)=>{ const x=i*step; const y=h-((v-minV)/range)*(h-6)-3; return `${x},${y}`; }).join(' ');
        const area=`0,${h} ${pts} ${w},${h}`;
        return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" style="display:block;width:100%;height:40px;">
            <polygon points="${area}" fill="${color}" opacity="0.12"/>
            <polyline points="${pts}" fill="none" stroke="${color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
        </svg>`;
    }

    function buildBrandsHtml(allBrands) {
        if (!allBrands || allBrands.length === 0) return '<div style="font-size:12px;color:#b0c0d0;padding:8px 12px;">Нет данных</div>';
        const palette=['#E63946','#457B9D','#F4A261','#2A9D8F','#8E5CF6','#E67E22','#2ECC71','#3498DB','#9B59B6','#1ABC9C'];
        const maxVal = Math.max(...allBrands.map(x=>Number(x.brand_rev||0)), 1);
        const listId = 'bl-'+Math.random().toString(36).substr(2,9);
        const top5 = allBrands.slice(0, 5);
        const rest = allBrands.slice(5);
        let html = `<div class="brand-list" id="${listId}">`;
        top5.forEach((b,i) => {
            const w = Math.max(6, (Number(b.brand_rev||0)/maxVal)*100);
            html += `<div class="brand-row"><div><div class="brand-name">${esc(b.brand)}</div><div class="brand-bar-wrap"><div class="brand-bar" style="width:${w}%;background:${palette[i%palette.length]};"/></div></div><div class="brand-val">${fmtMoney(b.brand_rev)}</div></div>`;
        });
        if (rest.length > 0) {
            html += `<div id="${listId}-more" style="display:none;">`;
            rest.forEach((b,i) => {
                const idx = i+5; const w = Math.max(6, (Number(b.brand_rev||0)/maxVal)*100);
                html += `<div class="brand-row"><div><div class="brand-name">${esc(b.brand)}</div><div class="brand-bar-wrap"><div class="brand-bar" style="width:${w}%;background:${palette[idx%palette.length]};"/></div></div><div class="brand-val">${fmtMoney(b.brand_rev)}</div></div>`;
            });
            html += `</div><button class="show-more-btn" onclick="toggleBrands('${listId}',${allBrands.length})">📊 Показать все (${allBrands.length})</button>`;
        }
        html += '</div>';
        return html;
    }
    window.toggleBrands = function(listId, total) {
        const moreBlock = document.getElementById(listId+'-more');
        const container = document.getElementById(listId);
        const btn = container?.querySelector('button');
        if (!moreBlock || !btn) return;
        if (moreBlock.style.display === 'none') { moreBlock.style.display='block'; btn.innerHTML='📊 Скрыть'; }
        else { moreBlock.style.display='none'; btn.innerHTML=`📊 Показать все (${total})`; }
    };

    function makeStorePopup(store, detail) {
        const color = COLOR_MAP[store.c] || '#7c8798';
        return `<div class="store-popup">
            <div class="popup-header" style="background:linear-gradient(135deg,${color} 0%,#1a2634 100%);">
                <div class="badge">${esc(store.c||'—')}</div>
                <div class="popup-title">🏪 ${esc(store.n)}</div>
                <div class="popup-subtitle">${esc(detail.format||'—')} · code: ${esc(detail.store_code||'—')}</div>
            </div>
            <div class="popup-body">
                <div class="popup-meta">
                    <span>📍 ${esc(detail.address||'—')}</span>
                    <span>🏙️ ${esc(detail.city||'—')}</span>
                    <span>🧭 ${esc(detail.geo_precision||'—')}</span>
                </div>
                <div class="kpi-grid">
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtMoney(store.rev)}</div><div class="lbl">Выручка</div></div>
                    <div class="mini-kpi"><div class="val" style="color:#457B9D;">${fmtNum(store.qty)} шт</div><div class="lbl">Продажи</div></div>
                    <div class="mini-kpi"><div class="val">${fmtPrice(detail.avg_price)}</div><div class="lbl">Средняя цена</div></div>
                    <div class="mini-kpi"><div class="val">${fmtPrice(detail.avg_cost)}</div><div class="lbl">Себестоимость</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(detail.periods_count)}</div><div class="lbl">Периодов</div></div>
                    <div class="mini-kpi"><div class="val">${fmtMoney(detail.avg_month_rev)}</div><div class="lbl">Ср. выручка/мес</div></div>
                </div>
                <div class="section-title">📈 Динамика выручки</div>
                <div class="spark-box">${sparkline(detail.dyn?.rev||[], color)}</div>
                <div class="section-title">📦 Динамика продаж</div>
                <div class="spark-box">${sparkline(detail.dyn?.qty||[], '#457B9D')}</div>
                <div class="section-title">🏷️ Бренды</div>
                ${buildBrandsHtml(detail.all_brands||[])}
                <div class="popup-footer">Брендов: <strong>${fmtNum(detail.brands_count)}</strong> · Топ: <strong>${esc(detail.top_brand||'—')}</strong></div>
            </div>
        </div>`;
    }

    function makeCityPopup(city) {
        const pc = city.qty_per_1000;
        const hasPopulation = city.population !== null && city.population !== undefined;
        const rank = city.rank_per_capita ? `#${city.rank_per_capita} ` : '';
        const popStr = hasPopulation ? fmtNum(city.population) : 'Нет данных';
        const pcStr = pc !== null && pc !== undefined ? fmtPc(pc) : '—';
        return `<div class="city-popup">
            <div class="popup-header" style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);">
                <div class="badge">${rank}ГОРОД</div>
                <div class="popup-title">🏙️ ${esc(city.city)}</div>
                <div class="popup-subtitle">${esc(city.region||'—')}</div>
            </div>
            <div class="popup-body">
                ${hasPopulation ? `
                <div class="per-capita-kpi">
                    <div class="big-val">${pcStr}</div>
                    <div class="big-lbl">продаж на 1 000 жителей</div>
                </div>` : `<div style="background:#fff3cd;border-radius:10px;padding:10px 14px;margin-bottom:12px;font-size:12px;color:#856404;">
                    ℹ️ Данные о населении не найдены
                </div>`}
                <div class="kpi-grid">
                    <div class="mini-kpi"><div class="val" style="color:#764ba2;">${fmtNum(city.qty)} шт</div><div class="lbl">Всего продаж</div></div>
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtMoney(city.rev)}</div><div class="lbl">Выручка</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(city.stores)}</div><div class="lbl">Магазинов</div></div>
                    <div class="mini-kpi"><div class="val">${popStr}</div><div class="lbl">Население</div></div>
                </div>
            </div>
        </div>`;
    }

    function makeRegionPopup(region, color) {
        return `<div class="region-popup">
            <div class="popup-header" style="background:linear-gradient(135deg,${color} 0%,#1a2634 100%);">
                <div class="badge">РЕГИОН</div>
                <div class="popup-title">🗺️ ${esc(region.name)}</div>
            </div>
            <div class="popup-body">
                <div class="kpi-grid-3">
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtNum(region.qty)}</div><div class="lbl">Продажи, шт</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(region.stores)}</div><div class="lbl">Магазинов</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(region.cities)}</div><div class="lbl">Городов</div></div>
                </div>
                <div class="kpi-grid">
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtMoney(region.rev)}</div><div class="lbl">Выручка</div></div>
                </div>
            </div>
        </div>`;
    }

    // ─── KPI UPDATE ────────────────────────────────────────────────────────
    function updateKpi(rows, layer) {
        const rev = rows.reduce((a,x)=>a+Number(x.rev||x.rev||0),0);
        const qty = rows.reduce((a,x)=>a+Number(x.qty||x.qty||0),0);
        document.getElementById('kpi-rev').textContent = fmtMoney(rev);
        document.getElementById('kpi-qty').textContent = fmtNum(qty)+' шт';

        if (layer === 'stores') {
            document.getElementById('kpi-stores').textContent = fmtNum(rows.length);
            document.getElementById('kpi-stores-lbl').textContent = 'Магазинов';
            document.getElementById('panel-title').textContent = 'Федеральные сети';
            document.getElementById('panel-sub').textContent = 'Картофельные чипсы — магазины';
            document.getElementById('filters-note').innerHTML = `Всего: <strong>${fmtNum(TOTAL_ALL_STORES)}</strong> · На карте: <strong>${fmtNum(TOTAL_GEO_STORES)}</strong> · Показано: <strong>${fmtNum(rows.length)}</strong>`;
        } else if (layer === 'cities') {
            document.getElementById('kpi-stores').textContent = fmtNum(rows.length);
            document.getElementById('kpi-stores-lbl').textContent = 'Городов';
            document.getElementById('panel-title').textContent = 'Продажи на душу населения';
            document.getElementById('panel-sub').textContent = 'Суммарная выручка и количество';
            const withPop = rows.filter(x => x.population !== null && x.population !== undefined).length;
            document.getElementById('filters-note').innerHTML = `Городов: <strong>${fmtNum(rows.length)}</strong> (с населением: ${fmtNum(withPop)})`;
        } else if (layer === 'regions') {
            document.getElementById('kpi-stores').textContent = fmtNum(rows.length);
            document.getElementById('kpi-stores-lbl').textContent = 'Регионов';
            document.getElementById('panel-title').textContent = 'Регионы';
            document.getElementById('panel-sub').textContent = 'Сводка по регионам';
            document.getElementById('filters-note').innerHTML = `Данные по <strong>${fmtNum(rows.length)}</strong> регионам`;
        }
    }

    // ─── LEGEND ────────────────────────────────────────────────────────────
    function updateLegendStores(rows) {
        const counts = {};
        rows.forEach(s => { counts[s.c] = (counts[s.c]||0)+1; });
        const chains = uniq(rows.map(x => x.c));
        let html = `<div class="legend-title"><span class="icon">🏪</span> Сети</div>`;
        chains.forEach(chain => {
            const color = COLOR_MAP[chain] || '#7c8798';
            html += `<div class="legend-item">
                <span class="legend-dot" style="background:${color};"></span>
                <span class="legend-label">${chain}</span>
                <span class="legend-count">${counts[chain]||0}</span>
            </div>`;
        });
        html += `<hr class="legend-divider"/>
                <div class="legend-hint">Размер точки = выручка</div>`;
        document.getElementById('legend').innerHTML = html;
    }

    function updateLegendCities(rows) {
        const vals = rows.map(x=>x.qty_per_1000).filter(x=>x!==null&&x!==undefined);
        const minV = vals.length ? Math.min(...vals) : 0;
        const maxV = vals.length ? Math.max(...vals) : 1;
        const grad = 'linear-gradient(to right, rgb(99,179,237), rgb(72,187,120), rgb(246,224,94), rgb(237,137,54), rgb(197,48,48))';
        let html = `<div class="legend-title"><span class="icon">🏙️</span> Продажи / 1 000 жителей</div>
        <div class="colorbar-wrap">
            <div class="colorbar" style="background:${grad};"></div>
            <div class="colorbar-labels">
                <span>${fmtPc(minV)}</span>
                <span>${fmtPc((minV+maxV)/2)}</span>
                <span>${fmtPc(maxV)}</span>
            </div>
        </div>
        <hr class="legend-divider"/>
        <div class="legend-hint">Серый = нет данных о населении<br>Размер = абс. продажи</div>`;
        document.getElementById('legend').innerHTML = html;
    }

    function updateLegendRegions(rows) {
        let html = `<div class="legend-title"><span class="icon">🗺️</span> Регионы</div>`;
        rows.forEach((region, i) => {
            const color = REGION_COLORS[i % REGION_COLORS.length];
            html += `<div class="legend-item">
                <span class="legend-dot" style="background:${color};"></span>
                <span class="legend-label">${region.name}</span>
                <span class="legend-count">${fmtNum(region.qty)} шт</span>
            </div>`;
        });
        html += `<hr class="legend-divider"/>
                <div class="legend-hint">Размер = кол-во продаж</div>`;
        document.getElementById('legend').innerHTML = html;
    }

    // ─── RENDER FUNCTIONS ─────────────────────────────────────────────────
    function renderStores(rows) {
        cluster.clearLayers();
        rows.forEach(store => {
            const color = COLOR_MAP[store.c] || '#7c8798';
            const marker = L.circleMarker([store.lat, store.lon], {
                radius: store.r, color: color, weight:2,
                fillColor: color, fillOpacity:0.7, opacity:0.9
            });
            marker.bindTooltip(`<strong>${esc(store.n)}</strong><br>${esc(store.c)} · ${fmtMoney(store.rev)}`, {direction:'top',sticky:true});
            marker.bindPopup((layer) => {
                const detail = DETAILS[store.id] || {};
                return makeStorePopup(store, detail);
            }, {maxWidth:430, closeButton:true});
            cluster.addLayer(marker);
        });
        updateKpi(rows, 'stores');
        updateLegendStores(rows);
    }

    function renderCities(rows) {
        cityLayerGroup.clearLayers();
        const pcVals = rows.map(x=>x.qty_per_1000).filter(x=>x!==null&&x!==undefined);
        const minPc = pcVals.length ? Math.min(...pcVals) : 0;
        const maxPc = pcVals.length ? Math.max(...pcVals) : 1;

        rows.forEach(city => {
            const color = perCapitaColor(city.qty_per_1000, minPc, maxPc);
            const marker = L.circleMarker([city.lat, city.lon], {
                radius: city.r || 8,
                color: '#fff', weight: 2,
                fillColor: color, fillOpacity: 0.82, opacity: 0.9
            });
            const pcStr = city.qty_per_1000 !== null && city.qty_per_1000 !== undefined ? fmtPc(city.qty_per_1000)+' шт/1000' : 'нет данных';
            marker.bindTooltip(`<strong>${esc(city.city)}</strong><br>На 1000 жит.: <b>${pcStr}</b><br>Продаж: ${fmtNum(city.qty)} шт`, {direction:'top', sticky:true});
            marker.bindPopup(makeCityPopup(city), {maxWidth:380, closeButton:true});
            cityLayerGroup.addLayer(marker);

            const labelIcon = L.divIcon({
                className: 'city-label',
                html: `<div>${esc(city.city)}</div>`,
                iconSize: [0,0],
                iconAnchor: [0, -12]
            });
            const label = L.marker([city.lat, city.lon], { icon: labelIcon, interactive: false });
            cityLayerGroup.addLayer(label);
        });

        updateKpi(rows, 'cities');
        updateLegendCities(rows);
    }

    function renderRegions(rows) {
        regionsLayerGroup.clearLayers();
        rows.forEach((region, i) => {
            const color = REGION_COLORS[i % REGION_COLORS.length];
            const marker = L.circleMarker([region.lat, region.lon], {
                radius: region.r,
                color: '#fff', weight: 3,
                fillColor: color, fillOpacity: 0.65, opacity: 1.0
            });
            marker.bindTooltip(
                `<strong>${esc(region.name)}</strong><br>Продажи: ${fmtNum(region.qty)} шт`,
                {direction:'top', sticky:true}
            );
            marker.bindPopup(makeRegionPopup(region, color), {maxWidth:360, closeButton:true});
            regionsLayerGroup.addLayer(marker);
        });
        updateKpi(rows, 'regions');
        updateLegendRegions(rows);
    }

    // ─── FILTERS ──────────────────────────────────────────────────────────
    function getSelectedChains() {
        return [...document.querySelectorAll('#chain-checkboxes input[type="checkbox"]')]
            .filter(cb => cb.checked).map(cb => cb.value);
    }

    function getFilteredStores() {
        const chains = getSelectedChains();
        const region = document.getElementById('f-region').value;
        const city   = document.getElementById('f-city').value;
        const search = (document.getElementById('f-search').value||'').trim().toLowerCase();
        return STORES.filter(s => {
            if (!chains.includes(s.c)) return false;
            const det = DETAILS[s.id] || {};
            if (region && det.region !== region) return false;
            if (city   && det.city   !== city)   return false;
            if (search) {
                const hay = [s.n, det.address, det.city, det.region, det.store_code, s.c].join(' ').toLowerCase();
                if (!hay.includes(search)) return false;
            }
            return true;
        });
    }

    function getFilteredCities() {
        const region = document.getElementById('f-region').value;
        const city   = document.getElementById('f-city').value;
        const search = (document.getElementById('f-search').value||'').trim().toLowerCase();
        const popFilter = document.getElementById('sel-population-filter').value;
        return CITIES.filter(c => {
            if (region && c.region !== region) return false;
            if (city   && c.city   !== city)   return false;
            if (search) {
                const hay = [c.city, c.region].join(' ').toLowerCase();
                if (!hay.includes(search)) return false;
            }
            if (popFilter === 'only_with_pop' && (c.population === null || c.population === undefined || Number.isNaN(c.population))) {
                return false;
            }
            if (popFilter === 'only_without_pop' && !(c.population === null || c.population === undefined || Number.isNaN(c.population))) {
                return false;
            }
            return true;
        });
    }

    function getFilteredRegions() {
        const search = (document.getElementById('f-search').value||'').trim().toLowerCase();
        return REGIONS_DATA.filter(r => {
            if (search && !r.name.toLowerCase().includes(search)) return false;
            return true;
        });
    }

    // ─── SWITCH LAYER ─────────────────────────────────────────────────────
    window.switchLayer = function(layer) {
        currentLayer = layer;
        ['stores','cities','regions'].forEach(l => {
            document.getElementById(`lopt-${l}`).classList.toggle('active', l===layer);
        });

        const isStores = layer === 'stores';
        const isCities = layer === 'cities';
        const isRegions = layer === 'regions';

        document.getElementById('chain-filter-block').style.display = isStores ? '' : 'none';
        document.getElementById('city-filter-block').style.display = isCities ? '' : 'none';
        document.getElementById('layer-notice').style.display = isStores ? 'none' : '';

        if (isCities) {
            const cityRegions = uniq(CITIES.map(c => c.region).filter(Boolean));
            const cityCities = uniq(CITIES.map(c => c.city).filter(Boolean));
            fillSelect(document.getElementById('f-region'), cityRegions, 'Все регионы');
            fillSelect(document.getElementById('f-city'), cityCities, 'Все города');
        } else if (isStores) {
            const storeRegions = uniq(Object.values(DETAILS).map(d => d.region).filter(Boolean));
            const storeCities = uniq(Object.values(DETAILS).map(d => d.city).filter(Boolean));
            fillSelect(document.getElementById('f-region'), storeRegions, 'Все регионы');
            fillSelect(document.getElementById('f-city'), storeCities, 'Все города');
        } else {
            // для регионов не обновляем списки, т.к. их там быть не должно
            // можно оставить, но не обязательно
        }

        if (isStores) {
            map.addLayer(cluster);
            map.removeLayer(cityLayerGroup);
            map.removeLayer(regionsLayerGroup);
        } else if (isCities) {
            map.removeLayer(cluster);
            map.addLayer(cityLayerGroup);
            map.removeLayer(regionsLayerGroup);
        } else if (isRegions) {
            map.removeLayer(cluster);
            map.removeLayer(cityLayerGroup);
            map.addLayer(regionsLayerGroup);
        }
        applyFilters();
    };

    function applyFilters() {
        if (currentLayer === 'stores') {
            const rows = getFilteredStores();
            renderStores(rows);
        } else if (currentLayer === 'cities') {
            const rows = getFilteredCities();
            renderCities(rows);
        } else if (currentLayer === 'regions') {
            const rows = getFilteredRegions();
            renderRegions(rows);
        }
    }

    function resetFilters() {
        [...document.querySelectorAll('#chain-checkboxes input[type="checkbox"]')].forEach(cb => cb.checked=true);
        document.getElementById('f-region').value = '';
        document.getElementById('f-city').value = '';
        document.getElementById('f-search').value = '';
        document.getElementById('sel-population-filter').value = 'all';
        applyFilters();
        map.setView(DEFAULT_CENTER, 5);
    }

    // ─── ИНИЦИАЛИЗАЦИЯ КАРТЫ ─────────────────────────────────────────────
    function initMap() {
        document.getElementById('loading').style.display = 'none';

        map = L.map('map', { zoomControl:true, preferCanvas:true }).setView(DEFAULT_CENTER, 5);
        L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
            maxZoom:19, attribution:'© OpenStreetMap © CARTO'
        }).addTo(map);

        cluster = L.markerClusterGroup({ chunkedLoading:true, spiderfyOnMaxZoom:true, showCoverageOnHover:false, maxClusterRadius:60 });
        cityLayerGroup = L.layerGroup();
        regionsLayerGroup   = L.layerGroup();
        map.addLayer(cluster);

        // ===== ФИЛЬТР СЕТЕЙ (стильные кнопки) =====
        const chains = uniq(STORES.map(x => x.c));
        const chainCheckboxes = document.getElementById('chain-checkboxes');
        const counts = {};
        STORES.forEach(s => { counts[s.c] = (counts[s.c]||0)+1; });
        chainCheckboxes.innerHTML = '';
        chains.forEach(chain => {
            const color = COLOR_MAP[chain] || '#7c8798';
            const btn = document.createElement('label');
            btn.className = 'chain-checkbox-btn active';
            btn.innerHTML = `
                <input type="checkbox" value="${chain}" checked/>
                <span class="dot" style="background:${color};"></span>
                ${chain}
                <span class="count">${counts[chain]||0}</span>
            `;
            chainCheckboxes.appendChild(btn);

            btn.addEventListener('click', function(e) {
                e.stopPropagation();
                const cb = this.querySelector('input[type="checkbox"]');
                cb.checked = !cb.checked;
                this.classList.toggle('active', cb.checked);
                applyFilters();
            });
        });

        // Остальные фильтры
        const allRegions = uniq(Object.values(DETAILS).map(d => d.region).filter(Boolean));
        const allCities = uniq(Object.values(DETAILS).map(d => d.city).filter(Boolean));
        fillSelect(document.getElementById('f-region'), allRegions, 'Все регионы');
        fillSelect(document.getElementById('f-city'), allCities, 'Все города');

        document.getElementById('btn-apply').addEventListener('click', applyFilters);
        document.getElementById('btn-reset').addEventListener('click', resetFilters);
        document.getElementById('f-search').addEventListener('keydown', e => { if (e.key==='Enter') applyFilters(); });
        document.getElementById('sel-population-filter').addEventListener('change', applyFilters);

        const filtersEl = document.getElementById('filters');
        const toggleBtn = document.getElementById('filter-toggle');
        let collapsed = false;
        toggleBtn.addEventListener('click', function() {
            collapsed = !collapsed;
            filtersEl.classList.toggle('collapsed', collapsed);
            toggleBtn.innerHTML = collapsed ? '⚙️' : '⚙️ Фильтры';
        });

        switchLayer('stores');
    }

    // ─── ЗАГРУЗКА ДАННЫХ ──────────────────────────────────────────────────
    async function loadAll() {
        try {
            const [storesRes, citiesRes, regionsRes, detailsRes] = await Promise.all([
                fetch('/data/stores.json'),
                fetch('/data/cities.json'),
                fetch('/data/regions.json'),
                fetch('/data/details.json')
            ]);
            if (!storesRes.ok || !citiesRes.ok || !regionsRes.ok || !detailsRes.ok) {
                throw new Error('Ошибка загрузки данных');
            }
            STORES = await storesRes.json();
            CITIES = await citiesRes.json();
            REGIONS_DATA = await regionsRes.json();
            DETAILS = await detailsRes.json();
            initMap();
        } catch (err) {
            document.getElementById('loading').innerHTML = `
                <div style="color:#E63946;font-size:18px;">❌ Ошибка загрузки данных</div>
                <div style="font-size:14px;color:#6b7a8d;margin-top:10px;">${err.message}</div>
                <button onclick="location.reload()" style="margin-top:20px;padding:10px 24px;border:none;border-radius:12px;background:#4f7cff;color:#fff;font-weight:600;cursor:pointer;">Попробовать снова</button>
            `;
            console.error('Load error:', err);
        }
    }

    loadAll();
    </script>
</body>
</html>
"""

# ============================================================
# ФИНАЛЬНАЯ СБОРКА HTML
# ============================================================
html_content = (
    HTML_TEMPLATE
    .replace("__COLOR_MAP__",        json.dumps(color_map, ensure_ascii=False))
    .replace("__CENTER_LAT__",       f"{center_lat:.8f}")
    .replace("__CENTER_LON__",       f"{center_lon:.8f}")
    .replace("__TOTAL_ALL_STORES__", str(int(total_stores_all)))
    .replace("__TOTAL_GEO_STORES__", str(int(total_stores_geo)))
)

with open(MAP_PATH, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"\n✅ Карта сохранена: {MAP_PATH}")
print(f"Размер HTML: {MAP_PATH.stat().st_size / 1024:.2f} KB")
print(f"\n✅ Готово! Данные разнесены в JSON-файлах.\n")