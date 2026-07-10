# generate_map.py - запускается один раз для генерации карты

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

REQUESTS_PER_KEY = 1200

DATA_DIR = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAP_PATH = DATA_DIR / "x5_stores_map_final.html"
GEO_CACHE_PATH = DATA_DIR / "dim_stores_geo.csv"
CITIES_XLSX = DATA_DIR / "Города.xlsx"

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
# ЗАГРУЗКА ДАННЫХ НАСЕЛЕНИЯ
# ============================================================
print("\nЗагружаем данные о населении из Excel...")
cities_pop_df = pd.read_excel(CITIES_XLSX)

# Нормализуем колонки (берём первые три: Город, Регион, Федеральный округ, Население)
# Подстраиваемся под реальные названия колонок
col_names = cities_pop_df.columns.tolist()
print(f"Колонки в Excel: {col_names}")

# Переименуем колонки в удобные имена
# Ожидаем: Город, Регион, Федеральный округ, Население, Статус города
rename_map = {}
for c in col_names:
    cl = str(c).lower().strip()
    if 'город' in cl and 'федерал' not in cl:
        rename_map[c] = 'city'
    elif 'регион' in cl:
        rename_map[c] = 'region'
    elif 'федерал' in cl:
        rename_map[c] = 'federal_district'
    elif 'насел' in cl:
        rename_map[c] = 'population'
    elif 'статус' in cl:
        rename_map[c] = 'status'

cities_pop_df = cities_pop_df.rename(columns=rename_map)
print(f"После переименования: {cities_pop_df.columns.tolist()}")

# Оставляем нужные колонки
needed_cols = [c for c in ['city', 'region', 'federal_district', 'population'] if c in cities_pop_df.columns]
cities_pop_df = cities_pop_df[needed_cols].copy()

# Чистим
cities_pop_df['city'] = cities_pop_df['city'].astype(str).str.strip()
cities_pop_df['population'] = pd.to_numeric(cities_pop_df['population'], errors='coerce')
cities_pop_df = cities_pop_df.dropna(subset=['population']).copy()
cities_pop_df['population'] = cities_pop_df['population'].astype(int)
cities_pop_df['city_lower'] = cities_pop_df['city'].str.lower().str.strip()

print(f"Городов в базе населения: {len(cities_pop_df)}")
print(cities_pop_df.head(5))

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

print("Загружаем fact Перекрёсток...")
fact_p = client.query_df(SQL_FACT_PEREKRESTOK)
print(f"  → {len(fact_p):,} строк")

print("Загружаем fact Пятёрочка...")
fact_5 = client.query_df(SQL_FACT_PYATEROCHKA)
print(f"  → {len(fact_5):,} строк")

print("Загружаем stores Перекрёсток...")
stores_p = client.query_df(SQL_STORES_PEREKRESTOK)
print(f"  → {len(stores_p):,} строк")

print("Загружаем stores Пятёрочка...")
stores_5 = client.query_df(SQL_STORES_PYATEROCHKA)
print(f"  → {len(stores_5):,} строк")

fact = pd.concat([fact_p, fact_5], ignore_index=True)
stores = pd.concat([stores_p, stores_5], ignore_index=True)

print(f"\n📦 Итого fact: {len(fact):,} строк")
print(f"📦 Итого stores: {len(stores):,} строк")

# ============================================================
# ХЕЛПЕРЫ
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
    return norm(x) or "Другое"

def make_full_address(row) -> str:
    region = norm(row.get("region_name", ""))
    city = norm(row.get("city_name", ""))
    address = clean_address(row.get("address", ""))
    parts = []
    if region:
        parts.append(region)
    if city and city.lower() not in address.lower():
        parts.append(city)
    if address:
        parts.append(address)
    return ", ".join(parts)

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
# НОРМАЛИЗАЦИЯ
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
# СПРАВОЧНИК МАГАЗИНОВ + ГЕОКОДИНГ
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

# ============================================================
# ГЕОКОДИНГ (с кэшем + ротация ключей)
# ============================================================
if GEO_CACHE_PATH.exists():
    geo_cache = pd.read_csv(GEO_CACHE_PATH, encoding="utf-8-sig")
    geo_cache = geo_cache.drop_duplicates("full_address", keep="last").copy()
    print(f"Кэш загружен: {len(geo_cache):,} записей")
else:
    geo_cache = pd.DataFrame(columns=[
        "full_address", "lat", "lon", "geo_precision", "geo_found_address"
    ])
    print("Кэш пустой — геокодим всё")

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
        tqdm(need_geo.iterrows(), total=len(need_geo), desc="Geocoding")
    ):
        if not key_rotator.has_capacity:
            print(f"\n⚠️ Все ключи исчерпаны! Остановка. Обработано: {ok + fail}")
            skipped = len(need_geo) - (ok + fail)
            break

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
            _tmp.to_csv(GEO_CACHE_PATH, index=False, encoding="utf-8-sig")

        time.sleep(0.15)

    print(f"\n✅ Успешно: {ok} | ❌ Ошибок: {fail} | ⏭️ Пропущено: {skipped}")

    updated = pd.concat([
        geo_cache,
        address_ref[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]]
    ]).drop_duplicates("full_address", keep="last")
    updated.to_csv(GEO_CACHE_PATH, index=False, encoding="utf-8-sig")
    print(f"Кэш обновлён: {GEO_CACHE_PATH} ({len(updated):,} записей)")

store_ref = store_ref.merge(
    address_ref[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]],
    on="full_address", how="left"
)

# ============================================================
# АГРЕГАЦИЯ — БАЗОВАЯ (по магазинам)
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

# топ-бренды
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

geo = store_sales.dropna(subset=["lat", "lon"]).copy()

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
# АГРЕГАЦИЯ ПО ГОРОДАМ — для слоя "на душу населения"
# ============================================================
print("\nАгрегируем продажи по городам...")

city_sales = (
    fact
    .groupby("city_name", as_index=False)
    .agg(
        city_qty=("sales_quantity", "sum"),
        city_rev=("sales_amount_rub", "sum"),
        city_stores=("store_key", "nunique"),
    )
)
city_sales["city_name_lower"] = city_sales["city_name"].str.lower().str.strip()

# Функция нормализации названий городов для джойна
def normalize_city_for_join(s):
    s = str(s).lower().strip()
    # Убираем "г.", "г ", "город " и т.д.
    s = re.sub(r'^г\.?\s*', '', s)
    s = re.sub(r'^город\s+', '', s)
    # Убираем "сп.", "д.", "с.", "пос.", "рп." и т.д.
    s = re.sub(r'^(сп\.|д\.|с\.|пос\.|рп\.|пгт\.?)\s*', '', s)
    # Берём первое слово если содержит запятую (адресная строка)
    if ',' in s:
        s = s.split(',')[0].strip()
    return s.strip()

city_sales["city_key"] = city_sales["city_name"].map(normalize_city_for_join)
cities_pop_df["city_key"] = cities_pop_df["city"].map(normalize_city_for_join)

# Джойн
city_merged = city_sales.merge(
    cities_pop_df[["city_key", "city", "region", "federal_district", "population"]],
    on="city_key",
    how="left"
)

# Для ненайденных — попробуем fuzzy через contains
not_found = city_merged[city_merged["population"].isna()].copy()
found = city_merged[city_merged["population"].notna()].copy()
print(f"  Городов найдено в базе населения: {len(found)}")
print(f"  Городов не найдено: {len(not_found)}")
if len(not_found) > 0:
    print(f"  Примеры ненайденных: {not_found['city_name'].head(10).tolist()}")

# Геокодируем центры городов для слоя "душа"
# Используем средние координаты магазинов в этом городе
city_coords = (
    geo
    .groupby("city_name", as_index=False)
    .agg(
        city_lat=("lat", "mean"),
        city_lon=("lon", "mean"),
    )
)
city_coords["city_key"] = city_coords["city_name"].map(normalize_city_for_join)

city_merged2 = city_merged.merge(
    city_coords[["city_key", "city_lat", "city_lon"]],
    on="city_key",
    how="left"
)

# Оставляем только города с координатами
city_layer = city_merged2.dropna(subset=["city_lat", "city_lon", "city_qty"]).copy()

# Считаем продажи на душу населения
city_layer["qty_per_capita"] = np.where(
    city_layer["population"].notna() & (city_layer["population"] > 0),
    city_layer["city_qty"] / city_layer["population"],
    np.nan
)

# Продажи на 1000 чел
city_layer["qty_per_1000"] = city_layer["qty_per_capita"] * 1000

print(f"\nГородов для слоя 'на душу': {len(city_layer)}")
print(f"Из них с данными о населении: {city_layer['population'].notna().sum()}")
print(f"Из них без данных о населении: {city_layer['population'].isna().sum()}")

# Топ-5 городов по продажам на душу
top5_pc = city_layer.dropna(subset=["qty_per_1000"]).nlargest(5, "qty_per_1000")
print("\nТоп-5 городов по продажам на 1000 жителей:")
for _, row in top5_pc.iterrows():
    print(f"  {row['city_name']}: {row['qty_per_1000']:.2f} шт/1000 чел (население: {row.get('population', 'N/A'):,})")

# ============================================================
# АГРЕГАЦИЯ ПО ФЕДЕРАЛЬНЫМ ОКРУГАМ — третий слой
# ============================================================
print("\nАгрегируем по федеральным округам...")

# Джойним факт с данными о ФО через city_key
fact_with_fd = fact.copy()
fact_with_fd["city_key"] = fact_with_fd["city_name"].map(normalize_city_for_join)

city_to_fd = cities_pop_df[["city_key", "federal_district", "region", "population"]].drop_duplicates("city_key")
fact_with_fd = fact_with_fd.merge(city_to_fd, on="city_key", how="left")

fd_sales = (
    fact_with_fd.dropna(subset=["federal_district"])
    .groupby("federal_district", as_index=False)
    .agg(
        fd_qty=("sales_quantity", "sum"),
        fd_rev=("sales_amount_rub", "sum"),
        fd_stores=("store_key", "nunique"),
        fd_cities=("city_name", "nunique"),
    )
)

# Суммарное население по ФО
pop_by_fd = (
    cities_pop_df
    .groupby("federal_district", as_index=False)
    .agg(fd_population=("population", "sum"))
)

fd_sales = fd_sales.merge(pop_by_fd, on="federal_district", how="left")
fd_sales["fd_qty_per_1000"] = np.where(
    fd_sales["fd_population"] > 0,
    fd_sales["fd_qty"] / fd_sales["fd_population"] * 1000,
    np.nan
)

# Координаты центров ФО (приблизительные)
FD_CENTERS = {
    "Центральный":          (55.75, 37.60),
    "Северо-Западный":      (59.93, 30.32),
    "Южный":                (45.04, 38.98),
    "Северо-Кавказский":    (43.50, 43.60),
    "Приволжский":          (56.26, 50.19),
    "Уральский":            (60.60, 56.83),
    "Сибирский":            (54.99, 82.90),
    "Дальневосточный":      (51.50, 133.60),
}

fd_sales["fd_lat"] = fd_sales["federal_district"].map(lambda x: FD_CENTERS.get(x, (None, None))[0])
fd_sales["fd_lon"] = fd_sales["federal_district"].map(lambda x: FD_CENTERS.get(x, (None, None))[1])

print("Федеральные округа:")
for _, row in fd_sales.iterrows():
    print(f"  {row['federal_district']}: {row['fd_qty']:,.0f} шт, {row.get('fd_qty_per_1000', 0):.2f}/1000 чел")

# ============================================================
# ПОДГОТОВКА JSON ДЛЯ КАРТЫ
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
}

if len(geo) == 0:
    print("ВНИМАНИЕ: Нет данных для отображения на карте!")
    exit()

center_lat = float(geo["lat"].mean())
center_lon = float(geo["lon"].mean())
max_rev = max(float(geo["revenue_rub"].max()), 1.0)

# --- Слой 1: магазины ---
stores_data = []
for _, row in geo.iterrows():
    sk = row["store_key"]
    rev = float(row["revenue_rub"])
    r = max(5.0, min(23.0, 5 + 18 * math.sqrt(rev / max_rev)))

    stores_data.append({
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

# --- Слой 2: города (продажи на душу) ---
max_qty_city = float(city_layer["city_qty"].max()) if len(city_layer) > 0 else 1.0
max_pc = float(city_layer["qty_per_1000"].max(skipna=True)) if city_layer["qty_per_1000"].notna().any() else 1.0

cities_data = []
for _, row in city_layer.iterrows():
    qty = float(row["city_qty"])
    r_abs = max(6.0, min(40.0, 6 + 34 * math.sqrt(qty / max(max_qty_city, 1))))
    pc = safe_float(row.get("qty_per_1000"))
    # r для per-capita (нормируем)
    r_pc = max(6.0, min(40.0, 6 + 34 * math.sqrt((pc or 0) / max(max_pc, 1)))) if pc else 6.0

    cities_data.append({
        "city": norm(row.get("city_name")),
        "city_official": norm(row.get("city")) or norm(row.get("city_name")),
        "region": norm(row.get("region")) or norm(row.get("city_name")),
        "federal_district": norm(row.get("federal_district")) or "—",
        "lat": float(row["city_lat"]),
        "lon": float(row["city_lon"]),
        "qty": qty,
        "rev": float(row["city_rev"]),
        "stores": safe_int(row.get("city_stores")),
        "population": safe_int(row.get("population")),
        "qty_per_1000": pc,
        "radius_abs": round(r_abs, 2),
        "radius_pc": round(r_pc, 2),
    })

# --- Слой 3: федеральные округа ---
max_fd_qty = float(fd_sales["fd_qty"].max()) if len(fd_sales) > 0 else 1.0
max_fd_pc = float(fd_sales["fd_qty_per_1000"].max(skipna=True)) if fd_sales["fd_qty_per_1000"].notna().any() else 1.0

fd_data = []
for _, row in fd_sales.dropna(subset=["fd_lat", "fd_lon"]).iterrows():
    qty = float(row["fd_qty"])
    r = max(20.0, min(70.0, 20 + 50 * math.sqrt(qty / max(max_fd_qty, 1))))
    pc = safe_float(row.get("fd_qty_per_1000"))

    fd_data.append({
        "name": norm(row.get("federal_district")),
        "lat": float(row["fd_lat"]),
        "lon": float(row["fd_lon"]),
        "qty": qty,
        "rev": float(row["fd_rev"]),
        "stores": safe_int(row.get("fd_stores")),
        "cities": safe_int(row.get("fd_cities")),
        "population": safe_int(row.get("fd_population")),
        "qty_per_1000": pc,
        "radius": round(r, 2),
    })

stores_json = json.dumps(stores_data, ensure_ascii=False)
cities_json = json.dumps(cities_data, ensure_ascii=False)
fd_json = json.dumps(fd_data, ensure_ascii=False)
color_map_json = json.dumps(color_map, ensure_ascii=False)

print(f"\nТочек (магазины): {len(stores_data)}")
print(f"Точек (города): {len(cities_data)}")
print(f"Точек (ФО): {len(fd_data)}")

# ============================================================
# HTML ТЕМПЛЕЙТ
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
        html, body, #map {
            height:100%; width:100%;
            font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;
        }
        #map { background:#f0f4f9; }

        /* TOP PANEL */
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

        /* LAYER SWITCHER */
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

        /* FILTER TOGGLE */
        #filter-toggle {
            position:fixed; top:20px; right:24px; z-index:10001;
            border:none; border-radius:14px; padding:10px 18px;
            background:rgba(255,255,255,0.92); backdrop-filter:blur(12px);
            color:#1a2634; font-weight:600; font-size:13px; cursor:pointer;
            box-shadow:0 4px 20px rgba(0,0,0,0.08); border:1px solid rgba(255,255,255,0.6);
            transition:all 0.2s ease; display:flex; align-items:center; gap:8px;
        }
        #filter-toggle:hover { background:rgba(255,255,255,0.98); transform:translateY(-1px); }

        /* FILTERS PANEL */
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

        .chain-filter-group { margin-bottom:14px; }
        .chain-filter-group label.group-label { display:block; font-size:11px; font-weight:600; color:#6b7a8d; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px; }
        .chain-checkbox { display:flex; align-items:center; gap:8px; margin:6px 0; cursor:pointer; font-size:13px; color:#1a2634; }
        .chain-checkbox input[type="checkbox"] { width:16px; height:16px; accent-color:#4f7cff; cursor:pointer; }
        .chain-dot { width:12px; height:12px; border-radius:50%; flex-shrink:0; border:1px solid rgba(0,0,0,0.06); }

        .filters-note { margin-top:16px; padding-top:14px; border-top:1px solid #eef2f7; font-size:12px; color:#6b7a8d; line-height:1.6; }

        /* LEGEND */
        #legend {
            position:fixed; left:20px; bottom:24px; z-index:10000;
            background:rgba(255,255,255,0.92); backdrop-filter:blur(12px);
            border-radius:16px; padding:16px 20px;
            box-shadow:0 4px 24px rgba(0,0,0,0.08); border:1px solid rgba(255,255,255,0.6);
            min-width:200px; max-width:260px;
        }
        .legend-title { font-weight:700; font-size:13px; color:#1a2634; margin-bottom:10px; }
        .legend-row { display:flex; align-items:center; gap:10px; margin:6px 0; font-size:12px; color:#324155; }
        .legend-dot { width:12px; height:12px; border-radius:50%; flex-shrink:0; border:1px solid rgba(0,0,0,0.06); }
        .legend-count { font-weight:600; color:#8a99aa; margin-left:auto; font-size:11px; }
        .legend-divider { border:none; border-top:1px solid #eef2f7; margin:10px 0; }
        .legend-hint { font-size:11px; color:#8a99aa; line-height:1.5; }

        /* COLORBAR для слоя душа населения */
        .colorbar-wrap { margin-top:8px; }
        .colorbar { height:10px; border-radius:8px; margin:4px 0; }
        .colorbar-labels { display:flex; justify-content:space-between; font-size:10px; color:#8a99aa; }

        /* POPUPS */
        .store-popup { width:400px; font-family:inherit; color:#1a2634; }
        .city-popup { width:340px; font-family:inherit; color:#1a2634; }
        .fd-popup { width:320px; font-family:inherit; color:#1a2634; }

        .popup-header { padding:16px 20px; border-radius:16px 16px 0 0; }
        .popup-header .badge { display:inline-block; background:rgba(255,255,255,0.2); padding:2px 10px; border-radius:20px; font-size:10px; font-weight:600; color:#fff; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:6px; }
        .popup-title { font-size:17px; font-weight:800; line-height:1.3; color:#fff; }
        .popup-subtitle { margin-top:4px; font-size:12px; opacity:0.9; color:#fff; }

        .popup-body { background:#fafcfe; border:1px solid #eef2f7; border-top:none; border-radius:0 0 16px 16px; padding:16px 20px 20px; }
        .popup-meta { display:flex; flex-wrap:wrap; gap:4px 16px; font-size:12px; color:#6b7a8d; margin-bottom:14px; padding-bottom:12px; border-bottom:1px solid #eef2f7; }

        .kpi-grid { display:grid; grid-template-columns:1fr 1fr; gap:8px; margin-bottom:14px; }
        .kpi-grid-3 { display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px; margin-bottom:14px; }
        .mini-kpi { background:#fff; border:1px solid #eef2f7; border-radius:12px; padding:10px 12px; }
        .mini-kpi .val { font-size:15px; font-weight:700; color:#1a2634; }
        .mini-kpi .lbl { margin-top:3px; font-size:10px; font-weight:600; color:#8a99aa; text-transform:uppercase; letter-spacing:0.3px; }

        .per-capita-kpi { background:linear-gradient(135deg,#667eea 0%,#764ba2 100%); border-radius:14px; padding:14px 16px; margin-bottom:12px; text-align:center; }
        .per-capita-kpi .big-val { font-size:28px; font-weight:900; color:#fff; }
        .per-capita-kpi .big-lbl { font-size:11px; color:rgba(255,255,255,0.8); margin-top:4px; text-transform:uppercase; letter-spacing:0.5px; }

        .section-title { margin:14px 0 8px; font-size:11px; font-weight:700; color:#6b7a8d; text-transform:uppercase; letter-spacing:0.5px; }
        .spark-box { background:#fff; border:1px solid #eef2f7; border-radius:12px; padding:6px 8px; margin-bottom:6px; }

        .brand-list { background:#fff; border:1px solid #eef2f7; border-radius:12px; padding:10px 12px; max-height:300px; overflow-y:auto; }
        .brand-row { display:grid; grid-template-columns:1fr auto; gap:10px; align-items:center; margin-bottom:8px; }
        .brand-row:last-child { margin-bottom:0; }
        .brand-name { font-size:12px; font-weight:600; color:#1a2634; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
        .brand-bar-wrap { height:6px; background:#eef2f7; border-radius:999px; overflow:hidden; margin-top:3px; }
        .brand-bar { height:6px; border-radius:999px; }
        .brand-val { font-size:12px; font-weight:600; color:#6b7a8d; white-space:nowrap; }

        .show-more-btn {
            margin-top:10px; padding:8px 16px; background:#f0f4f9; border:1px solid #e4e9f0;
            border-radius:10px; font-size:12px; font-weight:600; color:#4f7cff; cursor:pointer;
            width:100%; font-family:inherit;
            display:flex; align-items:center; justify-content:center; gap:8px;
        }

        .popup-footer { margin-top:12px; padding-top:10px; border-top:1px solid #eef2f7; font-size:11px; color:#8a99aa; }

        .leaflet-popup-content-wrapper { border-radius:16px !important; padding:0 !important; overflow:hidden; box-shadow:0 16px 48px rgba(0,0,0,0.18) !important; }
        .leaflet-popup-content { margin:0 !important; min-width:300px; }
        .leaflet-popup-close-button { top:12px !important; right:12px !important; color:rgba(255,255,255,0.7) !important; font-size:20px !important; font-weight:300 !important; }
        .leaflet-popup-close-button:hover { color:#fff !important; }

        /* слой без фильтров */
        .layer-notice {
            background:#FFF8E1; border:1px solid #FFE082; border-radius:10px; padding:8px 12px;
            font-size:12px; color:#795548; margin-bottom:12px; display:none;
        }
    </style>
</head>
<body>
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
                <div class="layer-sub">Продажи на душу нас.</div>
            </div>
        </label>
        <label class="layer-option" id="lopt-fd" onclick="switchLayer('fd')">
            <input type="radio" name="layer" value="fd"/>
            <span class="layer-icon">🗺️</span>
            <div>
                <div class="layer-label">Фед. округа</div>
                <div class="layer-sub">Сводка по округам</div>
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

        <!-- Фильтр по сети — только для слоя магазинов -->
        <div class="chain-filter-group" id="chain-filter-block">
            <label class="group-label">Сеть</label>
            <div id="chain-checkboxes"></div>
        </div>

        <div class="filter-group">
            <label>Регион</label>
            <select id="f-region"><option value="">Все регионы</option></select>
        </div>
        <div class="filter-group">
            <label>Город</label>
            <select id="f-city"><option value="">Все города</option></select>
        </div>
        <div class="filter-group" id="format-filter-block">
            <label>Формат магазина</label>
            <select id="f-format"><option value="">Все форматы</option></select>
        </div>
        <div class="filter-group" id="fd-filter-block" style="display:none;">
            <label>Федеральный округ</label>
            <select id="f-fd"><option value="">Все округа</option></select>
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
    // ─── DATA ────────────────────────────────────────────────────────────
    const STORES   = __STORES_JSON__;
    const CITIES   = __CITIES_JSON__;
    const FD_DATA  = __FD_JSON__;
    const COLOR_MAP = __COLOR_MAP__;
    const DEFAULT_CENTER = [__CENTER_LAT__, __CENTER_LON__];
    const TOTAL_ALL_STORES = __TOTAL_ALL_STORES__;
    const TOTAL_GEO_STORES = __TOTAL_GEO_STORES__;

    // ─── MAP INIT ────────────────────────────────────────────────────────
    const map = L.map('map', { zoomControl:true, preferCanvas:true }).setView(DEFAULT_CENTER, 5);
    L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
        maxZoom:19, attribution:'© OpenStreetMap © CARTO'
    }).addTo(map);

    // ─── LAYERS ──────────────────────────────────────────────────────────
    const cluster = L.markerClusterGroup({ chunkedLoading:true, spiderfyOnMaxZoom:true, showCoverageOnHover:false, maxClusterRadius:60 });
    const cityLayerGroup = L.layerGroup();
    const fdLayerGroup   = L.layerGroup();

    map.addLayer(cluster); // default

    let currentLayer = 'stores';

    // ─── UI REFS ─────────────────────────────────────────────────────────
    const regionSelect  = document.getElementById('f-region');
    const citySelect    = document.getElementById('f-city');
    const formatSelect  = document.getElementById('f-format');
    const fdSelect      = document.getElementById('f-fd');
    const searchInput   = document.getElementById('f-search');
    const filtersNote   = document.getElementById('filters-note');
    const chainCheckboxes = document.getElementById('chain-checkboxes');
    const legendEl      = document.getElementById('legend');
    const chainBlock    = document.getElementById('chain-filter-block');
    const formatBlock   = document.getElementById('format-filter-block');
    const fdBlock       = document.getElementById('fd-filter-block');
    const layerNotice   = document.getElementById('layer-notice');

    // ─── UTILS ───────────────────────────────────────────────────────────
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

    // ─── COLORSCALE (для слоя cities) ────────────────────────────────────
    // Зелёный (низко) → Жёлтый → Оранжевый → Красный (высоко)
    function perCapitaColor(value, min, max) {
        if (value === null || value === undefined) return '#aaaaaa';
        const t = Math.max(0, Math.min(1, (value - min) / Math.max(max - min, 0.001)));
        // Градиент: синий → голубой → зелёный → жёлтый → оранжевый → красный
        const stops = [
            [0,   [99,  179, 237]],  // голубой
            [0.25,[72,  187, 120]],  // зелёный
            [0.5, [246, 224,  94]],  // жёлтый
            [0.75,[237, 137,  54]],  // оранжевый
            [1.0, [197,  48,  48]],  // красный
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

    // Цвета для ФО
    const FD_COLORS = [
        '#E63946','#457B9D','#2A9D8F','#E9C46A',
        '#F4A261','#264653','#A8DADC','#6D6875'
    ];

    // ─── INIT SELECTS ─────────────────────────────────────────────────────
    const CHAINS  = uniq(STORES.map(x => x.chain));
    const REGIONS = uniq(STORES.map(x => x.region));
    const FORMATS = uniq(STORES.map(x => x.format).filter(x => x && x !== '—'));
    const FDS     = uniq(FD_DATA.map(x => x.name));
    const ALL_CITIES = uniq([...STORES.map(x => x.city), ...CITIES.map(x => x.city)]);

    fillSelect(regionSelect, REGIONS, 'Все регионы');
    fillSelect(citySelect, ALL_CITIES, 'Все города');
    fillSelect(formatSelect, FORMATS, 'Все форматы');
    fillSelect(fdSelect, FDS, 'Все округа');

    // Чекбоксы по сетям
    function buildChainCheckboxes() {
        const counts = {};
        STORES.forEach(s => { counts[s.chain] = (counts[s.chain]||0)+1; });
        chainCheckboxes.innerHTML = '';
        CHAINS.forEach(chain => {
            const color = COLOR_MAP[chain] || '#7c8798';
            const lbl = document.createElement('label');
            lbl.className = 'chain-checkbox';
            lbl.innerHTML = `
                <input type="checkbox" value="${chain}" checked/>
                <span class="chain-dot" style="background:${color};"></span>
                ${chain} <span style="color:#8a99aa;font-size:11px;">(${counts[chain]||0})</span>`;
            chainCheckboxes.appendChild(lbl);
        });
    }
    buildChainCheckboxes();

    function getSelectedChains() {
        return [...chainCheckboxes.querySelectorAll('input[type="checkbox"]')]
            .filter(cb => cb.checked).map(cb => cb.value);
    }

    function updateCityOptions() {
        const region = regionSelect.value;
        let rows = (currentLayer === 'cities') ? CITIES : STORES;
        if (region) rows = rows.filter(x => x.region === region);
        if (currentLayer === 'stores') {
            const chains = getSelectedChains();
            if (chains.length < CHAINS.length) rows = rows.filter(x => chains.includes(x.chain));
        }
        const cities = uniq(rows.map(x => x.city));
        const cur = citySelect.value;
        fillSelect(citySelect, cities, 'Все города');
        if (cities.includes(cur)) citySelect.value = cur;
    }
    regionSelect.addEventListener('change', updateCityOptions);

    // ─── SPARKLINE ────────────────────────────────────────────────────────
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

    // ─── BRANDS HTML ──────────────────────────────────────────────────────
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

    // ─── POPUPS ────────────────────────────────────────────────────────────
    function makeStorePopup(store) {
        const color = COLOR_MAP[store.chain] || '#7c8798';
        return `<div class="store-popup">
            <div class="popup-header" style="background:linear-gradient(135deg,${color} 0%,#1a2634 100%);">
                <div class="badge">${esc(store.chain||'—')}</div>
                <div class="popup-title">🏪 ${esc(store.name)}</div>
                <div class="popup-subtitle">${esc(store.format||'—')} · code: ${esc(store.store_code||'—')}</div>
            </div>
            <div class="popup-body">
                <div class="popup-meta">
                    <span>📍 ${esc(store.address||'—')}</span>
                    <span>🏙️ ${esc(store.city||'—')}</span>
                    <span>🧭 ${esc(store.geo_precision||'—')}</span>
                </div>
                <div class="kpi-grid">
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtMoney(store.rev)}</div><div class="lbl">Выручка</div></div>
                    <div class="mini-kpi"><div class="val" style="color:#457B9D;">${fmtNum(store.qty)} шт</div><div class="lbl">Продажи</div></div>
                    <div class="mini-kpi"><div class="val">${fmtPrice(store.avg_price)}</div><div class="lbl">Средняя цена</div></div>
                    <div class="mini-kpi"><div class="val">${fmtPrice(store.avg_cost)}</div><div class="lbl">Себестоимость</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(store.periods_count)}</div><div class="lbl">Периодов</div></div>
                    <div class="mini-kpi"><div class="val">${fmtMoney(store.avg_month_rev)}</div><div class="lbl">Ср. выручка/мес</div></div>
                </div>
                <div class="section-title">📈 Динамика выручки</div>
                <div class="spark-box">${sparkline(store.dyn?.rev||[], color)}</div>
                <div class="section-title">📦 Динамика продаж</div>
                <div class="spark-box">${sparkline(store.dyn?.qty||[], '#457B9D')}</div>
                <div class="section-title">🏷️ Бренды</div>
                ${buildBrandsHtml(store.all_brands||[])}
                <div class="popup-footer">Брендов: <strong>${fmtNum(store.brands_count)}</strong> · Топ: <strong>${esc(store.top_brand||'—')}</strong></div>
            </div>
        </div>`;
    }

    function makeCityPopup(city) {
        const pc = city.qty_per_1000;
        const hasPopulation = city.population !== null && city.population !== undefined;
        return `<div class="city-popup">
            <div class="popup-header" style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);">
                <div class="badge">ГОРОД</div>
                <div class="popup-title">🏙️ ${esc(city.city)}</div>
                <div class="popup-subtitle">${esc(city.federal_district||'—')} · ${esc(city.region||'—')}</div>
            </div>
            <div class="popup-body">
                ${hasPopulation ? `
                <div class="per-capita-kpi">
                    <div class="big-val">${fmtPc(pc)}</div>
                    <div class="big-lbl">продаж на 1 000 жителей</div>
                </div>` : `<div style="background:#fff3cd;border-radius:10px;padding:10px 14px;margin-bottom:12px;font-size:12px;color:#856404;">
                    ℹ️ Данные о населении не найдены — показатель н/д
                </div>`}
                <div class="kpi-grid">
                    <div class="mini-kpi"><div class="val" style="color:#764ba2;">${fmtNum(city.qty)} шт</div><div class="lbl">Всего продаж</div></div>
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtMoney(city.rev)}</div><div class="lbl">Выручка</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(city.stores)}</div><div class="lbl">Магазинов</div></div>
                    <div class="mini-kpi"><div class="val">${hasPopulation ? fmtNum(city.population) : '—'}</div><div class="lbl">Население</div></div>
                </div>
            </div>
        </div>`;
    }

    function makeFdPopup(fd, color) {
        const pc = fd.qty_per_1000;
        return `<div class="fd-popup">
            <div class="popup-header" style="background:linear-gradient(135deg,${color} 0%,#1a2634 100%);">
                <div class="badge">ФЕДЕРАЛЬНЫЙ ОКРУГ</div>
                <div class="popup-title">🗺️ ${esc(fd.name)}</div>
            </div>
            <div class="popup-body">
                ${pc !== null && pc !== undefined ? `
                <div class="per-capita-kpi" style="background:linear-gradient(135deg,${color} 0%,#1a2634 100%);">
                    <div class="big-val">${fmtPc(pc)}</div>
                    <div class="big-lbl">продаж на 1 000 жителей</div>
                </div>` : ''}
                <div class="kpi-grid-3">
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtNum(fd.qty)}</div><div class="lbl">Продажи, шт</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(fd.stores)}</div><div class="lbl">Магазинов</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(fd.cities)}</div><div class="lbl">Городов</div></div>
                </div>
                <div class="kpi-grid">
                    <div class="mini-kpi"><div class="val" style="color:#E63946;">${fmtMoney(fd.rev)}</div><div class="lbl">Выручка</div></div>
                    <div class="mini-kpi"><div class="val">${fmtNum(fd.population)}</div><div class="lbl">Население (ГиС)</div></div>
                </div>
            </div>
        </div>`;
    }

    // ─── KPI UPDATE ────────────────────────────────────────────────────────
    function updateKpi(rows, layer) {
        const rev = rows.reduce((a,x)=>a+Number(x.rev||x.city_rev||x.fd_rev||0),0);
        const qty = rows.reduce((a,x)=>a+Number(x.qty||x.city_qty||x.fd_qty||0),0);
        document.getElementById('kpi-rev').textContent = fmtMoney(rev);
        document.getElementById('kpi-qty').textContent = fmtNum(qty)+' шт';

        if (layer === 'stores') {
            document.getElementById('kpi-stores').textContent = fmtNum(rows.length);
            document.getElementById('kpi-stores-lbl').textContent = 'Магазинов';
            document.getElementById('panel-title').textContent = 'Федеральные сети';
            document.getElementById('panel-sub').textContent = 'Картофельные чипсы — магазины';
            filtersNote.innerHTML = `Всего: <strong>${fmtNum(TOTAL_ALL_STORES)}</strong> · На карте: <strong>${fmtNum(TOTAL_GEO_STORES)}</strong> · Показано: <strong>${fmtNum(rows.length)}</strong>`;
        } else if (layer === 'cities') {
            document.getElementById('kpi-stores').textContent = fmtNum(rows.length);
            document.getElementById('kpi-stores-lbl').textContent = 'Городов';
            document.getElementById('panel-title').textContent = 'Продажи на душу населения';
            document.getElementById('panel-sub').textContent = 'Картофельные чипсы — шт / 1 000 жителей';
            const withPop = rows.filter(x => x.qty_per_1000 !== null).length;
            filtersNote.innerHTML = `Городов с данными о населении: <strong>${fmtNum(withPop)}</strong> из <strong>${fmtNum(rows.length)}</strong>`;
        } else if (layer === 'fd') {
            document.getElementById('kpi-stores').textContent = fmtNum(rows.length);
            document.getElementById('kpi-stores-lbl').textContent = 'Округов';
            document.getElementById('panel-title').textContent = 'Федеральные округа';
            document.getElementById('panel-sub').textContent = 'Картофельные чипсы — сводка по округам';
            filtersNote.innerHTML = `Данные по <strong>${fmtNum(rows.length)}</strong> федеральным округам`;
        }
    }

    // ─── LEGEND ────────────────────────────────────────────────────────────
    function updateLegendStores(rows) {
        const counts = {};
        rows.forEach(s => { counts[s.chain] = (counts[s.chain]||0)+1; });
        let html = '<div class="legend-title">🏪 Сети</div>';
        CHAINS.forEach(chain => {
            const color = COLOR_MAP[chain] || '#7c8798';
            html += `<div class="legend-row">
                <span class="legend-dot" style="background:${color};"/>
                ${chain}
                <span class="legend-count">${counts[chain]||0}</span>
            </div>`;
        });
        html += '<hr class="legend-divider"/><div class="legend-hint">Размер точки = выручка<br>Цвет = сеть</div>';
        legendEl.innerHTML = html;
    }

    function updateLegendCities(rows) {
        const vals = rows.map(x=>x.qty_per_1000).filter(x=>x!==null&&x!==undefined);
        const minV = vals.length ? Math.min(...vals) : 0;
        const maxV = vals.length ? Math.max(...vals) : 1;
        // Colorbar
        const grad = 'linear-gradient(to right, rgb(99,179,237), rgb(72,187,120), rgb(246,224,94), rgb(237,137,54), rgb(197,48,48))';
        let html = `<div class="legend-title">🏙️ Продажи / 1 000 жителей</div>
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
        legendEl.innerHTML = html;
    }

    function updateLegendFd(fdRows) {
        let html = '<div class="legend-title">🗺️ Федеральные округа</div>';
        fdRows.forEach((fd, i) => {
            const color = FD_COLORS[i % FD_COLORS.length];
            html += `<div class="legend-row">
                <span class="legend-dot" style="background:${color};"/>
                <span style="font-size:11px;">${fd.name}</span>
                <span class="legend-count">${fmtNum(fd.qty)} шт</span>
            </div>`;
        });
        html += '<hr class="legend-divider"/><div class="legend-hint">Размер = кол-во продаж</div>';
        legendEl.innerHTML = html;
    }

    // ─── RENDER FUNCTIONS ─────────────────────────────────────────────────

    function renderStores(rows) {
        cluster.clearLayers();
        rows.forEach(store => {
            const color = COLOR_MAP[store.chain] || '#7c8798';
            const marker = L.circleMarker([store.lat, store.lon], {
                radius: store.radius, color: color, weight:2,
                fillColor: color, fillOpacity:0.7, opacity:0.9
            });
            marker.bindTooltip(`<strong>${esc(store.name)}</strong><br>${esc(store.chain)} · ${fmtMoney(store.rev)}`, {direction:'top',sticky:true});
            marker.bindPopup(makeStorePopup(store), {maxWidth:430, closeButton:true});
            cluster.addLayer(marker);
        });
        updateKpi(rows, 'stores');
        updateLegendStores(rows);
    }

    function renderCities(rows) {
        cityLayerGroup.clearLayers();
        // Считаем min/max для colorscale
        const pcVals = rows.map(x=>x.qty_per_1000).filter(x=>x!==null&&x!==undefined);
        const minPc = pcVals.length ? Math.min(...pcVals) : 0;
        const maxPc = pcVals.length ? Math.max(...pcVals) : 1;

        rows.forEach(city => {
            const color = perCapitaColor(city.qty_per_1000, minPc, maxPc);
            // Размер — абсолютные продажи
            const marker = L.circleMarker([city.lat, city.lon], {
                radius: city.radius_pc || 8,
                color: '#fff', weight: 2,
                fillColor: color, fillOpacity: 0.82, opacity: 0.9
            });
            const pcStr = city.qty_per_1000 !== null ? fmtPc(city.qty_per_1000)+' шт/1000' : 'нет данных';
            marker.bindTooltip(`<strong>${esc(city.city)}</strong><br>На 1000 жит.: <b>${pcStr}</b><br>Продаж: ${fmtNum(city.qty)} шт`, {direction:'top', sticky:true});
            marker.bindPopup(makeCityPopup(city), {maxWidth:380, closeButton:true});
            cityLayerGroup.addLayer(marker);
        });
        updateKpi(rows, 'cities');
        updateLegendCities(rows);
    }

    function renderFd(rows) {
        fdLayerGroup.clearLayers();
        rows.forEach((fd, i) => {
            const color = FD_COLORS[i % FD_COLORS.length];
            const marker = L.circleMarker([fd.lat, fd.lon], {
                radius: fd.radius,
                color: '#fff', weight: 3,
                fillColor: color, fillOpacity: 0.65, opacity: 1.0
            });
            marker.bindTooltip(
                `<strong>${esc(fd.name)}</strong><br>Продажи: ${fmtNum(fd.qty)} шт<br>На 1000 жит.: ${fmtPc(fd.qty_per_1000)}`,
                {direction:'top', sticky:true}
            );
            marker.bindPopup(makeFdPopup(fd, color), {maxWidth:360, closeButton:true});
            fdLayerGroup.addLayer(marker);
        });
        updateKpi(rows, 'fd');
        updateLegendFd(rows);
    }

    // ─── FILTER LOGIC ──────────────────────────────────────────────────────
    function getFilteredStores() {
        const chains = getSelectedChains();
        const region = regionSelect.value;
        const city   = citySelect.value;
        const fmt    = formatSelect.value;
        const search = (searchInput.value||'').trim().toLowerCase();
        return STORES.filter(s => {
            if (!chains.includes(s.chain)) return false;
            if (region && s.region !== region) return false;
            if (city   && s.city   !== city)   return false;
            if (fmt    && s.format !== fmt)    return false;
            if (search) {
                const hay = [s.name,s.address,s.city,s.region,s.store_code,s.chain].join(' ').toLowerCase();
                if (!hay.includes(search)) return false;
            }
            return true;
        });
    }

    function getFilteredCities() {
        const region = regionSelect.value;
        const city   = citySelect.value;
        const search = (searchInput.value||'').trim().toLowerCase();
        return CITIES.filter(c => {
            if (region && c.region !== region) return false;
            if (city   && c.city   !== city)   return false;
            if (search) {
                const hay = [c.city, c.region, c.federal_district].join(' ').toLowerCase();
                if (!hay.includes(search)) return false;
            }
            return true;
        });
    }

    function getFilteredFd() {
        const fdVal  = fdSelect.value;
        const search = (searchInput.value||'').trim().toLowerCase();
        return FD_DATA.filter(fd => {
            if (fdVal && fd.name !== fdVal) return false;
            if (search && !fd.name.toLowerCase().includes(search)) return false;
            return true;
        });
    }

    // ─── SWITCH LAYER ─────────────────────────────────────────────────────
    window.switchLayer = function(layer) {
        currentLayer = layer;

        // Обновляем active-стиль
        ['stores','cities','fd'].forEach(l => {
            document.getElementById(`lopt-${l}`).classList.toggle('active', l===layer);
        });

        // Скрываем/показываем слои
        if (layer === 'stores') {
            map.addLayer(cluster);
            map.removeLayer(cityLayerGroup);
            map.removeLayer(fdLayerGroup);
            chainBlock.style.display = '';
            formatBlock.style.display = '';
            fdBlock.style.display = 'none';
            layerNotice.style.display = 'none';
        } else if (layer === 'cities') {
            map.removeLayer(cluster);
            map.addLayer(cityLayerGroup);
            map.removeLayer(fdLayerGroup);
            chainBlock.style.display = 'none';
            formatBlock.style.display = 'none';
            fdBlock.style.display = 'none';
            layerNotice.style.display = '';
        } else if (layer === 'fd') {
            map.removeLayer(cluster);
            map.removeLayer(cityLayerGroup);
            map.addLayer(fdLayerGroup);
            chainBlock.style.display = 'none';
            formatBlock.style.display = 'none';
            fdBlock.style.display = '';
            layerNotice.style.display = '';
        }

        // Обновляем опции городов в фильтре
        updateCityOptions();
        applyFilters();
    };

    function applyFilters() {
        if (currentLayer === 'stores') {
            const rows = getFilteredStores();
            renderStores(rows);
        } else if (currentLayer === 'cities') {
            const rows = getFilteredCities();
            renderCities(rows);
        } else if (currentLayer === 'fd') {
            const rows = getFilteredFd();
            renderFd(rows);
        }
    }

    function resetFilters() {
        // Сбрасываем чекбоксы
        [...chainCheckboxes.querySelectorAll('input[type="checkbox"]')].forEach(cb => cb.checked=true);
        regionSelect.value = '';
        fillSelect(citySelect, ALL_CITIES, 'Все города');
        citySelect.value  = '';
        formatSelect.value = '';
        fdSelect.value    = '';
        searchInput.value = '';
        applyFilters();
        map.setView(DEFAULT_CENTER, 5);
    }

    document.getElementById('btn-apply').addEventListener('click', applyFilters);
    document.getElementById('btn-reset').addEventListener('click', resetFilters);
    searchInput.addEventListener('keydown', e => { if (e.key==='Enter') applyFilters(); });

    // Filter toggle
    const filtersEl = document.getElementById('filters');
    const toggleBtn = document.getElementById('filter-toggle');
    let collapsed = false;
    toggleBtn.addEventListener('click', function() {
        collapsed = !collapsed;
        filtersEl.classList.toggle('collapsed', collapsed);
        toggleBtn.innerHTML = collapsed ? '⚙️' : '⚙️ Фильтры';
    });

    // ─── INIT ─────────────────────────────────────────────────────────────
    // Рендерим все слои сразу (данные готовы)
    renderStores(STORES);
    renderCities(CITIES);
    renderFd(FD_DATA);

    // По умолчанию показываем только stores
    map.addLayer(cluster);
    map.removeLayer(cityLayerGroup);
    map.removeLayer(fdLayerGroup);

    updateKpi(STORES, 'stores');
    updateLegendStores(STORES);

    </script>
</body>
</html>
"""

# ============================================================
# ФИНАЛЬНАЯ СБОРКА
# ============================================================
html_content = (
    HTML_TEMPLATE
    .replace("__STORES_JSON__",      stores_json)
    .replace("__CITIES_JSON__",      cities_json)
    .replace("__FD_JSON__",          fd_json)
    .replace("__COLOR_MAP__",        color_map_json)
    .replace("__CENTER_LAT__",       f"{center_lat:.8f}")
    .replace("__CENTER_LON__",       f"{center_lon:.8f}")
    .replace("__TOTAL_ALL_STORES__", str(int(total_stores_all)))
    .replace("__TOTAL_GEO_STORES__", str(int(total_stores_geo)))
)

with open(MAP_PATH, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"\n✅ Карта сохранена: {MAP_PATH}")
print(f"Размер файла: {MAP_PATH.stat().st_size / 1024 / 1024:.2f} MB")
print(f"\n✅ Готово!")