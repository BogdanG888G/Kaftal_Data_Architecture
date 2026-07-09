# generate_map.py - запускается один раз для генерации карты

import re
import time
import json
import math
from pathlib import Path

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

YANDEX_KEY = "18ffa901-3ca3-4490-9222-ed66046d64d7"

# Пути на сервере
DATA_DIR = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

MAP_PATH = DATA_DIR / "x5_stores_map_final.html"
GEO_CACHE_PATH = DATA_DIR / "dim_stores_geo.csv"

print(f"DATA_DIR: {DATA_DIR}")

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
# ЗАГРУЗКА ДАННЫХ
# ============================================================
SQL_FACT = """
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

    sum(ifNull(sales_quantity, 0)) AS sales_quantity,
    sum(ifNull(sales_amount_rub, 0)) AS sales_amount_rub,
    sum(ifNull(sales_cost_price, 0)) AS cost_amount_rub

FROM sales_mart
WHERE
    (
        positionCaseInsensitiveUTF8(retail_chain, 'перекресток') > 0
        OR positionCaseInsensitiveUTF8(retail_chain, 'x5 united') > 0
    )
    AND parseDateTimeBestEffortOrNull(date) IS NOT NULL
    AND chip_type = 'Картофельные чипсы'
    AND address is not NULL
    AND year = 2026

GROUP BY
    period_month,
    retail_chain, region_name, city_name, address,
    store_code, store_name, store_format, brand

ORDER BY period_month
"""

SQL_STORES = """
SELECT DISTINCT
    retail_chain,
    region_name,
    city_name,
    address,
    store_code,
    store_name,
    store_format
FROM sales_mart
WHERE
    positionCaseInsensitiveUTF8(retail_chain, 'перекресток') > 0
    OR positionCaseInsensitiveUTF8(retail_chain, 'x5 united') > 0
    AND year = 2026
    AND address is not NULL
"""

print("Загружаем fact (только чипсы)...")
fact = client.query_df(SQL_FACT)
print(f"fact: {len(fact):,} строк")

print("Загружаем stores (все магазины сети)...")
stores = client.query_df(SQL_STORES)
print(f"stores: {len(stores):,} строк")

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
    if "джем" in x_n:
        return "Перекресток-Джем"
    if "перекресток" in x_n:
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
    params = {"apikey": api_key, "geocode": query,
              "format": "json", "lang": "ru_RU", "results": 1}
    r = requests.get(url, params=params, timeout=20)
    r.raise_for_status()
    data = r.json()
    members = (data.get("response", {})
               .get("GeoObjectCollection", {})
               .get("featureMember", []))
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

print(f"fact:   {len(fact):,} строк | {fact['store_key'].nunique():,} уникальных магазинов")
print(f"stores: {len(stores):,} строк | {stores['store_key'].nunique():,} уникальных магазинов")
print(f"Пересечение: {len(fact_keys & stores_keys):,}")
print(f"Только в fact: {len(fact_keys - stores_keys):,}")

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
# ГЕОКОДИНГ (с кэшем)
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

print(f"Всего адресов: {len(address_ref):,}")
print(f"Уже в кэше: {len(address_ref) - len(need_geo):,}")
print(f"Нужно геокодить: {len(need_geo):,}")

if len(need_geo) > 0:
    ok, fail = 0, 0
    for idx, row in tqdm(need_geo.iterrows(), total=len(need_geo), desc="Geocoding"):
        try:
            lat, lon, prec, found = geocode_yandex(row["full_address"], YANDEX_KEY)
            address_ref.at[idx, "lat"] = lat
            address_ref.at[idx, "lon"] = lon
            address_ref.at[idx, "geo_precision"] = prec
            address_ref.at[idx, "geo_found_address"] = found
            ok += int(lat is not None)
            fail += int(lat is None)
        except Exception as e:
            print(f"Ошибка: {row['full_address'][:60]} → {e}")
            for col in ["lat", "lon", "geo_precision", "geo_found_address"]:
                address_ref.at[idx, col] = None
            fail += 1
        time.sleep(0.25)

    print(f"Успешно: {ok} | Ошибок: {fail}")

    updated = pd.concat([
        geo_cache,
        address_ref[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]]
    ]).drop_duplicates("full_address", keep="last")
    updated.to_csv(GEO_CACHE_PATH, index=False, encoding="utf-8-sig")
    print(f"Кэш обновлён: {GEO_CACHE_PATH}")

store_ref = store_ref.merge(
    address_ref[["full_address", "lat", "lon", "geo_precision", "geo_found_address"]],
    on="full_address", how="left"
)

print(f"\nС координатами: {store_ref['lat'].notna().sum():,}")
print(f"Без координат: {store_ref['lat'].isna().sum():,}")

# ============================================================
# АГРЕГАЦИЯ
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

# ============================================================
# ВСЕ БРЕНДЫ (для кнопки "Показать все")
# ============================================================
all_brands_dict = {}
for sk, grp in store_brand_rev.groupby("store_key"):
    all_brands_dict[sk] = [
        {"brand": str(r["brand"]), "brand_rev": float(r["brand_rev"])}
        for _, r in grp.iterrows()
    ]

# Топ-5 для быстрого отображения
top5_dict = {
    sk: brands[:5]
    for sk, brands in all_brands_dict.items()
}

# динамика
store_monthly = (
    fact
    .groupby(["store_key", "period_str"], as_index=False)
    .agg(month_rev=("sales_amount_rub", "sum"),
         month_qty=("sales_quantity", "sum"))
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

# merge всего
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

print(f"store_sales строк: {len(store_sales)}")
print(f"geo строк: {len(geo)}")
print(f"Всего магазинов: {total_stores_all}")
print(f"На карте: {total_stores_geo}")
print(f"Без координат: {total_stores_all - total_stores_geo}")
print(f"Выручка: {total_rev_all:,.0f} ₽")
print(f"Продажи: {total_qty_all:,.0f} шт")

# ============================================================
# ГЕНЕРАЦИЯ HTML
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
    "Перекресток": "#E63946",
    "Перекресток-Джем": "#F4A261",
    "X5 United": "#457B9D",
}

if len(geo) == 0:
    print("ВНИМАНИЕ: Нет данных для отображения на карте!")
    exit()

center_lat = float(geo["lat"].mean())
center_lon = float(geo["lon"].mean())
max_rev = max(float(geo["revenue_rub"].max()), 1.0)

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
        "all_brands": all_brands_dict.get(sk, []),  # <-- ВСЕ бренды
        "dyn": dyn_dict.get(sk, {"periods": [], "rev": [], "qty": []}),
        "radius": round(r, 2),
    })

stores_json = json.dumps(stores_data, ensure_ascii=False)
color_map_json = json.dumps(color_map, ensure_ascii=False)

print(f"Точек на карте: {len(stores_data)}")
print(f"Размер JSON: {len(stores_json)/1024:.1f} KB")

# ============================================================
# HTML ТЕМПЛЕЙТ (полная версия с кнопкой "Показать все")
# ============================================================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
    <title>Магазины X5 / Перекресток - Картофельные чипсы</title>

    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>

    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        html, body, #map {
            height: 100%;
            width: 100%;
            font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Arial, sans-serif;
        }

        #map {
            background: #f0f4f9;
        }

        .top-panel {
            position: fixed;
            top: 16px;
            left: 50%;
            transform: translateX(-50%);
            z-index: 10000;
            background: rgba(255, 255, 255, 0.92);
            backdrop-filter: blur(16px) saturate(180%);
            -webkit-backdrop-filter: blur(16px) saturate(180%);
            box-shadow: 0 8px 40px rgba(0, 0, 0, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.6);
            border-radius: 20px;
            padding: 14px 28px;
            display: flex;
            align-items: center;
            gap: 32px;
            min-width: 600px;
            max-width: 90vw;
            justify-content: center;
            flex-wrap: wrap;
        }

        .top-panel .brand-icon {
            font-size: 28px;
        }

        .top-panel .brand-title {
            font-size: 18px;
            font-weight: 800;
            color: #1a2634;
            letter-spacing: -0.3px;
        }

        .top-panel .brand-sub {
            font-size: 12px;
            font-weight: 400;
            color: #6b7a8d;
            margin-top: 2px;
        }

        .top-panel .stats {
            display: flex;
            gap: 20px;
            align-items: center;
        }

        .stat-item {
            display: flex;
            flex-direction: column;
            align-items: center;
        }

        .stat-value {
            font-size: 16px;
            font-weight: 700;
            color: #1a2634;
        }

        .stat-label {
            font-size: 10px;
            font-weight: 600;
            color: #8a99aa;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-top: 2px;
        }

        .stat-divider {
            width: 1px;
            height: 32px;
            background: #e4e9f0;
        }

        #filter-toggle {
            position: fixed;
            top: 20px;
            right: 24px;
            z-index: 10001;
            border: none;
            border-radius: 14px;
            padding: 10px 18px;
            background: rgba(255, 255, 255, 0.92);
            backdrop-filter: blur(12px);
            color: #1a2634;
            font-weight: 600;
            font-size: 13px;
            cursor: pointer;
            box-shadow: 0 4px 20px rgba(0, 0, 0, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.6);
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }

        #filter-toggle:hover {
            background: rgba(255, 255, 255, 0.98);
            box-shadow: 0 8px 30px rgba(0, 0, 0, 0.12);
            transform: translateY(-1px);
        }

        #filters {
            position: fixed;
            top: 76px;
            right: 24px;
            width: 320px;
            z-index: 10001;
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(20px);
            border-radius: 20px;
            padding: 20px 22px;
            box-shadow: 0 12px 48px rgba(0, 0, 0, 0.12);
            border: 1px solid rgba(255, 255, 255, 0.6);
            transition: transform 0.3s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.3s ease;
            max-height: calc(100vh - 100px);
            overflow-y: auto;
        }

        #filters.collapsed {
            transform: translateX(360px);
            opacity: 0;
            pointer-events: none;
        }

        #filters h3 {
            margin: 0 0 16px 0;
            color: #1a2634;
            font-size: 17px;
            font-weight: 700;
        }

        .filter-group {
            margin-bottom: 14px;
        }

        .filter-group label {
            display: block;
            font-size: 11px;
            font-weight: 600;
            color: #6b7a8d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 6px;
        }

        .filter-group select,
        .filter-group input {
            width: 100%;
            border: 1.5px solid #e4e9f0;
            border-radius: 12px;
            padding: 10px 14px;
            font-size: 13px;
            outline: none;
            background: #ffffff;
            color: #1a2634;
            transition: border-color 0.2s ease, box-shadow 0.2s ease;
            font-family: inherit;
        }

        .filter-group select:focus,
        .filter-group input:focus {
            border-color: #4f7cff;
            box-shadow: 0 0 0 4px rgba(79, 124, 255, 0.1);
        }

        .btn-row {
            display: flex;
            gap: 10px;
            margin-top: 16px;
        }

        .btn {
            flex: 1;
            border: none;
            border-radius: 12px;
            padding: 11px 16px;
            font-weight: 600;
            font-size: 13px;
            cursor: pointer;
            transition: all 0.2s ease;
            font-family: inherit;
        }

        .btn-apply {
            background: #4f7cff;
            color: #fff;
        }

        .btn-apply:hover {
            background: #3d6ae6;
            transform: translateY(-1px);
            box-shadow: 0 4px 16px rgba(79, 124, 255, 0.3);
        }

        .btn-reset {
            background: #f0f4f9;
            color: #324155;
        }

        .btn-reset:hover {
            background: #e4e9f0;
        }

        .filters-note {
            margin-top: 16px;
            padding-top: 14px;
            border-top: 1px solid #eef2f7;
            font-size: 12px;
            color: #6b7a8d;
            line-height: 1.6;
        }

        #legend {
            position: fixed;
            left: 20px;
            bottom: 24px;
            z-index: 10000;
            background: rgba(255, 255, 255, 0.92);
            backdrop-filter: blur(12px);
            border-radius: 16px;
            padding: 16px 20px;
            box-shadow: 0 4px 24px rgba(0, 0, 0, 0.08);
            border: 1px solid rgba(255, 255, 255, 0.6);
            min-width: 160px;
        }

        .legend-title {
            font-weight: 700;
            font-size: 13px;
            color: #1a2634;
            margin-bottom: 10px;
        }

        .legend-row {
            display: flex;
            align-items: center;
            gap: 10px;
            margin: 6px 0;
            font-size: 12px;
            color: #324155;
        }

        .legend-dot {
            width: 12px;
            height: 12px;
            border-radius: 50%;
            flex-shrink: 0;
            border: 1px solid rgba(0,0,0,0.06);
        }

        .legend-divider {
            border: none;
            border-top: 1px solid #eef2f7;
            margin: 10px 0;
        }

        .legend-hint {
            font-size: 11px;
            color: #8a99aa;
            line-height: 1.5;
        }

        .store-popup {
            width: 400px;
            font-family: inherit;
            color: #1a2634;
        }

        .popup-header {
            padding: 16px 20px;
            border-radius: 16px 16px 0 0;
        }

        .popup-header .badge {
            display: inline-block;
            background: rgba(255,255,255,0.2);
            padding: 2px 10px;
            border-radius: 20px;
            font-size: 10px;
            font-weight: 600;
            color: #fff;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 6px;
        }

        .popup-title {
            font-size: 17px;
            font-weight: 800;
            line-height: 1.3;
            color: #fff;
        }

        .popup-subtitle {
            margin-top: 4px;
            font-size: 12px;
            opacity: 0.9;
            color: #fff;
        }

        .popup-body {
            background: #fafcfe;
            border: 1px solid #eef2f7;
            border-top: none;
            border-radius: 0 0 16px 16px;
            padding: 16px 20px 20px;
        }

        .popup-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 4px 16px;
            font-size: 12px;
            color: #6b7a8d;
            margin-bottom: 14px;
            padding-bottom: 12px;
            border-bottom: 1px solid #eef2f7;
        }

        .kpi-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 8px;
            margin-bottom: 14px;
        }

        .mini-kpi {
            background: #fff;
            border: 1px solid #eef2f7;
            border-radius: 12px;
            padding: 10px 12px;
        }

        .mini-kpi .val {
            font-size: 15px;
            font-weight: 700;
            color: #1a2634;
        }

        .mini-kpi .lbl {
            margin-top: 3px;
            font-size: 10px;
            font-weight: 600;
            color: #8a99aa;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }

        .section-title {
            margin: 14px 0 8px;
            font-size: 11px;
            font-weight: 700;
            color: #6b7a8d;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .spark-box {
            background: #fff;
            border: 1px solid #eef2f7;
            border-radius: 12px;
            padding: 6px 8px;
            margin-bottom: 6px;
        }

        .brand-list {
            background: #fff;
            border: 1px solid #eef2f7;
            border-radius: 12px;
            padding: 10px 12px;
            max-height: 300px;
            overflow-y: auto;
        }

        .brand-list::-webkit-scrollbar {
            width: 4px;
        }

        .brand-list::-webkit-scrollbar-track {
            background: #f0f4f9;
            border-radius: 10px;
        }

        .brand-list::-webkit-scrollbar-thumb {
            background: #c0cbd8;
            border-radius: 10px;
        }

        .brand-list::-webkit-scrollbar-thumb:hover {
            background: #a0b0c0;
        }

        .brand-row {
            display: grid;
            grid-template-columns: 1fr auto;
            gap: 10px;
            align-items: center;
            margin-bottom: 8px;
        }

        .brand-row:last-child {
            margin-bottom: 0;
        }

        .brand-name {
            font-size: 12px;
            font-weight: 600;
            color: #1a2634;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .brand-bar-wrap {
            height: 6px;
            background: #eef2f7;
            border-radius: 999px;
            overflow: hidden;
            margin-top: 3px;
        }

        .brand-bar {
            height: 6px;
            border-radius: 999px;
            transition: width 0.6s ease;
        }

        .brand-val {
            font-size: 12px;
            font-weight: 600;
            color: #6b7a8d;
            white-space: nowrap;
        }

        .popup-footer {
            margin-top: 12px;
            padding-top: 10px;
            border-top: 1px solid #eef2f7;
            font-size: 11px;
            color: #8a99aa;
        }

        .show-more-btn {
            margin-top: 10px;
            padding: 8px 16px;
            background: #f0f4f9;
            border: 1px solid #e4e9f0;
            border-radius: 10px;
            font-size: 12px;
            font-weight: 600;
            color: #4f7cff;
            cursor: pointer;
            width: 100%;
            transition: all 0.2s ease;
            font-family: inherit;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 8px;
        }

        .show-more-btn:hover {
            background: #e4e9f0;
            transform: translateY(-1px);
        }

        .leaflet-popup-content-wrapper {
            border-radius: 16px !important;
            padding: 0 !important;
            overflow: hidden;
            box-shadow: 0 16px 48px rgba(0, 0, 0, 0.18) !important;
        }

        .leaflet-popup-content {
            margin: 0 !important;
            min-width: 380px;
        }

        .leaflet-popup-tip {
            box-shadow: none !important;
        }

        .leaflet-popup-close-button {
            top: 12px !important;
            right: 12px !important;
            color: rgba(255,255,255,0.7) !important;
            font-size: 20px !important;
            font-weight: 300 !important;
        }

        .leaflet-popup-close-button:hover {
            color: #fff !important;
        }

        @media (max-width: 768px) {
            .top-panel {
                top: 12px;
                padding: 10px 16px;
                min-width: auto;
                width: calc(100vw - 24px);
                gap: 12px;
                border-radius: 16px;
            }
            .top-panel .brand-title { font-size: 14px; }
            .top-panel .stats { gap: 12px; }
            .stat-value { font-size: 13px; }
            .stat-divider { display: none; }
            #filter-toggle {
                top: 14px;
                right: 14px;
                padding: 8px 14px;
                font-size: 12px;
            }
            #filters {
                right: 12px;
                width: calc(100vw - 24px);
                top: 68px;
                max-height: calc(100vh - 80px);
                padding: 16px;
            }
            #filters.collapsed {
                transform: translateX(calc(100vw - 12px));
            }
            #legend {
                left: 12px;
                bottom: 12px;
                padding: 12px 14px;
                min-width: 120px;
            }
            .store-popup { width: 300px; }
            .leaflet-popup-content { min-width: 280px; }
        }
    </style>
</head>
<body>
    <div id="map"></div>

    <div class="top-panel">
        <div>
            <span class="brand-icon">🥔</span>
            <span class="brand-title">X5 / Перекресток</span>
            <div class="brand-sub">Картофельные чипсы на карте</div>
        </div>

        <div class="stats">
            <div class="stat-item">
                <span class="stat-value" id="kpi-stores">0</span>
                <span class="stat-label">Магазинов</span>
            </div>
            <div class="stat-divider"></div>
            <div class="stat-item">
                <span class="stat-value" id="kpi-rev">0 ₽</span>
                <span class="stat-label">Выручка</span>
            </div>
            <div class="stat-divider"></div>
            <div class="stat-item">
                <span class="stat-value" id="kpi-qty">0 шт</span>
                <span class="stat-label">Продажи</span>
            </div>
        </div>
    </div>

    <button id="filter-toggle">⚙️ Фильтры</button>

    <div id="filters">
        <h3>🔍 Фильтры</h3>
        <div class="filter-group">
            <label>Сеть</label>
            <select id="f-chain"><option value="">Все сети</option></select>
        </div>
        <div class="filter-group">
            <label>Регион</label>
            <select id="f-region"><option value="">Все регионы</option></select>
        </div>
        <div class="filter-group">
            <label>Город</label>
            <select id="f-city"><option value="">Все города</option></select>
        </div>
        <div class="filter-group">
            <label>Поиск по названию</label>
            <input id="f-search" type="text" placeholder="Например: Перекресток 123"/>
        </div>
        <div class="btn-row">
            <button class="btn btn-apply" id="btn-apply">Применить</button>
            <button class="btn btn-reset" id="btn-reset">Сбросить</button>
        </div>
        <div class="filters-note" id="filters-note"></div>
    </div>

    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>

    <script>
        const STORES = __STORES_JSON__;
        const COLOR_MAP = __COLOR_MAP__;
        const DEFAULT_CENTER = [__CENTER_LAT__, __CENTER_LON__];
        const TOTAL_ALL_STORES = __TOTAL_ALL_STORES__;
        const TOTAL_GEO_STORES = __TOTAL_GEO_STORES__;
        const GEO_MISSING = __GEO_MISSING__;

        const map = L.map('map', { zoomControl: true, preferCanvas: true }).setView(DEFAULT_CENTER, 5);

        L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
            maxZoom: 19,
            attribution: '© OpenStreetMap © CARTO'
        }).addTo(map);

        const cluster = L.markerClusterGroup({
            chunkedLoading: true,
            spiderfyOnMaxZoom: true,
            showCoverageOnHover: false,
            maxClusterRadius: 60
        });

        map.addLayer(cluster);

        const chainSelect = document.getElementById('f-chain');
        const regionSelect = document.getElementById('f-region');
        const citySelect = document.getElementById('f-city');
        const searchInput = document.getElementById('f-search');
        const filtersNote = document.getElementById('filters-note');

        const uniq = arr => [...new Set(arr.filter(x => x && String(x).trim() !== ''))].sort((a, b) => String(a).localeCompare(String(b), 'ru'));

        const CHAINS = uniq(STORES.map(x => x.chain));
        const REGIONS = uniq(STORES.map(x => x.region));

        function fillSelect(selectEl, items, firstText) {
            selectEl.innerHTML = '';
            const first = document.createElement('option');
            first.value = '';
            first.textContent = firstText;
            selectEl.appendChild(first);
            items.forEach(item => {
                const opt = document.createElement('option');
                opt.value = item;
                opt.textContent = item;
                selectEl.appendChild(opt);
            });
        }

        fillSelect(chainSelect, CHAINS, 'Все сети');
        fillSelect(regionSelect, REGIONS, 'Все регионы');
        fillSelect(citySelect, uniq(STORES.map(x => x.city)), 'Все города');

        function updateCityOptions() {
            const chain = chainSelect.value;
            const region = regionSelect.value;
            let rows = STORES.slice();
            if (chain) rows = rows.filter(x => x.chain === chain);
            if (region) rows = rows.filter(x => x.region === region);
            const cities = uniq(rows.map(x => x.city));
            const currentCity = citySelect.value;
            fillSelect(citySelect, cities, 'Все города');
            if (cities.includes(currentCity)) citySelect.value = currentCity;
        }

        chainSelect.addEventListener('change', updateCityOptions);
        regionSelect.addEventListener('change', updateCityOptions);

        function esc(s) { return String(s ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;').replace(/'/g,'&#039;'); }

        function fmtNum(n) { if (n === null || n === undefined || Number.isNaN(Number(n))) return '—'; return Number(n).toLocaleString('ru-RU', {maximumFractionDigits: 0}); }
        function fmtMoney(n) { if (n === null || n === undefined || Number.isNaN(Number(n))) return '—'; return Number(n).toLocaleString('ru-RU', {maximumFractionDigits: 0}) + ' ₽'; }
        function fmtPrice(n) { if (n === null || n === undefined || Number.isNaN(Number(n))) return '—'; return Number(n).toLocaleString('ru-RU', {minimumFractionDigits: 2, maximumFractionDigits: 2}) + ' ₽'; }

        function sparkline(values, color) {
            if (!values || values.length < 2) return '<div style="padding:8px 4px;color:#b0c0d0;font-size:12px;">Нет данных</div>';
            const w = 340, h = 40;
            const maxV = Math.max(...values), minV = Math.min(...values), range = (maxV - minV) || 1;
            const step = w / (values.length - 1);
            const points = values.map((v, i) => { const x = i * step; const y = h - ((v - minV) / range) * (h - 6) - 3; return `${x},${y}`; }).join(' ');
            const area = `0,${h} ${points} ${w},${h}`;
            return `<svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}" style="display:block;width:100%;height:40px;">
                <polygon points="${area}" fill="${color}" opacity="0.12"></polygon>
                <polyline points="${points}" fill="none" stroke="${color}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"></polyline>
            </svg>`;
        }

        // ============================================================
        // ФУНКЦИЯ ДЛЯ ПОСТРОЕНИЯ СПИСКА БРЕНДОВ (с кнопкой "Развернуть")
        // ============================================================
        function buildBrandsHtml(top5, allBrands) {
            if (!allBrands || allBrands.length === 0) {
                return '<div style="font-size:12px;color:#b0c0d0;padding:8px 12px;">Нет данных по брендам</div>';
            }
            
            const palette = ['#E63946', '#457B9D', '#F4A261', '#2A9D8F', '#8E5CF6', 
                             '#E67E22', '#2ECC71', '#3498DB', '#9B59B6', '#1ABC9C',
                             '#F39C12', '#27AE60', '#2980B9', '#8E44AD', '#16A085'];
            
            const maxVal = Math.max(...allBrands.map(x => Number(x.brand_rev || 0)), 1);
            const listId = 'brand-list-' + Math.random().toString(36).substr(2, 9);
            
            // Топ-5 для отображения по умолчанию
            const top5List = allBrands.slice(0, 5);
            const remainingList = allBrands.slice(5);
            const hasMore = remainingList.length > 0;
            
            let html = `<div class="brand-list" id="${listId}">`;
            
            // Рендерим топ-5
            top5List.forEach((b, i) => {
                const width = Math.max(6, (Number(b.brand_rev || 0) / maxVal) * 100);
                html += `<div class="brand-row">
                    <div class="brand-left">
                        <div class="brand-name">${esc(b.brand)}</div>
                        <div class="brand-bar-wrap">
                            <div class="brand-bar" style="width:${width}%;background:${palette[i % palette.length]};"></div>
                        </div>
                    </div>
                    <div class="brand-val">${fmtMoney(b.brand_rev)}</div>
                </div>`;
            });
            
            if (hasMore) {
                // Скрытый блок с остальными брендами
                html += `<div id="${listId}-more" style="display:none;">`;
                
                remainingList.forEach((b, i) => {
                    const idx = i + 5;
                    const width = Math.max(6, (Number(b.brand_rev || 0) / maxVal) * 100);
                    html += `<div class="brand-row">
                        <div class="brand-left">
                            <div class="brand-name">${esc(b.brand)}</div>
                            <div class="brand-bar-wrap">
                                <div class="brand-bar" style="width:${width}%;background:${palette[idx % palette.length]};"></div>
                            </div>
                        </div>
                        <div class="brand-val">${fmtMoney(b.brand_rev)}</div>
                    </div>`;
                });
                
                html += `</div>`;
                
                // Кнопка "Показать все"
                html += `
                    <button class="show-more-btn" onclick="toggleBrands('${listId}')">
                        <span>📊</span> Показать все (${allBrands.length} брендов)
                    </button>
                `;
            }
            
            html += '</div>';
            return html;
        }

        // Функция для раскрытия/сворачивания списка
        function toggleBrands(listId) {
            const moreBlock = document.getElementById(listId + '-more');
            const container = document.getElementById(listId);
            const button = container ? container.querySelector('button') : null;
            
            if (!moreBlock || !button) return;
            
            if (moreBlock.style.display === 'none') {
                moreBlock.style.display = 'block';
                button.innerHTML = '<span>📊</span> Скрыть';
                button.style.background = '#e8edf5';
                // Прокручиваем к списку
                container.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
            } else {
                moreBlock.style.display = 'none';
                const count = allBrandsCount(container);
                button.innerHTML = `<span>📊</span> Показать все (${count} брендов)`;
                button.style.background = '#f0f4f9';
            }
        }

        function allBrandsCount(container) {
            // Пытаемся найти количество брендов в тексте кнопки
            const button = container.querySelector('button');
            if (button) {
                const match = button.textContent.match(/(\d+)\s*брендов/);
                if (match) return match[1];
            }
            return 'все';
        }

        // ============================================================
        // ПОПАП
        // ============================================================
        function makePopup(store) {
            const color = COLOR_MAP[store.chain] || '#7c8798';
            return `<div class="store-popup">
                <div class="popup-header" style="background:linear-gradient(135deg, ${color} 0%, #1a2634 100%);">
                    <div class="badge">${esc(store.chain || '—')}</div>
                    <div class="popup-title">🏪 ${esc(store.name)}</div>
                    <div class="popup-subtitle">${esc(store.format || '—')} · code: ${esc(store.store_code || '—')}</div>
                </div>
                <div class="popup-body">
                    <div class="popup-meta">
                        <span>📍 ${esc(store.address || '—')}</span>
                        <span>🏙️ ${esc(store.city || '—')}</span>
                        <span>🧭 ${esc(store.geo_precision || '—')}</span>
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
                    <div class="spark-box">${sparkline(store.dyn?.rev || [], '#E63946')}</div>
                    <div class="section-title">📦 Динамика продаж</div>
                    <div class="spark-box">${sparkline(store.dyn?.qty || [], '#457B9D')}</div>
                    <div class="section-title">🏷️ Все бренды</div>
                    ${buildBrandsHtml(store.top5 || [], store.all_brands || [])}
                    <div class="popup-footer">Брендов: <strong>${fmtNum(store.brands_count)}</strong> · Топ-бренд: <strong>${esc(store.top_brand || '—')}</strong></div>
                </div>
            </div>`;
        }

        function updateKpi(rows) {
            const rev = rows.reduce((acc, x) => acc + Number(x.rev || 0), 0);
            const qty = rows.reduce((acc, x) => acc + Number(x.qty || 0), 0);
            const stores = rows.length;
            document.getElementById('kpi-rev').textContent = fmtMoney(rev);
            document.getElementById('kpi-qty').textContent = fmtNum(qty) + ' шт';
            document.getElementById('kpi-stores').textContent = fmtNum(stores);
            filtersNote.innerHTML = `Всего: <strong>${fmtNum(TOTAL_ALL_STORES)}</strong> магазинов<br>На карте: <strong>${fmtNum(TOTAL_GEO_STORES)}</strong><br>Показано: <strong>${fmtNum(stores)}</strong>`;
        }

        function render(rows, fitBounds = false) {
            cluster.clearLayers();
            const bounds = [];
            rows.forEach(store => {
                const color = COLOR_MAP[store.chain] || '#7c8798';
                const marker = L.circleMarker([store.lat, store.lon], {
                    radius: store.radius,
                    color: color,
                    weight: 2,
                    fillColor: color,
                    fillOpacity: 0.7,
                    stroke: true,
                    opacity: 0.9
                });
                marker.bindTooltip(`<strong>${esc(store.name)}</strong><br>${fmtMoney(store.rev)}`, {direction: 'top', sticky: true});
                marker.bindPopup(makePopup(store), {maxWidth: 430, closeButton: true});
                cluster.addLayer(marker);
                bounds.push([store.lat, store.lon]);
            });
            updateKpi(rows);
            if (fitBounds && bounds.length > 0) map.fitBounds(bounds, {padding: [48, 48]});
        }

        function getFilteredRows() {
            const chain = chainSelect.value;
            const region = regionSelect.value;
            const city = citySelect.value;
            const search = (searchInput.value || '').trim().toLowerCase();
            return STORES.filter(store => {
                if (chain && store.chain !== chain) return false;
                if (region && store.region !== region) return false;
                if (city && store.city !== city) return false;
                if (search) {
                    const hay = [store.name, store.address, store.city, store.region, store.store_code].join(' ').toLowerCase();
                    if (!hay.includes(search)) return false;
                }
                return true;
            });
        }

        function applyFilters() { const rows = getFilteredRows(); render(rows, rows.length > 0); }
        function resetFilters() {
            chainSelect.value = '';
            regionSelect.value = '';
            fillSelect(citySelect, uniq(STORES.map(x => x.city)), 'Все города');
            citySelect.value = '';
            searchInput.value = '';
            render(STORES, false);
            map.setView(DEFAULT_CENTER, 5);
        }

        document.getElementById('btn-apply').addEventListener('click', applyFilters);
        document.getElementById('btn-reset').addEventListener('click', resetFilters);
        searchInput.addEventListener('keydown', function(e) { if (e.key === 'Enter') applyFilters(); });

        const filtersEl = document.getElementById('filters');
        const toggleBtn = document.getElementById('filter-toggle');
        let collapsed = false;
        toggleBtn.addEventListener('click', function() {
            collapsed = !collapsed;
            filtersEl.classList.toggle('collapsed', collapsed);
            toggleBtn.innerHTML = collapsed ? '⚙️' : '⚙️ Фильтры';
        });

        updateCityOptions();
        render(STORES, false);
    </script>
</body>
</html>
"""

# ============================================================
# СОХРАНЯЕМ КАРТУ
# ============================================================
html_content = (
    HTML_TEMPLATE
    .replace("__STORES_JSON__", stores_json)
    .replace("__COLOR_MAP__", color_map_json)
    .replace("__CENTER_LAT__", f"{center_lat:.8f}")
    .replace("__CENTER_LON__", f"{center_lon:.8f}")
    .replace("__TOTAL_ALL_STORES__", str(int(total_stores_all)))
    .replace("__TOTAL_GEO_STORES__", str(int(total_stores_geo)))
    .replace("__GEO_MISSING__", str(int(total_stores_all - total_stores_geo)))
)

with open(MAP_PATH, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"\n✅ Карта сохранена: {MAP_PATH}")
print(f"Размер файла: {MAP_PATH.stat().st_size / 1024 / 1024:.2f} MB")
print(f"✅ Данные готовы! Запускай сервер: python app.py")