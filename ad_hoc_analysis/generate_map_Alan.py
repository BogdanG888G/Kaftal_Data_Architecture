# extract_perekrestok_moscow_mkad.py

import json
import math
import pandas as pd
import folium
from pathlib import Path

DATA_DIR = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data")

# ============================================================
# МКАД — точные координаты (108 км-столбов)
# ============================================================
MKAD_RAW = [
    [1,37.842762,55.774558], [2,37.842789,55.76522], [3,37.842627,55.755723],
    [4,37.841828,55.747399], [5,37.841217,55.739103], [6,37.840175,55.730482],
    [7,37.83916,55.721939], [8,37.837121,55.712203], [9,37.83262,55.703048],
    [10,37.829512,55.694287], [11,37.831353,55.68529], [12,37.834605,55.675945],
    [13,37.837597,55.667752], [14,37.839348,55.658667], [15,37.833842,55.650053],
    [16,37.824787,55.643713], [17,37.814564,55.637347], [18,37.802473,55.62913],
    [19,37.794235,55.623758], [20,37.781928,55.617713], [21,37.771139,55.611755],
    [22,37.758725,55.604956], [23,37.747945,55.599677], [24,37.734785,55.594143],
    [25,37.723062,55.589234], [26,37.709425,55.583983], [27,37.696256,55.578834],
    [28,37.683167,55.574019], [29,37.668911,55.571999], [30,37.647765,55.573093],
    [31,37.633419,55.573928], [32,37.616719,55.574732], [33,37.60107,55.575816],
    [34,37.586536,55.5778], [35,37.571938,55.581271], [36,37.555732,55.585143],
    [37,37.545132,55.587509], [38,37.526366,55.5922], [39,37.516108,55.594728],
    [40,37.502274,55.60249], [41,37.49391,55.609685], [42,37.484846,55.617424],
    [43,37.474668,55.625801], [44,37.469925,55.630207], [45,37.456864,55.641041],
    [46,37.448195,55.648794], [47,37.441125,55.654675], [48,37.434424,55.660424],
    [49,37.42598,55.670701], [50,37.418712,55.67994], [51,37.414868,55.686873],
    [52,37.407528,55.695697], [53,37.397952,55.702805], [54,37.388969,55.709657],
    [55,37.383283,55.718273], [56,37.378369,55.728581], [57,37.374991,55.735201],
    [58,37.370248,55.744789], [59,37.369188,55.75435], [60,37.369053,55.762936],
    [61,37.369619,55.771444], [62,37.369853,55.779722], [63,37.372943,55.789542],
    [64,37.379824,55.79723], [65,37.386876,55.805796], [66,37.390397,55.814629],
    [67,37.393236,55.823606], [68,37.395275,55.83251], [69,37.394709,55.840376],
    [70,37.393056,55.850141], [71,37.397314,55.858801], [72,37.405588,55.867051],
    [73,37.416601,55.872703], [74,37.429429,55.877041], [75,37.443596,55.881091],
    [76,37.459065,55.882828], [77,37.473096,55.884625], [78,37.48861,55.888897],
    [79,37.5016,55.894232], [80,37.513206,55.899578], [81,37.527597,55.90526],
    [82,37.543443,55.907687], [83,37.559577,55.909388], [84,37.575531,55.910907],
    [85,37.590344,55.909257], [86,37.604637,55.905472], [87,37.619603,55.901637],
    [88,37.635961,55.898533], [89,37.647648,55.896973], [90,37.667878,55.895449],
    [91,37.681721,55.894868], [92,37.698807,55.893884], [93,37.712363,55.889094],
    [94,37.723636,55.883555], [95,37.735791,55.877501], [96,37.741261,55.874698],
    [97,37.764519,55.862464], [98,37.765992,55.861979], [99,37.788216,55.850257],
    [100,37.788522,55.850383], [101,37.800586,55.844167], [102,37.822819,55.832707],
    [103,37.829754,55.828789], [104,37.837148,55.821072], [105,37.838926,55.811599],
    [106,37.840004,55.802781], [107,37.840965,55.793991], [108,37.841576,55.785017],
]

MKAD_POLYGON = [(row[2], row[1]) for row in MKAD_RAW]
MKAD_KM = {row[0]: (row[2], row[1]) for row in MKAD_RAW}

def point_in_polygon(lat, lon, polygon):
    n = len(polygon)
    inside = False
    x, y = lon, lat
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i][1], polygon[i][0]
        xj, yj = polygon[j][1], polygon[j][0]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside

def is_inside_mkad(lat, lon):
    return point_in_polygon(float(lat), float(lon), MKAD_POLYGON)

def is_perekrestok(chain: str) -> bool:
    if not chain:
        return False
    c = chain.lower().strip()
    return ("перекресток" in c or "перекрёсток" in c or
            "джем" in c or "x5 united" in c)

def is_moscow(city: str) -> bool:
    if not city:
        return False
    return "москва" in city.lower().strip()

# ============================================================
# ЗАГРУЗКА JSON
# ============================================================
print("Загружаем данные...")
with open(DATA_DIR / "stores.json", encoding="utf-8") as f:
    stores = json.load(f)
with open(DATA_DIR / "details.json", encoding="utf-8") as f:
    details = json.load(f)

print(f"Всего магазинов в JSON: {len(stores)}")
print(f"Всего details: {len(details)}")

# ============================================================
# ФИЛЬТРАЦИЯ
# ============================================================
results = []
stats = {
    "total": 0, "moscow": 0, "perekrestok": 0,
    "with_coords": 0, "inside_mkad": 0
}

for store in stores:
    stats["total"] += 1
    store_id = store.get("id", "")
    detail   = details.get(store_id, {})
    chain    = store.get("c", "") or ""
    city     = detail.get("city", "") or ""
    lat      = store.get("lat")
    lon      = store.get("lon")

    # Фильтр: Москва
    if not is_moscow(city):
        continue
    stats["moscow"] += 1

    # Фильтр: Перекрёсток
    if not is_perekrestok(chain):
        continue
    stats["perekrestok"] += 1

    # Фильтр: есть координаты
    if lat is None or lon is None:
        continue
    stats["with_coords"] += 1

    # Фильтр: внутри МКАД
    if not is_inside_mkad(lat, lon):
        continue
    stats["inside_mkad"] += 1

    # ============================================================
    # СОБИРАЕМ ВСЕ ДАННЫЕ ИЗ DETAIL (как в основной карте)
    # ============================================================
    dyn = detail.get("dyn", {})
    all_brands = detail.get("all_brands", [])

    row = {
        "store_key":     store_id,
        "chain":         "Перекрёсток",
        "chain_original": chain,
        "store_name":    store.get("n", ""),
        "address":       detail.get("address", ""),
        "full_address":  detail.get("full_address", ""),
        "city":          detail.get("city", ""),
        "region":        detail.get("region", ""),
        "store_code":    detail.get("store_code", ""),
        "store_format":  detail.get("format", ""),
        "lat":           float(lat),
        "lon":           float(lon),
        "geo_precision": detail.get("geo_precision", ""),
        # Метрики из stores.json
        "revenue_rub":   store.get("rev", 0),
        "sales_qty":     store.get("qty", 0),
        # Метрики из details.json
        "avg_price":     detail.get("avg_price"),
        "avg_cost":      detail.get("avg_cost"),
        "avg_month_rev": detail.get("avg_month_rev"),
        "periods_count": detail.get("periods_count"),
        "brands_count":  detail.get("brands_count"),
        "top_brand":     detail.get("top_brand", ""),
        # Динамика и бренды — сохраняем целиком
        "dyn":           dyn,
        "all_brands":    all_brands,
    }

    # Добавляем колонки по месяцам для Excel
    periods = dyn.get("periods", [])
    rev_by_month = dyn.get("rev", [])
    qty_by_month = dyn.get("qty", [])
    for p, rv, qt in zip(periods, rev_by_month, qty_by_month):
        row[f"rev_{p}"] = rv
        row[f"qty_{p}"] = qt

    results.append(row)

print(f"\n📊 Статистика фильтрации:")
print(f"  Всего в JSON:              {stats['total']:>6}")
print(f"  Москва:                    {stats['moscow']:>6}")
print(f"  Из них Перекрёсток:        {stats['perekrestok']:>6}")
print(f"  С координатами:            {stats['with_coords']:>6}")
print(f"  ✅ Внутри МКАД:           {stats['inside_mkad']:>6}")

if not results:
    print("\n⚠️ Ничего не найдено!")
    exit()
# ============================================================
# EXCEL — только нужные колонки, один лист
# ============================================================
df_excel = pd.DataFrame(results).sort_values("revenue_rub", ascending=False)

df_excel = df_excel[["store_name", "store_format", "address"]].copy()

df_excel = df_excel.rename(columns={
    "store_name":   "Название магазина",
    "store_format": "Формат магазина",
    "address":      "Адрес ТТ",
})

out_xlsx = DATA_DIR / "perekrestok_moscow_mkad.xlsx"

with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
    df_excel.to_excel(writer, index=False, sheet_name="Магазины")

print(f"✅ Excel: {out_xlsx}  ({len(df_excel)} магазинов)")

# ============================================================
# КАРТА FOLIUM
# ============================================================
print("\nСоздаём карту...")

PEREKRESTOK_COLOR = "#E63946"

m = folium.Map(
    location=[55.751244, 37.618423],
    zoom_start=11,
    tiles="CartoDB positron",
    prefer_canvas=True,
)

# Контур МКАД
folium.Polygon(
    locations=MKAD_POLYGON,
    color="#2563EB", weight=3,
    fill=True, fill_color="#2563EB", fill_opacity=0.03,
    tooltip="МКАД", dash_array="6 4",
).add_to(m)

# Подписи км
km_labels = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
for km in km_labels:
    if km in MKAD_KM:
        lat, lon = MKAD_KM[km]
        folium.Marker(
            [lat, lon],
            icon=folium.DivIcon(
                html=f'<div style="font-size:9px;font-weight:700;color:#2563EB;'
                     f'background:rgba(255,255,255,0.9);padding:1px 5px;'
                     f'border-radius:6px;border:1px solid #2563EB;">{km} км</div>',
                icon_size=(40, 16), icon_anchor=(20, 8),
            )
        ).add_to(m)

# ============================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ POPUP
# ============================================================
def fmt_num(n):
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    return f"{int(n):,}".replace(",", " ")

def fmt_money(n):
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    return f"{int(n):,} ₽".replace(",", " ")

def fmt_price(n):
    if n is None or (isinstance(n, float) and math.isnan(n)):
        return "—"
    return f"{n:,.2f} ₽".replace(",", " ")

def build_sparkline(values, color):
    """SVG sparkline как в основной карте"""
    if not values or len(values) < 2:
        return '<div style="padding:8px 4px;color:#b0c0d0;font-size:12px;">Нет данных</div>'
    w, h = 300, 40
    max_v = max(values) or 1
    min_v = min(values)
    rng = (max_v - min_v) or 1
    step = w / (len(values) - 1)
    pts = " ".join(
        f"{i * step},{h - ((v - min_v) / rng) * (h - 6) - 3}"
        for i, v in enumerate(values)
    )
    area = f"0,{h} {pts} {w},{h}"
    return f'''<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" style="display:block;width:100%;height:40px;">
        <polygon points="{area}" fill="{color}" opacity="0.12"/>
        <polyline points="{pts}" fill="none" stroke="{color}" stroke-width="2.5" 
                  stroke-linecap="round" stroke-linejoin="round"/>
    </svg>'''

def build_brands_html(all_brands):
    """Список брендов как в основной карте"""
    if not all_brands:
        return '<div style="font-size:12px;color:#b0c0d0;padding:8px;">Нет данных</div>'
    palette = ['#E63946','#457B9D','#F4A261','#2A9D8F','#8E5CF6','#E67E22','#2ECC71','#3498DB','#9B59B6','#1ABC9C']
    max_val = max((b.get("brand_rev", 0) for b in all_brands), default=1)
    html = '<div style="background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;max-height:200px;overflow-y:auto;">'
    for i, b in enumerate(all_brands[:10]):  # Топ-10
        name = b.get("brand", "—")
        rev = b.get("brand_rev", 0)
        w = max(6, (rev / max_val) * 100) if max_val > 0 else 6
        color = palette[i % len(palette)]
        html += f'''
        <div style="display:grid;grid-template-columns:1fr auto;gap:10px;align-items:center;margin-bottom:8px;">
            <div>
                <div style="font-size:12px;font-weight:600;color:#1a2634;">{name}</div>
                <div style="height:6px;background:#eef2f7;border-radius:999px;overflow:hidden;margin-top:3px;">
                    <div style="height:6px;border-radius:999px;background:{color};width:{w}%;"></div>
                </div>
            </div>
            <div style="font-size:12px;font-weight:600;color:#6b7a8d;white-space:nowrap;">{fmt_money(rev)}</div>
        </div>'''
    if len(all_brands) > 10:
        html += f'<div style="font-size:11px;color:#8a99aa;text-align:center;margin-top:8px;">... и ещё {len(all_brands) - 10} брендов</div>'
    html += '</div>'
    return html

# ============================================================
# МАГАЗИНЫ НА КАРТЕ
# ============================================================
max_rev = max((r["revenue_rub"] for r in results), default=1.0)

for r in results:
    rev    = r["revenue_rub"]
    qty    = r["sales_qty"]
    radius = max(5, min(20, 5 + 15 * math.sqrt(rev / max(max_rev, 1))))

    dyn = r.get("dyn", {})
    all_brands = r.get("all_brands", [])

    # Динамика по месяцам — таблица
    dyn_table = ""
    periods = dyn.get("periods", [])
    revs = dyn.get("rev", [])
    qtys = dyn.get("qty", [])
    if periods:
        dyn_table = '<table style="width:100%;border-collapse:collapse;font-size:12px;margin-top:8px;">'
        dyn_table += '<tr style="background:#f0f4f9;"><th style="padding:4px 8px;text-align:left;">Месяц</th><th style="padding:4px 8px;text-align:right;">Выручка</th><th style="padding:4px 8px;text-align:right;">Кол-во</th></tr>'
        for p, rv, qt in zip(periods, revs, qtys):
            dyn_table += f'<tr><td style="padding:2px 8px;color:#6b7a8d;">{p}</td><td style="padding:2px 8px;text-align:right;font-weight:600;">{fmt_money(rv)}</td><td style="padding:2px 8px;text-align:right;">{fmt_num(qt)} шт</td></tr>'
        dyn_table += '</table>'

    popup_html = f"""
    <div style='font-family:Inter,Arial,sans-serif;width:400px;'>
      <div style='background:linear-gradient(135deg,{PEREKRESTOK_COLOR},#1a2634);
                  padding:16px 20px;border-radius:16px 16px 0 0;'>
        <div style='background:rgba(255,255,255,0.2);display:inline-block;
                    padding:2px 10px;border-radius:20px;font-size:10px;
                    font-weight:600;color:#fff;text-transform:uppercase;
                    letter-spacing:0.5px;margin-bottom:6px;'>Перекрёсток</div>
        <div style='color:#fff;font-size:17px;font-weight:800;line-height:1.3;'>
            🏪 {r['store_name'] or '—'}</div>
        <div style='color:rgba(255,255,255,0.9);font-size:12px;margin-top:4px;'>
            {r['store_format'] or '—'} · code: {r['store_code'] or '—'}</div>
      </div>
      <div style='background:#fafcfe;border:1px solid #eef2f7;border-top:none;
                  border-radius:0 0 16px 16px;padding:16px 20px;'>
        <div style='display:flex;flex-wrap:wrap;gap:4px 16px;font-size:12px;
                    color:#6b7a8d;margin-bottom:14px;padding-bottom:12px;
                    border-bottom:1px solid #eef2f7;'>
            <span>📍 {r['address'] or '—'}</span>
            <span>🏙️ {r['city'] or '—'}</span>
            <span>🧭 {r['geo_precision'] or '—'}</span>
        </div>

        <div style='display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:14px;'>
          <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;'>
            <div style='font-size:15px;font-weight:700;color:#E63946;'>{fmt_money(rev)}</div>
            <div style='margin-top:3px;font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;'>Выручка</div>
          </div>
          <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;'>
            <div style='font-size:15px;font-weight:700;color:#457B9D;'>{fmt_num(qty)} шт</div>
            <div style='margin-top:3px;font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;'>Продажи</div>
          </div>
          <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;'>
            <div style='font-size:15px;font-weight:700;color:#1a2634;'>{fmt_price(r['avg_price'])}</div>
            <div style='margin-top:3px;font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;'>Средняя цена</div>
          </div>
          <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;'>
            <div style='font-size:15px;font-weight:700;color:#1a2634;'>{fmt_price(r['avg_cost'])}</div>
            <div style='margin-top:3px;font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;'>Себестоимость</div>
          </div>
          <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;'>
            <div style='font-size:15px;font-weight:700;color:#1a2634;'>{fmt_num(r['periods_count'])}</div>
            <div style='margin-top:3px;font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;'>Периодов</div>
          </div>
          <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;'>
            <div style='font-size:15px;font-weight:700;color:#1a2634;'>{fmt_money(r['avg_month_rev'])}</div>
            <div style='margin-top:3px;font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;'>Ср. выручка/мес</div>
          </div>
        </div>

        <div style='margin:14px 0 8px;font-size:11px;font-weight:700;color:#6b7a8d;
                    text-transform:uppercase;letter-spacing:0.5px;'>📈 Динамика выручки</div>
        <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:6px 8px;margin-bottom:6px;'>
            {build_sparkline(revs, PEREKRESTOK_COLOR)}
        </div>

        <div style='margin:14px 0 8px;font-size:11px;font-weight:700;color:#6b7a8d;
                    text-transform:uppercase;letter-spacing:0.5px;'>📦 Динамика продаж</div>
        <div style='background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:6px 8px;margin-bottom:6px;'>
            {build_sparkline(qtys, '#457B9D')}
        </div>

        <div style='margin:14px 0 8px;font-size:11px;font-weight:700;color:#6b7a8d;
                    text-transform:uppercase;letter-spacing:0.5px;'>🏷️ Бренды</div>
        {build_brands_html(all_brands)}

        <div style='margin-top:12px;padding-top:10px;border-top:1px solid #eef2f7;
                    font-size:11px;color:#8a99aa;'>
            Брендов: <strong>{r['brands_count'] or 0}</strong> · Топ: <strong>{r['top_brand'] or '—'}</strong>
        </div>

        {dyn_table}
      </div>
    </div>"""

    folium.CircleMarker(
        location=[r["lat"], r["lon"]],
        radius=radius,
        color=PEREKRESTOK_COLOR, weight=2,
        fill=True, fill_color=PEREKRESTOK_COLOR, fill_opacity=0.75,
        tooltip=folium.Tooltip(
            f"<b>{r['store_name'] or r['address']}</b><br>"
            f"Перекрёсток · {r['store_format'] or '—'}<br>"
            f"Выручка: {fmt_money(rev)} | {fmt_num(qty)} шт",
            sticky=True,
        ),
        popup=folium.Popup(popup_html, max_width=430),
    ).add_to(m)

# ============================================================
# ЛЕГЕНДА И СЧЁТЧИК
# ============================================================
total_rev = sum(r['revenue_rub'] for r in results)
total_qty = sum(r['sales_qty'] for r in results)

legend_html = f"""
<div style='position:fixed;bottom:30px;left:20px;z-index:9999;
            background:rgba(255,255,255,0.95);backdrop-filter:blur(12px);
            border-radius:16px;padding:16px 20px;
            box-shadow:0 8px 32px rgba(0,0,0,0.12);min-width:220px;
            font-family:Inter,Arial,sans-serif;'>
  <div style='font-weight:700;font-size:14px;color:#1a2634;margin-bottom:12px;'>
    🛒 Перекрёсток — Москва
  </div>
  <div style='display:flex;align-items:center;gap:8px;margin:6px 0;font-size:12px;color:#324155;'>
    <span style='width:14px;height:14px;border-radius:50%;background:#E63946;'></span>
    Магазины ({len(results)} шт)
  </div>
  <hr style='border:none;border-top:1px solid #eef2f7;margin:10px 0;'/>
  <div style='display:flex;align-items:center;gap:8px;margin:6px 0;font-size:12px;color:#2563EB;'>
    <span style='width:14px;height:3px;background:#2563EB;border-radius:2px;'></span>
    МКАД (108 км)
  </div>
  <div style='font-size:11px;color:#8a99aa;margin-top:8px;'>Размер = выручка</div>
</div>"""
m.get_root().html.add_child(folium.Element(legend_html))

counter_html = f"""
<div style='position:fixed;top:20px;right:20px;z-index:9999;
            background:rgba(255,255,255,0.95);backdrop-filter:blur(12px);
            border-radius:16px;padding:14px 20px;
            box-shadow:0 8px 32px rgba(0,0,0,0.12);
            font-family:Inter,Arial,sans-serif;'>
  <div style='font-size:11px;font-weight:600;color:#6b7a8d;
              text-transform:uppercase;margin-bottom:8px;'>
    Перекрёсток · внутри МКАД
  </div>
  <div style='display:flex;gap:20px;'>
    <div style='text-align:center;'>
      <div style='font-size:22px;font-weight:800;color:#E63946;'>{len(results)}</div>
      <div style='font-size:10px;color:#8a99aa;text-transform:uppercase;'>Магазинов</div>
    </div>
    <div style='width:1px;background:#eef2f7;'></div>
    <div style='text-align:center;'>
      <div style='font-size:22px;font-weight:800;color:#1a2634;'>{total_rev/1e6:.1f}М</div>
      <div style='font-size:10px;color:#8a99aa;text-transform:uppercase;'>Выручка ₽</div>
    </div>
    <div style='width:1px;background:#eef2f7;'></div>
    <div style='text-align:center;'>
      <div style='font-size:22px;font-weight:800;color:#457B9D;'>{int(total_qty/1000)}К</div>
      <div style='font-size:10px;color:#8a99aa;text-transform:uppercase;'>Продажи шт</div>
    </div>
  </div>
</div>"""
m.get_root().html.add_child(folium.Element(counter_html))

out_map = DATA_DIR / "perekrestok_moscow_mkad_map.html"
m.save(str(out_map))

print(f"\n✅ Карта: {out_map}")
print(f"🗺️  Магазинов: {len(results)}")
print(f"📊 Выручка: {total_rev:,.0f} ₽")
print(f"📦 Продажи: {total_qty:,.0f} шт")