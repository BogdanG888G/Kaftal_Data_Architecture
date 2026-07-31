# generate_perekrestok_addresses_map.py
# Генерирует HTML-карту с фильтрами (МКАД/За МКАД/МКАД+Ростов)
# Множественный выбор через чипсы (кнопки)
# Фильтр по регионам (колонка "Регион", а не "Регион по АМ")

import pandas as pd
import json
from pathlib import Path

# ============ НАСТРОЙКИ ============
INPUT_FILE = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data/perekrestok_tt.xlsx")
DATA_DIR = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data")
MAP_FILE = DATA_DIR / "perekrestok_addresses_map.html"
# ===================================

# ============================================================
# МКАД
# ============================================================
MKAD_RAW = [
    [1,37.842762,55.774558],[2,37.842789,55.76522],[3,37.842627,55.755723],
    [4,37.841828,55.747399],[5,37.841217,55.739103],[6,37.840175,55.730482],
    [7,37.83916,55.721939],[8,37.837121,55.712203],[9,37.83262,55.703048],
    [10,37.829512,55.694287],[11,37.831353,55.68529],[12,37.834605,55.675945],
    [13,37.837597,55.667752],[14,37.839348,55.658667],[15,37.833842,55.650053],
    [16,37.824787,55.643713],[17,37.814564,55.637347],[18,37.802473,55.62913],
    [19,37.794235,55.623758],[20,37.781928,55.617713],[21,37.771139,55.611755],
    [22,37.758725,55.604956],[23,37.747945,55.599677],[24,37.734785,55.594143],
    [25,37.723062,55.589234],[26,37.709425,55.583983],[27,37.696256,55.578834],
    [28,37.683167,55.574019],[29,37.668911,55.571999],[30,37.647765,55.573093],
    [31,37.633419,55.573928],[32,37.616719,55.574732],[33,37.60107,55.575816],
    [34,37.586536,55.5778],[35,37.571938,55.581271],[36,37.555732,55.585143],
    [37,37.545132,55.587509],[38,37.526366,55.5922],[39,37.516108,55.594728],
    [40,37.502274,55.60249],[41,37.49391,55.609685],[42,37.484846,55.617424],
    [43,37.474668,55.625801],[44,37.469925,55.630207],[45,37.456864,55.641041],
    [46,37.448195,55.648794],[47,37.441125,55.654675],[48,37.434424,55.660424],
    [49,37.42598,55.670701],[50,37.418712,55.67994],[51,37.414868,55.686873],
    [52,37.407528,55.695697],[53,37.397952,55.702805],[54,37.388969,55.709657],
    [55,37.383283,55.718273],[56,37.378369,55.728581],[57,37.374991,55.735201],
    [58,37.370248,55.744789],[59,37.369188,55.75435],[60,37.369053,55.762936],
    [61,37.369619,55.771444],[62,37.369853,55.779722],[63,37.372943,55.789542],
    [64,37.379824,55.79723],[65,37.386876,55.805796],[66,37.390397,55.814629],
    [67,37.393236,55.823606],[68,37.395275,55.83251],[69,37.394709,55.840376],
    [70,37.393056,55.850141],[71,37.397314,55.858801],[72,37.405588,55.867051],
    [73,37.416601,55.872703],[74,37.429429,55.877041],[75,37.443596,55.881091],
    [76,37.459065,55.882828],[77,37.473096,55.884625],[78,37.48861,55.888897],
    [79,37.5016,55.894232],[80,37.513206,55.899578],[81,37.527597,55.90526],
    [82,37.543443,55.907687],[83,37.559577,55.909388],[84,37.575531,55.910907],
    [85,37.590344,55.909257],[86,37.604637,55.905472],[87,37.619603,55.901637],
    [88,37.635961,55.898533],[89,37.647648,55.896973],[90,37.667878,55.895449],
    [91,37.681721,55.894868],[92,37.698807,55.893884],[93,37.712363,55.889094],
    [94,37.723636,55.883555],[95,37.735791,55.877501],[96,37.741261,55.874698],
    [97,37.764519,55.862464],[98,37.765992,55.861979],[99,37.788216,55.850257],
    [100,37.788522,55.850383],[101,37.800586,55.844167],[102,37.822819,55.832707],
    [103,37.829754,55.828789],[104,37.837148,55.821072],[105,37.838926,55.811599],
    [106,37.840004,55.802781],[107,37.840965,55.793991],[108,37.841576,55.785017],
]
MKAD_POLYGON = [(r[2], r[1]) for r in MKAD_RAW]
MKAD_KM = {r[0]: (r[2], r[1]) for r in MKAD_RAW}

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

# ============================================================
# ЧТЕНИЕ
# ============================================================
print("📂 Читаем файл...")
df = pd.read_excel(INPUT_FILE)
df.columns = df.columns.str.strip()
print(f"   Строк: {len(df)}, Колонки: {df.columns.tolist()}")

# Ищем колонку координат
coord_col = None
for c in df.columns:
    if 'координат' in c.lower():
        coord_col = c
        break
if not coord_col:
    print("❌ Колонка «Координаты» не найдена!")
    exit()

df["_cs"] = df[coord_col].astype(str).str.strip()
# Заменяем все виды пробелов на обычный, убираем лишнее
df["_cs"] = df["_cs"].str.replace(r'\s+', ' ', regex=True)
# Разделяем по запятой (или точке с запятой, если есть)
df[["lat","lon"]] = df["_cs"].str.split(",", n=1, expand=True)
# Дополнительная очистка от нечисловых символов (кроме точки и минуса)
df["lat"] = df["lat"].str.replace(r'[^0-9.\-]', '', regex=True)
df["lon"] = df["lon"].str.replace(r'[^0-9.\-]', '', regex=True)
df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

# Сохраняем индексы до удаления
df_before = df.copy()
df_after = df.dropna(subset=["lat","lon"])
dropped = df_before[~df_before.index.isin(df_after.index)]
if len(dropped) > 0:
    print("⚠️ Пропущены строки с некорректными координатами:")
    print(dropped[["Наименование", "Координаты"]].to_string())

df = df.dropna(subset=["lat","lon"]).copy()
print(f"   С координатами: {len(df)}")

# Колонки – явно ищем точные названия, чтобы не перепутать
def get_col(exact_name):
    for col in df.columns:
        if col.strip() == exact_name:
            return col
    return None

def fc(candidates):
    for c in candidates:
        for col in df.columns:
            if c.lower() in col.lower():
                return col
    return None

C = {
    "name":     get_col("Наименование") or fc(["Наименование","Название"]),
    "format":   get_col("Формат") or fc(["Формат"]),
    "address":  get_col("Адрес") or fc(["Адрес"]),
    "city":     get_col("Город") or fc(["Город"]),
    "street":   get_col("Улица") or fc(["Улица"]),
    "house":    get_col("Номер дома") or fc(["Номер дома"]),
    "status":   get_col("Статус завода") or fc(["Статус"]),  # точное имя
    "region":   get_col("Регион") or fc(["Регион"]),        # теперь "Регион", а не "Регион по АМ"
    "division": get_col("Дивизион") or fc(["Дивизион"]),
    "filial":   get_col("Наименование филиала") or fc(["филиал"]),
    "best":     get_col("Best") or fc(["Best"]),
    "rc":       get_col("РЦ") or fc(["РЦ"]),
    "ua":       get_col("УА") or fc(["УА"]),
}

# МКАД флаг
df["mkad"] = df.apply(lambda r: point_in_polygon(r["lat"], r["lon"], MKAD_POLYGON), axis=1)
cnt_in = int(df["mkad"].sum())
cnt_out = len(df) - cnt_in
print(f"   ✅ Внутри МКАД: {cnt_in} | ⬜ За МКАД: {cnt_out}")

# JSON
def s(row, key):
    col = C.get(key)
    if not col: return ""
    v = row.get(col, "")
    return "" if pd.isna(v) else str(v).strip()

stores = []
for _, r in df.iterrows():
    stores.append({
        "lat": float(r["lat"]), "lon": float(r["lon"]), "mkad": bool(r["mkad"]),
        "name": s(r,"name"), "format": s(r,"format"), "address": s(r,"address"),
        "city": s(r,"city"), "street": s(r,"street"), "house": s(r,"house"),
        "status": s(r,"status"), "region": s(r,"region"), "division": s(r,"division"),
        "filial": s(r,"filial"), "best": s(r,"best"), "rc": s(r,"rc"),
        "ua": s(r,"ua"),
    })

total = len(stores)
center_lat = sum(x["lat"] for x in stores) / total
center_lon = sum(x["lon"] for x in stores) / total
mkad_js = [[lat,lon] for lat,lon in MKAD_POLYGON]
km_js = [{"km":km,"lat":lat,"lon":lon} for km in [1,10,20,30,40,50,60,70,80,90,100] if km in MKAD_KM for lat,lon in [MKAD_KM[km]]]

FORMAT_COLORS = {"S1":"#E63946","G1":"#2A9D8F","S2":"#457B9D","G2":"#F4A261","M1":"#8E5CF6","M2":"#E67E22","H1":"#6366F1","H2":"#EC4899"}

# ============================================================
# HTML (без изменений, кроме имени фильтра "Регион")
# ============================================================
html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8"/><meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>Перекрёсток — адреса ТТ</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css"/>
<link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css"/>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
*{{margin:0;padding:0;box-sizing:border-box;}}
html,body,#map{{height:100%;width:100%;font-family:'Inter',system-ui,sans-serif;}}
#map{{background:#f0f4f9;}}
.top-panel{{position:fixed;top:16px;left:50%;transform:translateX(-50%);z-index:10000;background:rgba(255,255,255,0.96);backdrop-filter:blur(20px) saturate(180%);box-shadow:0 8px 40px rgba(0,0,0,0.08);border:1px solid rgba(255,255,255,0.6);border-radius:22px;padding:14px 28px;display:flex;align-items:center;gap:24px;flex-wrap:wrap;justify-content:center;}}
.brand{{display:flex;align-items:center;gap:10px;}}.brand-icon{{font-size:28px;}}.brand-title{{font-size:18px;font-weight:800;color:#1a2634;}}.brand-sub{{font-size:11px;color:#6b7a8d;margin-top:1px;}}
.stats{{display:flex;gap:16px;align-items:center;}}.stat{{text-align:center;}}.stat-val{{font-size:17px;font-weight:800;color:#1a2634;}}.stat-lbl{{font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;letter-spacing:0.4px;margin-top:2px;}}.stat-div{{width:1px;height:32px;background:#e4e9f0;}}
#fp{{position:fixed;top:16px;right:24px;z-index:10001;background:rgba(255,255,255,0.96);backdrop-filter:blur(20px);border-radius:20px;padding:18px 22px;width:380px;max-height:calc(100vh - 40px);overflow-y:auto;box-shadow:0 8px 36px rgba(0,0,0,0.10);border:1px solid rgba(255,255,255,0.6);transition:transform 0.3s ease,opacity 0.3s ease;}}
#fp.collapsed{{transform:translateX(420px);opacity:0;pointer-events:none;}}
#fp h3{{font-size:16px;font-weight:700;color:#1a2634;margin-bottom:14px;}}
.fg{{margin-bottom:14px;}}.fg label{{display:block;font-size:11px;font-weight:600;color:#6b7a8d;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;}}
.chip-group{{display:flex;flex-wrap:wrap;gap:6px;}}
.chip{{display:inline-flex;align-items:center;gap:4px;padding:6px 14px;border-radius:20px;border:2px solid #e4e9f0;background:#fff;font-size:12px;font-weight:500;color:#1a2634;cursor:pointer;transition:all 0.15s;user-select:none;}}
.chip:hover{{border-color:#b0c4d8;background:#f8fafc;}}
.chip.active{{border-color:#E63946;background:#FFF0F0;color:#C62828;}}
.chip .clear{{margin-left:4px;font-weight:700;font-size:14px;line-height:1;}}
.chip-reset{{font-size:11px;color:#8a99aa;cursor:pointer;padding:4px 8px;border-radius:12px;border:1px dashed #d0d8e0;margin-left:4px;}}
.chip-reset:hover{{background:#f0f4f9;color:#1a2634;}}
.mkad-filter{{margin-bottom:16px;}}.mkad-filter .gl{{display:block;font-size:11px;font-weight:600;color:#6b7a8d;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:8px;}}.mkad-btns{{display:flex;gap:6px;flex-wrap:wrap;}}
.mb{{display:flex;align-items:center;gap:6px;padding:7px 14px;border-radius:20px;border:2px solid #e4e9f0;background:#fff;font-size:13px;font-weight:600;color:#1a2634;cursor:pointer;transition:all 0.2s;user-select:none;font-family:inherit;}}.mb:hover{{border-color:#b0c4d8;background:#f8fafc;}}.mb.active{{border-color:#4f7cff;background:#EEF3FF;box-shadow:0 2px 8px rgba(79,124,255,0.15);}}.mb input{{display:none;}}.mb .ic{{font-size:14px;}}
.br{{display:flex;gap:10px;margin-top:14px;}}.bt{{flex:1;border:none;border-radius:12px;padding:11px;font-weight:600;font-size:13px;cursor:pointer;font-family:inherit;transition:all 0.2s;}}.ba{{background:#E63946;color:#fff;}}.ba:hover{{background:#C62828;transform:translateY(-1px);}}.bs{{background:#f0f4f9;color:#324155;}}.bs:hover{{background:#e4e9f0;}}
.note{{margin-top:14px;padding-top:12px;border-top:1px solid #eef2f7;font-size:12px;color:#6b7a8d;line-height:1.6;}}
#ft{{position:fixed;top:20px;right:24px;z-index:10002;border:none;border-radius:14px;padding:10px 18px;background:rgba(255,255,255,0.95);backdrop-filter:blur(12px);color:#1a2634;font-weight:600;font-size:13px;cursor:pointer;box-shadow:0 4px 20px rgba(0,0,0,0.08);font-family:inherit;display:flex;align-items:center;gap:8px;}}#ft:hover{{background:#fff;transform:translateY(-1px);}}
#leg{{position:fixed;left:20px;bottom:24px;z-index:10000;background:rgba(255,255,255,0.95);backdrop-filter:blur(14px);border-radius:18px;padding:18px 22px;min-width:210px;max-width:300px;box-shadow:0 6px 28px rgba(0,0,0,0.08);border:1px solid rgba(255,255,255,0.6);}}
.lt{{font-weight:700;font-size:14px;color:#1a2634;margin-bottom:12px;display:flex;align-items:center;gap:8px;}}.li{{display:flex;align-items:center;gap:8px;margin:5px 0;font-size:12px;color:#324155;padding:3px 6px;border-radius:8px;}}.li:hover{{background:rgba(0,0,0,0.03);}}.ld{{width:12px;height:12px;border-radius:50%;flex-shrink:0;border:1px solid rgba(0,0,0,0.06);}}.lc{{font-weight:600;color:#8a99aa;font-size:11px;background:#f0f4f9;padding:0 8px;border-radius:12px;line-height:20px;margin-left:auto;}}.lv{{border:none;border-top:1px solid #eef2f7;margin:10px 0;}}.lh{{font-size:11px;color:#8a99aa;line-height:1.5;}}
.leaflet-popup-content-wrapper{{border-radius:16px!important;padding:0!important;overflow:hidden;box-shadow:0 16px 48px rgba(0,0,0,0.18)!important;}}.leaflet-popup-content{{margin:0!important;min-width:300px;}}.leaflet-popup-close-button{{top:12px!important;right:12px!important;color:rgba(255,255,255,0.7)!important;font-size:22px!important;}}.leaflet-popup-close-button:hover{{color:#fff!important;}}
.marker-cluster-small{{background-color:rgba(230,57,70,0.2)!important;}}.marker-cluster-small div{{background-color:rgba(230,57,70,0.7)!important;color:#fff!important;font-weight:700!important;}}.marker-cluster-medium{{background-color:rgba(230,57,70,0.3)!important;}}.marker-cluster-medium div{{background-color:rgba(230,57,70,0.8)!important;color:#fff!important;font-weight:700!important;}}.marker-cluster-large{{background-color:rgba(230,57,70,0.4)!important;}}.marker-cluster-large div{{background-color:rgba(230,57,70,0.9)!important;color:#fff!important;font-weight:700!important;}}
.pw{{width:380px;font-family:'Inter',system-ui,sans-serif;color:#1a2634;}}.ph{{padding:16px 20px;border-radius:16px 16px 0 0;}}.pb2{{display:inline-block;background:rgba(255,255,255,0.2);padding:2px 10px;border-radius:20px;font-size:10px;font-weight:600;color:#fff;text-transform:uppercase;letter-spacing:0.5px;margin-bottom:6px;}}.pt2{{font-size:17px;font-weight:800;color:#fff;line-height:1.3;}}.ps{{font-size:12px;color:rgba(255,255,255,0.85);margin-top:4px;}}.pbo{{background:#fafcfe;border:1px solid #eef2f7;border-top:none;border-radius:0 0 16px 16px;padding:16px 20px;}}.pm{{display:flex;flex-wrap:wrap;gap:4px 14px;font-size:12px;color:#6b7a8d;margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid #eef2f7;}}.ig{{display:grid;grid-template-columns:1fr 1fr;gap:8px;}}.ic2{{background:#fff;border:1px solid #eef2f7;border-radius:12px;padding:10px 12px;}}.iv{{font-size:14px;font-weight:700;color:#1a2634;}}.il{{margin-top:3px;font-size:10px;font-weight:600;color:#8a99aa;text-transform:uppercase;letter-spacing:0.3px;}}
.mkb{{display:inline-block;padding:3px 10px;border-radius:20px;font-size:11px;font-weight:700;margin-top:10px;}}.mki{{background:#ECFDF5;color:#065F46;border:1px solid #A7F3D0;}}.mko{{background:#FEF2F2;color:#991B1B;border:1px solid #FECACA;}}
</style>
</head>
<body>
<div id="map"></div>
<div class="top-panel">
    <div class="brand"><span class="brand-icon">🛒</span><div><div class="brand-title">Перекрёсток — адреса ТТ</div><div class="brand-sub">Все точки + контур МКАД</div></div></div>
    <div class="stats">
        <div class="stat"><div class="stat-val" id="k1">{total}</div><div class="stat-lbl">Всего</div></div><div class="stat-div"></div>
        <div class="stat"><div class="stat-val" id="k2" style="color:#2A9D8F;">{cnt_in}</div><div class="stat-lbl">Внутри МКАД</div></div><div class="stat-div"></div>
        <div class="stat"><div class="stat-val" id="k3" style="color:#8a99aa;">{cnt_out}</div><div class="stat-lbl">За МКАД</div></div><div class="stat-div"></div>
        <div class="stat"><div class="stat-val" id="k4">{total}</div><div class="stat-lbl">Показано</div></div>
    </div>
</div>
<button id="ft" onclick="tgl()">⚙️ Фильтры</button>
<div id="fp" class="collapsed">
    <h3>🔍 Фильтры</h3>
    <div class="mkad-filter"><span class="gl">Расположение</span><div class="mkad-btns">
        <label class="mb" id="m-all" onclick="sM('all')"><input type="radio" name="mk" value="all"/><span class="ic">🗺️</span> Все</label>
        <label class="mb" id="m-in" onclick="sM('inside')"><input type="radio" name="mk" value="inside"/><span class="ic">🟢</span> МКАД</label>
        <label class="mb" id="m-out" onclick="sM('outside')"><input type="radio" name="mk" value="outside"/><span class="ic">⬜</span> За МКАД</label>
        <label class="mb active" id="m-mr" onclick="sM('mkad_rostov')"><input type="radio" name="mk" value="mkad_rostov" checked/><span class="ic">🟢➕🌹</span> МКАД + Ростов</label>
    </div></div>
    <div class="fg" id="fg-format"><label>Формат</label><div class="chip-group" id="chip-format"></div><span class="chip-reset" onclick="resetChips('format')">✕ сбросить</span></div>
    <div class="fg" id="fg-division"><label>Дивизион</label><div class="chip-group" id="chip-division"></div><span class="chip-reset" onclick="resetChips('division')">✕ сбросить</span></div>
    <div class="fg" id="fg-status"><label>Статус</label><div class="chip-group" id="chip-status"></div><span class="chip-reset" onclick="resetChips('status')">✕ сбросить</span></div>
    <div class="fg" id="fg-region"><label>Регион</label><div class="chip-group" id="chip-region"></div><span class="chip-reset" onclick="resetChips('region')">✕ сбросить</span></div>
    <div class="fg" id="fg-ua"><label>УА</label><div class="chip-group" id="chip-ua"></div><span class="chip-reset" onclick="resetChips('ua')">✕ сбросить</span></div>
    <div class="fg"><label>Поиск</label><input id="fq" type="text" placeholder="Название, адрес..."/></div>
    <div class="br"><button class="bt ba" onclick="af()">Применить</button><button class="bt bs" onclick="rf()">Сбросить всё</button></div>
    <div class="note" id="fn"></div>
</div>
<div id="leg"></div>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
<script>
const A={json.dumps(stores, ensure_ascii=False)};
const MK={json.dumps(mkad_js)};
const KL={json.dumps(km_js)};
const FC={json.dumps(FORMAT_COLORS, ensure_ascii=False)};
const DC='#E63946';
let cM='mkad_rostov';
// Состояние выбранных чипсов для каждого фильтра
const chipState = {{ format: new Set(), division: new Set(), status: new Set(), region: new Set(), ua: new Set() }};

const e=s=>String(s??'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
const u=a=>[...new Set(a.filter(x=>x&&String(x).trim()!==''))].sort((a,b)=>String(a).localeCompare(String(b),'ru'));

function buildChips(field, values) {{
    const container = document.getElementById('chip-' + field);
    container.innerHTML = '';
    values.forEach(v => {{
        const chip = document.createElement('span');
        chip.className = 'chip';
        chip.textContent = v;
        chip.dataset.value = v;
        chip.dataset.field = field;
        chip.onclick = function(e) {{
            const f = this.dataset.field;
            const val = this.dataset.value;
            if (chipState[f].has(val)) {{
                chipState[f].delete(val);
                this.classList.remove('active');
            }} else {{
                chipState[f].add(val);
                this.classList.add('active');
            }}
        }};
        container.appendChild(chip);
    }});
}}

// Заполняем чипсы
buildChips('format', u(A.map(s=>s.format)));
buildChips('division', u(A.map(s=>s.division)));
buildChips('status', u(A.map(s=>s.status)));
buildChips('region', u(A.map(s=>s.region)));
buildChips('ua', u(A.map(s=>s.ua)));

function resetChips(field) {{
    chipState[field].clear();
    document.querySelectorAll('#chip-' + field + ' .chip').forEach(el => el.classList.remove('active'));
}}

const map=L.map('map',{{zoomControl:true,preferCanvas:true}}).setView([55.751244,37.618423],6);
L.tileLayer('https://{{s}}.basemaps.cartocdn.com/light_all/{{z}}/{{x}}/{{y}}{{r}}.png',{{maxZoom:19,attribution:'© OpenStreetMap © CARTO'}}).addTo(map);
L.polygon(MK,{{color:'#2563EB',weight:3,fill:true,fillColor:'#2563EB',fillOpacity:0.03,dashArray:'6 4'}}).addTo(map).bindTooltip('МКАД');
KL.forEach(k=>{{L.marker([k.lat,k.lon],{{icon:L.divIcon({{className:'',html:`<div style="font-size:9px;font-weight:700;color:#2563EB;background:rgba(255,255,255,0.9);padding:1px 5px;border-radius:6px;border:1px solid #2563EB;">${{k.km}} км</div>`,iconSize:[40,16],iconAnchor:[20,8]}})}}).addTo(map);}});
const cl=L.markerClusterGroup({{chunkedLoading:true,spiderfyOnMaxZoom:true,showCoverageOnHover:false,maxClusterRadius:55}});
map.addLayer(cl);

function gc(f){{return FC[f]||DC;}}
function mp(s){{const c=gc(s.format);const mb=s.mkad?'<span class="mkb mki">✅ Внутри МКАД</span>':'<span class="mkb mko">⬜ За МКАД</span>';return`<div class="pw"><div class="ph" style="background:linear-gradient(135deg,${{c}} 0%,#1a2634 100%);"><div class="pb2">ПЕРЕКРЁСТОК</div><div class="pt2">🛒 ${{e(s.name)}}</div><div class="ps">${{e(s.format)||'—'}} · ${{e(s.status)||'—'}}</div></div><div class="pbo"><div class="pm"><span>📍 ${{e(s.address)||'—'}}</span><span>🏙️ ${{e(s.city)||'—'}}</span><span>🗺️ ${{e(s.division)||'—'}}</span></div><div class="ig"><div class="ic2"><div class="iv" style="color:${{c}};">${{e(s.format)||'—'}}</div><div class="il">Формат</div></div><div class="ic2"><div class="iv">${{e(s.best)||'—'}}</div><div class="il">Best</div></div><div class="ic2"><div class="iv">${{e(s.street)||'—'}}</div><div class="il">Улица</div></div><div class="ic2"><div class="iv">${{e(s.house)||'—'}}</div><div class="il">Дом</div></div><div class="ic2"><div class="iv">${{e(s.filial)||'—'}}</div><div class="il">Филиал</div></div><div class="ic2"><div class="iv">${{e(s.rc)||'—'}}</div><div class="il">РЦ</div></div><div class="ic2"><div class="iv">${{e(s.region)||'—'}}</div><div class="il">Регион</div></div><div class="ic2"><div class="iv" style="font-size:11px;">${{s.lat.toFixed(5)}}, ${{s.lon.toFixed(5)}}</div><div class="il">Координаты</div></div></div>${{mb}}</div></div>`;}}

function rn(st){{cl.clearLayers();const fc2={{}};let ci=0,co=0;st.forEach(s=>{{const f=s.format||'—';fc2[f]=(fc2[f]||0)+1;if(s.mkad)ci++;else co++;const c=gc(s.format);const op=s.mkad?0.85:0.4;const rd=s.mkad?8:6;const w=s.mkad?2:1;const m=L.circleMarker([s.lat,s.lon],{{radius:rd,color:c,weight:w,fillColor:c,fillOpacity:op,opacity:0.9}});m.bindTooltip(`<b>${{e(s.name)}}</b><br>${{e(s.format)||'—'}} · ${{e(s.city)||'—'}}<br>${{s.mkad?'✅ МКАД':'⬜ За МКАД'}}`,{{sticky:true,direction:'top'}});m.bindPopup(mp(s),{{maxWidth:420}});cl.addLayer(m);}});
document.getElementById('k4').textContent=st.length;document.getElementById('k2').textContent=ci;document.getElementById('k3').textContent=co;
document.getElementById('fn').innerHTML=`Показано: <strong>${{st.length}}</strong> из <strong>${{A.length}}</strong><br>МКАД: <strong>${{ci}}</strong> · За: <strong>${{co}}</strong>`;
let lh=`<div class="lt"><span>🛒</span> Форматы</div>`;Object.entries(fc2).sort((a,b)=>b[1]-a[1]).forEach(([f,n])=>{{lh+=`<div class="li"><span class="ld" style="background:${{gc(f)}};"></span><span>${{f}}</span><span class="lc">${{n}}</span></div>`;}});lh+=`<hr class="lv"/>`;lh+=`<div class="li"><span class="ld" style="background:#2A9D8F;"></span><span>Внутри МКАД</span><span class="lc">${{ci}}</span></div>`;lh+=`<div class="li"><span class="ld" style="background:#ccc;"></span><span>За МКАД</span><span class="lc">${{co}}</span></div>`;lh+=`<hr class="lv"/><div class="li"><span class="ld" style="background:transparent;border:2px dashed #2563EB;"></span><span style="color:#2563EB;">МКАД</span></div><div class="lh">Яркие = внутри МКАД</div>`;document.getElementById('leg').innerHTML=lh;}}

window.sM=function(m){{cM=m;['all','in','out','mr'].forEach(x=>{{const id='m-'+ (x==='mr'?'mr':x);const el=document.getElementById(id);if(el) el.classList.toggle('active', (x==='all'&&m==='all')||(x==='in'&&m==='inside')||(x==='out'&&m==='outside')||(x==='mr'&&m==='mkad_rostov'));}});af();if(m==='inside')map.setView([55.751244,37.618423],11);else if(m==='all')map.setView([55.751244,37.618423],6);}}

function gf(){{const fSel = [...chipState['format']]; const dSel = [...chipState['division']]; const sSel = [...chipState['status']]; const rSel = [...chipState['region']]; const uaSel = [...chipState['ua']]; const q=(document.getElementById('fq').value||'').trim().toLowerCase();
return A.filter(s=>{{if(cM==='inside'&&!s.mkad)return false;if(cM==='outside'&&s.mkad)return false;if(cM==='mkad_rostov'&&!(s.mkad||s.city==='г.Ростов-на-Дону'))return false;if(fSel.length&&!fSel.includes(s.format))return false;if(dSel.length&&!dSel.includes(s.division))return false;if(sSel.length&&!sSel.includes(s.status))return false;if(rSel.length&&!rSel.includes(s.region))return false;if(uaSel.length&&!uaSel.includes(s.ua))return false;if(q){{const h=[s.name,s.address,s.street,s.city,s.division,s.filial,s.format,s.region].join(' ').toLowerCase();if(!h.includes(q))return false;}}return true;}});}}

function af(){{rn(gf());}}

function rf(){{cM='all';['all','in','out','mr'].forEach(x=>{{const id='m-'+ (x==='mr'?'mr':x);const el=document.getElementById(id);if(el) el.classList.toggle('active',x==='all');}});Object.keys(chipState).forEach(k=>{{chipState[k].clear();document.querySelectorAll('#chip-'+k+' .chip').forEach(el=>el.classList.remove('active'));}});document.getElementById('fq').value='';rn(A);map.setView([55.751244,37.618423],6);}}

document.getElementById('fq').addEventListener('keydown',e2=>{{if(e2.key==='Enter')af();}});
let fv=false;function tgl(){{fv=!fv;document.getElementById('fp').classList.toggle('collapsed',!fv);document.getElementById('ft').style.display=fv?'none':'flex';}}

// Стартуем с фильтром МКАД+Ростов (город = г.Ростов-на-Дону)
rn(A.filter(s=>s.mkad||s.city==='г.Ростов-на-Дону'));
</script>
</body>
</html>"""

with open(MAP_FILE, "w", encoding="utf-8") as f:
    f.write(html)

print(f"\n✅ Карта: {MAP_FILE}")
print(f"   Всего: {total} | МКАД: {cnt_in} | За: {cnt_out}")