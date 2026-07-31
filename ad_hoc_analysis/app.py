# server.py — единый сервер с 4 картами

from functools import wraps
import secrets
from pathlib import Path
from flask import Flask, render_template_string, request, session, redirect, url_for, send_file

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

USERS = {'admin': '123', 'test': '123'}

DATA_DIR = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data")

MAPS = {
    'main': {
        'path':  DATA_DIR / "x5_stores_map_final.html",
        'title': 'Все сети — полная карта',
        'icon':  '🗺️',
    },
    'perekrestok_mkad': {
        'path':  DATA_DIR / "perekrestok_moscow_mkad_map.html",
        'title': 'Перекрёсток МКАД (продажи)',
        'icon':  '🛒',
    },
    'perekrestok_addresses': {
        'path':  DATA_DIR / "perekrestok_addresses_map.html",
        'title': 'Перекрёсток — адреса ТТ',
        'icon':  '📍',
    },
}

DOWNLOADS = {
    'perekrestok_xlsx': {
        'path':  DATA_DIR / "perekrestok_moscow_mkad.xlsx",
        'title': 'Перекрёсток МКАД (.xlsx)',
        'icon':  '📊',
    },
}

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated

def get_navbar_html(active_key=None):
    user = session.get('user', '?')
    map_links = ""
    for key, info in MAPS.items():
        is_active = 'active' if key == active_key else ''
        map_links += f"""
        <a href="/map/{key}" class="nav-link {is_active}">
            <span class="nav-icon">{info['icon']}</span>
            <span class="nav-text">{info['title']}</span>
        </a>"""
    download_links = ""
    for key, info in DOWNLOADS.items():
        if info['path'].exists():
            download_links += f"""
            <a href="/download/{key}" class="nav-link download">
                <span class="nav-icon">{info['icon']}</span>
                <span class="nav-text">{info['title']}</span>
            </a>"""
    return f"""
    <div id="app-navbar">
        <div class="nav-brand"><span class="nav-brand-icon">🥔</span><span class="nav-brand-text">Kaftal Analytics</span></div>
        <div class="nav-links">{map_links}{download_links}</div>
        <div class="nav-user">
            <span class="nav-user-name">👤 {user}</span>
            <form action="/logout" method="post" style="display:inline;"><button type="submit" class="nav-logout">Выйти</button></form>
        </div>
    </div>
    <style>
        #app-navbar {{position:fixed;bottom:0;left:0;right:0;z-index:99999;background:rgba(255,255,255,0.97);backdrop-filter:blur(20px) saturate(180%);box-shadow:0 -4px 32px rgba(0,0,0,0.08);border-top:1px solid rgba(0,0,0,0.06);display:flex;align-items:center;justify-content:space-between;padding:0 24px;height:56px;font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;}}
        .nav-brand {{display:flex;align-items:center;gap:8px;font-weight:800;font-size:15px;color:#1a2634;}}.nav-brand-icon {{font-size:22px;}}
        .nav-links {{display:flex;gap:4px;align-items:center;overflow-x:auto;}}
        .nav-link {{display:flex;align-items:center;gap:6px;padding:8px 16px;border-radius:12px;font-size:13px;font-weight:600;color:#324155;text-decoration:none;white-space:nowrap;transition:all 0.2s;border:2px solid transparent;}}.nav-link:hover {{background:#f0f4f9;}}.nav-link.active {{background:#EEF3FF;border-color:#4f7cff;color:#4f7cff;}}.nav-link.download {{color:#2A9D8F;}}.nav-link.download:hover {{background:#E6F7F5;}}
        .nav-icon {{font-size:16px;}}.nav-user {{display:flex;align-items:center;gap:12px;flex-shrink:0;}}.nav-user-name {{font-size:13px;font-weight:600;color:#6b7a8d;}}
        .nav-logout {{border:none;background:#f0f4f9;color:#E63946;padding:6px 14px;border-radius:10px;font-size:12px;font-weight:600;cursor:pointer;transition:all 0.2s;font-family:inherit;}}.nav-logout:hover {{background:#fde8ea;}}
        body {{padding-bottom:60px !important;}}
    </style>"""

@app.route('/')
@login_required
def index():
    return redirect(url_for('show_map', map_key='main'))

@app.route('/map/<map_key>')
@login_required
def show_map(map_key):
    if map_key not in MAPS:
        return f"<h1>Карта '{map_key}' не найдена</h1>", 404
    map_info = MAPS[map_key]
    if not map_info['path'].exists():
        return f"<h1>'{map_info['title']}' не сгенерирована</h1><p>{map_info['path']}</p>", 404
    with open(map_info['path'], 'r', encoding='utf-8') as f:
        html = f.read()
    navbar = get_navbar_html(active_key=map_key)
    if '</body>' in html:
        html = html.replace('</body>', navbar + '\n</body>')
    else:
        html += navbar
    return html

@app.route('/download/<file_key>')
@login_required
def download_file(file_key):
    if file_key not in DOWNLOADS:
        return "Not found", 404
    info = DOWNLOADS[file_key]
    if not info['path'].exists():
        return f"Файл не найден: {info['path']}", 404
    return send_file(info['path'], as_attachment=True, download_name=info['path'].name)

@app.route('/data/<filename>')
@login_required
def data_file(filename):
    allowed = {'stores.json','cities.json','fd.json','regions.json','details.json'}
    if filename not in allowed:
        return "Forbidden", 403
    fp = DATA_DIR / filename
    if not fp.exists():
        return "Not found", 404
    return send_file(fp, mimetype='application/json')

LOGIN_HTML = """<!DOCTYPE html><html><head><title>Вход</title>
<style>*{margin:0;padding:0;box-sizing:border-box;}body{font-family:'Inter',system-ui,sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;background:linear-gradient(135deg,#667eea,#764ba2);}.card{background:#fff;padding:48px 40px;border-radius:24px;max-width:420px;width:100%;box-shadow:0 24px 80px rgba(0,0,0,0.25);}h2{text-align:center;margin-bottom:8px;font-size:24px;color:#1a2634;}.sub{text-align:center;color:#6b7a8d;font-size:14px;margin-bottom:32px;}input{width:100%;padding:14px 18px;margin-bottom:16px;border:2px solid #e4e9f0;border-radius:14px;font-size:15px;outline:none;font-family:inherit;}input:focus{border-color:#4f7cff;}button{width:100%;padding:16px;background:#4f7cff;color:#fff;border:none;border-radius:14px;font-size:16px;font-weight:700;cursor:pointer;font-family:inherit;}button:hover{background:#3d6ae6;}.error{text-align:center;color:#E63946;margin-top:16px;font-weight:600;}.hint{text-align:center;color:#8a99aa;margin-top:20px;font-size:13px;}</style>
</head><body><div class="card"><h2>🥔 Kaftal Analytics</h2><div class="sub">Аналитика продаж</div><form method="POST"><input type="text" name="username" placeholder="Логин" autofocus/><input type="password" name="password" placeholder="Пароль"/><button type="submit">Войти</button></form>__ERROR__<div class="hint">💡 admin / 123</div></div></body></html>"""

@app.route('/login', methods=['GET','POST'])
def login():
    if request.method == 'POST':
        u = request.form.get('username','').strip()
        p = request.form.get('password','').strip()
        if u in USERS and USERS[u] == p:
            session['user'] = u
            return redirect(url_for('index'))
        return LOGIN_HTML.replace('__ERROR__','<div class="error">❌ Неверный логин или пароль</div>')
    return LOGIN_HTML.replace('__ERROR__','')

@app.route('/logout', methods=['POST'])
def logout():
    session.pop('user', None)
    return redirect(url_for('login'))

if __name__ == '__main__':
    print(f"\n📂 {DATA_DIR}\n\n🗺️  Карты:")
    for k, i in MAPS.items():
        print(f"  {'✅' if i['path'].exists() else '❌'} /map/{k} → {i['title']}")
    print(f"\n📥 Скачивание:")
    for k, i in DOWNLOADS.items():
        print(f"  {'✅' if i['path'].exists() else '❌'} /download/{k} → {i['title']}")
    print(f"\n🚀 http://0.0.0.0:5000\n")
    app.run(host='0.0.0.0', port=5000, debug=True)