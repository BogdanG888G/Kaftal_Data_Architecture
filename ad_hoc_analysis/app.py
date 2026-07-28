from functools import wraps
import secrets
from pathlib import Path
from flask import Flask, render_template_string, request, session, redirect, url_for, send_file

app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

# ============================================================
# НАСТРОЙКИ
# ============================================================
USERS = {
    'admin': '123',
    'test': '123',
}

# Путь к сгенерированной карте
MAP_PATH = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data/x5_stores_map_final.html")

# Путь к JSON-файлам (такой же, как в generate_map.py)
DATA_DIR = Path("/home/bogdangor/Kaftal_Data_Architecture/ad_hoc_analysis/data")

# ============================================================
# НОВЫЙ МАРШРУТ ДЛЯ ОТДАЧИ JSON-ФАЙЛОВ
# ============================================================
@app.route('/data/<filename>')
def data_file(filename):
    # Разрешаем только нужные файлы (безопасность)
    allowed = {
    'stores.json',
    'cities.json',
    'fd.json',
    'regions.json',
    'details.json'
}
    if filename not in allowed:
        return "Forbidden", 403
    file_path = DATA_DIR / filename
    if not file_path.exists():
        return "Not found", 404
    return send_file(file_path, mimetype='application/json')

# ============================================================
# ФУНКЦИИ АВТОРИЗАЦИИ
# ============================================================
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# ============================================================
# МАРШРУТЫ
# ============================================================
@app.route('/')
@login_required
def index():
    try:
        with open(MAP_PATH, 'r', encoding='utf-8') as f:
            html_content = f.read()
        return render_template_string(html_content)
    except FileNotFoundError:
        return "<h1>Карта еще не сгенерирована</h1><p>Запусти generate_map.py</p>", 404
    except Exception as e:
        return f"<h1>Ошибка</h1><pre>{str(e)}</pre>", 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '').strip()
        
        print(f"Попытка входа: username={username}, password={password}")
        
        # Проверка
        if username in USERS and USERS[username] == password:
            session['user'] = username
            print(f"✅ Успешный вход: {username}")
            return redirect(url_for('index'))
        
        print(f"❌ Неудачный вход: {username}")
        
        # Ошибка входа - показываем форму с ошибкой
        return '''
        <!DOCTYPE html>
        <html>
        <head><title>Вход</title></head>
        <body style="font-family:Arial;display:flex;justify-content:center;align-items:center;height:100vh;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);">
            <div style="background:white;padding:40px;border-radius:20px;max-width:400px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,0.3);">
                <h2 style="text-align:center;margin-bottom:30px;">🔐 Вход в систему</h2>
                <form method="POST">
                    <input type="text" name="username" placeholder="Логин" 
                           style="width:100%;padding:14px;margin-bottom:15px;border:2px solid #e4e9f0;border-radius:10px;font-size:16px;">
                    <input type="password" name="password" placeholder="Пароль" 
                           style="width:100%;padding:14px;margin-bottom:20px;border:2px solid #e4e9f0;border-radius:10px;font-size:16px;">
                    <button type="submit" 
                            style="width:100%;padding:16px;background:#4f7cff;color:white;border:none;border-radius:10px;font-size:16px;font-weight:bold;cursor:pointer;">
                        Войти
                    </button>
                </form>
                <p style="text-align:center;color:red;margin-top:15px;">Неверный логин или пароль</p>
                <p style="text-align:center;color:#888;margin-top:20px;font-size:14px;">
                    💡 Логин: <strong>test/admin</strong> · Пароль: <strong>123</strong>
                </p>
            </div>
        </body>
        </html>
        '''
    
    # GET запрос - показываем форму входа
    return '''
    <!DOCTYPE html>
    <html>
    <head><title>Вход</title></head>
    <body style="font-family:Arial;display:flex;justify-content:center;align-items:center;height:100vh;background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);">
        <div style="background:white;padding:40px;border-radius:20px;max-width:400px;width:100%;box-shadow:0 20px 60px rgba(0,0,0,0.3);">
            <h2 style="text-align:center;margin-bottom:30px;">🔐 Вход в систему</h2>
            <form method="POST">
                <input type="text" name="username" placeholder="Логин" 
                       style="width:100%;padding:14px;margin-bottom:15px;border:2px solid #e4e9f0;border-radius:10px;font-size:16px;">
                <input type="password" name="password" placeholder="Пароль" 
                       style="width:100%;padding:14px;margin-bottom:20px;border:2px solid #e4e9f0;border-radius:10px;font-size:16px;">
                <button type="submit" 
                        style="width:100%;padding:16px;background:#4f7cff;color:white;border:none;border-radius:10px;font-size:16px;font-weight:bold;cursor:pointer;">
                    Войти
                </button>
            </form>
            <p style="text-align:center;color:#888;margin-top:20px;font-size:14px;">
                💡 Логин: <strong>admin/test</strong> · Пароль: <strong>123</strong>
            </p>
        </div>
    </body>
    </html>
    '''

@app.route('/logout', methods=['POST'])
def logout():
    session.pop('user', None)
    return redirect(url_for('login'))

@app.route('/debug')
def debug():
    return f"Users: {USERS}, Session: {dict(session)}"

# ============================================================
# ЗАПУСК СЕРВЕРА
# ============================================================
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)