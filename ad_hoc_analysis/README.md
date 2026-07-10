# 1. Сначала генерируем карту (один раз, можно оставить на ночь)
source venv/bin/activate
python generate_map.py

# 2. Запускаем сервер (мгновенно)

nohup python3 app.py > app.log 2>&1 &