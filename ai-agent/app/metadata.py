"""Метаданные таблицы sales_mart для промпта LLM."""

TABLE_NAME = "sales_mart"
DATABASE = "default"

TABLE_DESCRIPTION = """
Таблица sales_mart — витрина продаж чипсов и снеков по 19 розничным сетям России.
Гранулярность: одна строка = один товар, продажа в конкретном магазине в конкретный день/месяц.
Период: 2023-05-01 .. 2026-05-01. Всего ~14 млн строк.
"""

# Описание колонок
COLUMNS = {
    # === ВРЕМЯ ===
    "date": {
        "type": "String (YYYY-MM-DD)",
        "desc": "Дата продажи. ВАЖНО: тип String, но формат YYYY-MM-DD, поэтому сортировка/сравнение работают правильно.",
        "example": "'2025-07-01'",
        "note": "Для агрегации по месяцам используй toStartOfMonth(toDate(date)) или колонки year+month.",
    },
    "year": {
        "type": "Int16",
        "desc": "Год продажи",
        "example": "2025",
    },
    "month": {
        "type": "String",
        "desc": "Месяц продажи прописью на русском",
        "example": "'Июль', 'Август', 'Сентябрь'",
    },
    "week_num": {
        "type": "Nullable(Int16)",
        "desc": "Номер недели в году (1-53)",
        "example": "28",
    },

    # === СЕТЬ / МАГАЗИН ===
    "retail_chain": {
        "type": "LowCardinality(String)",
        "desc": "Розничная сеть",
        "example": "'Дикси', 'Пятерочка', 'Магнит', 'Ашан', 'Перекресток', 'Лента', 'Окей', 'Верный', 'Красное и Белое', 'Чижик', 'Слата', 'Виктория', 'Батон', 'Бристоль', 'Самокат', 'Хлеб Соль', 'X5 United', 'Пятъница', 'Перекресток-Джем', 'Глобус'",
    },
    "region_name": {
        "type": "Nullable(String)",
        "desc": "Регион / область России. МОЖЕТ БЫТЬ NULL или пустой строкой.",
        "example": "'Московская область', 'Ленинградская область', 'Брянская область'",
        "note": "Часто с ведущим пробелом. Для очистки используй trim(coalesce(region_name, '')).",
    },
    "city_name": {
        "type": "Nullable(String)",
        "desc": "Название города",
        "example": "'Москва', 'Санкт-Петербург', 'Брянск'",
    },
    "address": {
        "type": "Nullable(String)",
        "desc": "Полный адрес магазина (используется для COUNT DISTINCT address = количество торговых точек)",
    },
    "store_code": {
        "type": "Nullable(String)",
        "desc": "Уникальный код магазина в сети",
        "example": "'ДИКСИ-77558'",
    },
    "store_name": {
        "type": "Nullable(String)",
        "desc": "Название магазина",
    },
    "store_format": {
        "type": "Nullable(String)",
        "desc": "Формат магазина (гипермаркет, супермаркет и т.п.)",
        "example": "'ГМ', 'СМ', 'У', 'Дискаунтер'",
    },

    # === ТОВАР ===
    "product_id": {
        "type": "Nullable(String)",
        "desc": "Уникальный идентификатор товара",
    },
    "product_name": {
        "type": "String",
        "desc": "Полное название товара",
        "example": "'КАРТОФЕЛЬ ХРУСТЯЩИЙ В ЛОМТИКАХ С СОЛЬЮ 160Г'",
    },
    "brand": {
        "type": "Nullable(String)",
        "desc": "Бренд товара",
        "example": "\"O'кей\", 'Lay\\'s', 'Pringles', 'Русская картошка', 'Cheetos', 'Lorenz'",
    },
    "vendor": {
        "type": "Nullable(String)",
        "desc": "Поставщик товара",
        "example": "'КДВ ГРУПП'",
    },
    "manufacturer": {
        "type": "Nullable(String)",
        "desc": "Производитель товара",
    },
    "product_category_2": {
        "type": "Nullable(String)",
        "desc": "Категория товара уровень 2 (высокоуровневая)",
        "example": "'Снэки', 'Чипсы', 'Кондитерские изделия (Food)', '213. Чипсы, снеки'",
    },
    "product_category_3": {
        "type": "String",
        "desc": "Категория товара уровень 3",
        "example": "'Чипсы', 'Снэки соленые', 'Снэки сладкие', 'Орехи и сухофрукты (фас)'",
    },
    "product_category_4": {
        "type": "Nullable(String)",
        "desc": "Категория товара уровень 4 (детальная)",
        "example": "'Чипсы картофельные', 'Чипсы прочие', 'Снэки соленые прочие'",
        "note": "ВНИМАНИЕ: в данных иногда встречаются числа как строки. Фильтруй notEmpty(product_category_4) AND NOT match(product_category_4, '^[0-9.]+$').",
    },
    "product_category_5": {
        "type": "Nullable(String)",
        "desc": "Категория товара уровень 5 (самая детальная)",
    },
    "flavor": {
        "type": "Nullable(String)",
        "desc": "Вкус товара",
        "example": "'Соль', 'Сметана и лук', 'Бекон', 'Морепродукты', 'Томат', 'Краб'",
    },
    "weight_grams": {
        "type": "Nullable(Float64) или String",
        "desc": "Вес упаковки в граммах. ТИП МОЖЕТ БЫТЬ РАЗНЫЙ (Float64 или String в зависимости от источника). Для универсальной конвертации ВСЕГДА используй toFloat64OrZero(toString(weight_grams)).",
        "example": "160",
    },
    "chip_type": {
        "type": "Nullable(String)",
        "desc": "Тип чипсов",
        "example": "'Картофельные чипсы', 'Кукурузные чипсы', 'Овощные чипсы', 'Пшеничные чипсы', 'Не чипсы'",
    },
    "package_type": {
        "type": "Nullable(String)",
        "desc": "Тип упаковки",
        "example": "'Пакет', 'Коробка', 'Туба', 'Пластины'",
    },
    "barcode": {
        "type": "Nullable(String)",
        "desc": "Штрихкод товара",
    },

    # === МЕТРИКИ ПРОДАЖ ===
    "sales_quantity": {
        "type": "Int32",
        "desc": "Количество проданных единиц товара (штук)",
        "example": "52",
    },
    "sales_amount_rub": {
        "type": "Float64",
        "desc": "Сумма продаж в рублях. Главная метрика выручки.",
        "example": "8976.43",
    },
    "sales_cost_price": {
        "type": "Float64",
        "desc": "Себестоимость проданного товара (общая, в рублях)",
        "example": "5254.70",
    },
    "sales_kg": {
        "type": "Nullable(Float64)",
        "desc": "Продажи в килограммах (может быть NULL)",
    },
    "sales_tons": {
        "type": "Nullable(Float64)",
        "desc": "Продажи в тоннах",
    },
    "average_cost_price": {
        "type": "Float64",
        "desc": "Средняя себестоимость единицы товара",
        "example": "101.05",
    },
    "average_sell_price": {
        "type": "Float64",
        "desc": "Средняя цена продажи единицы товара",
        "example": "172.62",
    },
    "margin_rub": {
        "type": "Nullable(Float64)",
        "desc": "Маржа в рублях. ВНИМАНИЕ: часто NULL. Если пусто — считай как sales_amount_rub - sales_cost_price.",
    },
    "margin_pct": {
        "type": "Nullable(Float64)",
        "desc": "Маржинальность в процентах. Часто NULL.",
    },
    "cost_price_rub": {
        "type": "Nullable(Float64)",
        "desc": "Себестоимость в рублях (альтернативное поле, часто NULL)",
    },
    "max_sell_price": {
        "type": "Nullable(Float64)",
        "desc": "Максимальная цена продажи. Часто NULL.",
    },
    "max_cost_price": {
        "type": "Nullable(Float64)",
        "desc": "Максимальная себестоимость. Часто NULL.",
    },
    "stock_qty": {
        "type": "Nullable(Int32)",
        "desc": "Остаток товара в штуках. Часто NULL.",
    },
    "stock_rub": {
        "type": "Nullable(Float64)",
        "desc": "Остаток товара в рублях. Часто NULL.",
    },
    "promo_sales_rub": {
        "type": "Nullable(Float64)",
        "desc": "Промо-продажи в рублях (по акции). Часто NULL.",
    },

    # === СЛУЖЕБНЫЕ ===
    "file_name": {
        "type": "String",
        "desc": "Имя исходного файла (служебное, для агента не показывать)",
    },
    "created_at": {
        "type": "Date",
        "desc": "Дата загрузки записи в базу (служебное)",
    },
}


# Часто используемые бизнес-фразы (для промпта)
BUSINESS_GLOSSARY = """
Бизнес-термины:
- "продажи", "выручка", "оборот" → SUM(sales_amount_rub)
- "количество", "штук", "единиц" → SUM(sales_quantity)
- "себестоимость" → SUM(sales_cost_price)
- "маржа" → SUM(coalesce(margin_rub, sales_amount_rub - sales_cost_price))
- "маржинальность %" → SUM(coalesce(margin_rub, sales_amount_rub - sales_cost_price)) / NULLIF(SUM(sales_amount_rub), 0) * 100
- "средний чек" / "средняя цена" → SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0)
- "остаток" → SUM(stock_qty) или SUM(stock_rub)
- "промо" / "по акции" → SUM(promo_sales_rub)
- "магазины" → COUNT(DISTINCT store_code)
- "торговые точки" / "ТТ" → COUNT(DISTINCT address)
- "товары" / "SKU" → COUNT(DISTINCT product_id)
- "чипсы" (как категория) → WHERE product_category_2 IN ('Чипсы', 'Снэки', '213. Чипсы, снеки')
- "цена за грамм" → (SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0)) / NULLIF(toFloat64OrZero(toString(weight_grams)), 0)
"""


COMMON_RULES = """
ВАЖНЫЕ ПРАВИЛА:
1. Диалект: ClickHouse.
2. Разрешены ТОЛЬКО SELECT-запросы. Никаких INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, CREATE.
3. Возвращай ТОЛЬКО SQL. Без markdown, без ```sql, без объяснений, без комментариев.
4. Всегда добавляй LIMIT, если запрос может вернуть много строк. Максимум 1000.
5. Для NULL используй coalesce() или IFNULL().
6. Строковые сравнения — регистр-чувствительные. Для нечёткого поиска используй ILIKE или positionCaseInsensitive.
7. Даты хранятся как String в формате YYYY-MM-DD. Сравнения через '<', '>', '=' работают. Для функций дат: toDate(date).
8. Для агрегации по месяцу используй year, month (String, на русском) или toStartOfMonth(toDate(date)).
9. Для агрегации по неделе используй week_num или toStartOfWeek(toDate(date)).
10. Веса товаров в колонке weight_grams — тип может быть Float64 или String. Для универсальной конвертации ВСЕГДА используй toFloat64OrZero(toString(weight_grams)). Пример фильтра: toInt32(toFloat64OrZero(toString(weight_grams))) = 120.
11. Для топов всегда используй ORDER BY <метрика> DESC LIMIT N.
12. Игнорируй строки где product_category_4 содержит только числа: NOT match(product_category_4, '^[0-9.]+$').
13. Округляй суммы: round(SUM(sales_amount_rub), 2).
14. Если пользователь не уточнил период — не фильтруй по дате.
15. Не используй CTE (WITH) без крайней нужды — просто вложенный SELECT.
16. Если пользователь просит "анализ", "детальный", "разбивку", "детально" — группируй по нескольким разрезам сразу: например по дате + бренду или бренду + вкусу.
17. Если просят "по месяцам" — используй toStartOfMonth(toDate(date)) или year+month.
18. Если просят "по дням" — GROUP BY date.
19. Если просят "динамику" — тоже time series (по месяцу или дню).
20. НЕ используй LIMIT 1 если только явно не просят "одну строку" или "общий итог".
21. Для агрегации ВСЕГДА добавляй сумму по количеству и выручке одновременно, чтобы дать полную картину.
22. Для "цены за 1 грамм": (SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0)) / NULLIF(toFloat64OrZero(toString(weight_grams)), 0)
23. Для "нормализованной цены к X грамм" (пересчёт как бы вес был X): цена_за_упаковку / вес * X. То есть: (SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0)) / NULLIF(toFloat64OrZero(toString(weight_grams)), 0) * X.
24. Если в промпте есть блок "РАСПОЗНАННЫЕ БИЗНЕС-МЕТРИКИ" — используй формулы ИМЕННО оттуда, не изобретай свои.
25. Если пользователь дал список граммовок (типа "70, 120, 140 грамм") — фильтруй toInt32(toFloat64OrZero(toString(weight_grams))) IN (...) и группируй ПО weight_grams, чтобы не смешивать разные фасовки.
26. При работе с ценой за грамм ОБЯЗАТЕЛЬНО группируй по weight_grams — иначе средневзвешенное будет ложным.
"""


IMPORTANT_HINTS = """
Специфичные подсказки для этой таблицы:
- Кол-во торговых точек (ТТ, магазинов) = COUNT(DISTINCT address).
- Store_format принимает значения: 'ГМ' (гипермаркет), 'СМ' (супермаркет), 'У' (у дома) и т.п.
- weight_grams — тип может быть Float64 или String. ВСЕГДА пиши: toInt32(toFloat64OrZero(toString(weight_grams))) = 120.
- year — Int16, сравнивай напрямую year = 2026.
- Для чистки региона используй trim(region_name) — там бывают ведущие пробелы.
- Всегда фильтруй is not null для колонок со звёздочкой (region_name, brand, flavor).
- Приоритетные вкусы часто спрашивают вместе: 'Сметана и лук', 'Морепродукты', 'Томат', 'Лосось'.
- ГМ = гипермаркет. СМ = супермаркет.
- Категории чипсов: 'Картофельные чипсы', 'Кукурузные чипсы', 'Овощные чипсы', 'Пшеничные чипсы'.
"""


def format_columns_for_prompt() -> str:
    """Форматирует колонки для системного промпта."""
    lines = [f"Таблица: {DATABASE}.{TABLE_NAME}", ""]
    lines.append(TABLE_DESCRIPTION.strip())
    lines.append("")
    lines.append("Колонки:")

    for name, meta in COLUMNS.items():
        line = f"  {name} ({meta['type']}) — {meta['desc']}"
        if "example" in meta:
            line += f" Пример: {meta['example']}."
        if "note" in meta:
            line += f" ⚠️ {meta['note']}"
        lines.append(line)

    lines.append("")
    lines.append(IMPORTANT_HINTS)

    return "\n".join(lines)