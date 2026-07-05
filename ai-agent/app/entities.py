"""Извлечение и обогащение сущностей из вопроса пользователя."""
import re
from functools import lru_cache
from database import db


# Словари синонимов (расширяй по мере обнаружения)
BRAND_SYNONYMS = {
    "lay's": ["Lay's", "Lays", "Lays МАХХ", "Lays STAX", "Лейз из печи"],
    "lays": ["Lay's", "Lays", "Lays МАХХ", "Lays STAX", "Лейз из печи"],
    "лейс": ["Lay's", "Lays", "Lays МАХХ", "Lays STAX", "Лейз из печи"],
    "лейз": ["Lay's", "Lays", "Lays МАХХ", "Lays STAX", "Лейз из печи"],
    "лэйс": ["Lay's", "Lays", "Lays МАХХ", "Lays STAX", "Лейз из печи"],
    "лей'c": ["Lay's", "Lays", "Lays МАХХ", "Lays STAX"],
    "cheetos": ["Cheetos", "CHEETOS"],
    "читос": ["Cheetos", "CHEETOS"],
    "принглс": ["Pringles"],
    "pringles": ["Pringles"],
    "русская картошка": ["Русская картошка", "Русская Картошка"],
    "хрустящий картофель": ["Хрустящий картофель", "О'кей"],
    "окей": ["О'кей"],
    "o'key": ["О'кей"],
    "bruto": ["Bruto", "BRUTO КРАФТ", "Bruto Gourmet Line", "Bruto NPC", "Bruto Black"],
    "бруто": ["Bruto", "BRUTO КРАФТ", "Bruto Gourmet Line", "Bruto NPC", "Bruto Black"],
    "doritos": ["Doritos"],
    "доритос": ["Doritos"],
    "лоренз": ["Lorenz", "Lorenz Деревенские"],
    "lorenz": ["Lorenz", "Lorenz Деревенские"],
    "принг": ["Pringles"],
    "фокс": ["Fox", "FOX"],
    "fox": ["Fox", "FOX"],
    "чио": ["Чио", "Чим-Чим"],
    "естрелла": ["Estrella"],
    "estrella": ["Estrella"],
}

CHAIN_SYNONYMS = {
    "магнит": ["Магнит"],
    "пятерочка": ["Пятерочка"],
    "пятёрочка": ["Пятерочка"],
    "пятерка": ["Пятерочка"],
    "пятёрка": ["Пятерочка"],
    "5ка": ["Пятерочка"],
    "дикси": ["Дикси"],
    "перекресток": ["Перекресток", "Перекресток-Джем"],
    "перекрёсток": ["Перекресток", "Перекресток-Джем"],
    "лента": ["Лента"],
    "ашан": ["Ашан"],
    "окей": ["Окей"],
    "верный": ["Верный"],
    "красное и белое": ["Красное и Белое"],
    "красное белое": ["Красное и Белое"],
    "кб": ["Красное и Белое"],
    "чижик": ["Чижик"],
    "слата": ["Слата"],
    "виктория": ["Виктория"],
    "батон": ["Батон"],
    "бристоль": ["Бристоль"],
    "самокат": ["Самокат"],
    "хлеб соль": ["Хлеб Соль"],
    "x5": ["X5 United", "Пятерочка", "Перекресток"],
    "х5": ["X5 United", "Пятерочка", "Перекресток"],
    # склонения (дательный, творительный, предложный)
    "магниту": ["Магнит"],
    "магнитом": ["Магнит"],
    "магните": ["Магнит"],
    "магнита": ["Магнит"],
    "пятерочке": ["Пятерочка"],
    "пятерочки": ["Пятерочка"],
    "пятерочкой": ["Пятерочка"],
    "дикси в": ["Дикси"],
    "ленте": ["Лента"],
    "ленты": ["Лента"],
    "лентой": ["Лента"],
    "ашане": ["Ашан"],
    "ашана": ["Ашан"],
    "ашаном": ["Ашан"],
    "перекрестке": ["Перекресток", "Перекресток-Джем"],
    "перекрестка": ["Перекресток", "Перекресток-Джем"],
    "перекрестком": ["Перекресток", "Перекресток-Джем"],
    "самокате": ["Самокат"],
    "самоката": ["Самокат"],
    "самокату": ["Самокат"],
    "окей в": ["Окей"],
    "окея": ["Окей"],
    "окею": ["Окей"],
    "верном": ["Верный"],
    "верного": ["Верный"],
    "чижике": ["Чижик"],
    "чижика": ["Чижик"],
}

FLAVOR_SYNONYMS = {
    "сметана и лук": ["Сметана и лук"],
    "сметана лук": ["Сметана и лук"],
    "сметана-лук": ["Сметана и лук"],
    "сметана": ["Сметана и лук", "Сметана"],
    "лук": ["Сметана и лук", "Лук"],
    "морепродукты": ["Морепродукты", "Лосось"],
    "краб": ["Морепродукты", "Камчатский краб"],
    "камчатский краб": ["Морепродукты", "Камчатский краб"],
    "лосось": ["Лосось"],
    "томат": ["Томат", "Кетчуп"],
    "кетчуп": ["Томат", "Кетчуп"],
    "кетчуп/томат": ["Томат", "Кетчуп"],
    "паприка": ["Паприка"],
    "сыр": ["Сыр", "Чеддер", "Пармезан"],
    "чеддер": ["Чеддер"],
    "бекон": ["Бекон"],
    "соль": ["Соль"],
    "чеснок": ["Чеснок"],
    "грибы": ["Грибы"],
    "стейк": ["Стейк"],
    "перец": ["Перец"],
}

FORMAT_SYNONYMS = {
    "гм": ["ГМ"],
    "гипермаркет": ["ГМ"],
    "гипер": ["ГМ"],
    "см": ["СМ"],
    "супермаркет": ["СМ"],
    "супер": ["СМ"],
    "у дома": ["У"],
    "дискаунтер": ["Дискаунтер"],
}

CHIP_TYPE_SYNONYMS = {
    "картофельные": ["Картофельные чипсы"],
    "картошка": ["Картофельные чипсы"],
    "кукурузные": ["Кукурузные чипсы"],
    "овощные": ["Овощные чипсы"],
    "пшеничные": ["Пшеничные чипсы"],
    "рисовые": ["Рисовые чипсы"],
    "бобовые": ["Бобовые чипсы"],
    "фруктовые": ["Фруктовые чипсы"],
    "батат": ["Чипсы из батата"],
    "водоросли": ["Чипсы из водорослей"],
    "лаваш": ["Чипсы из лаваша"],
}


# ============================================================
# Загрузка справочников из ClickHouse (один раз в кэш)
# ============================================================

@lru_cache(maxsize=1)
def load_brands() -> list:
    df = db.query("SELECT DISTINCT brand FROM sales_mart WHERE brand IS NOT NULL AND brand != ''")
    return df["brand"].tolist()


@lru_cache(maxsize=1)
def load_chains() -> list:
    df = db.query("SELECT DISTINCT retail_chain FROM sales_mart WHERE retail_chain != ''")
    return df["retail_chain"].tolist()


@lru_cache(maxsize=1)
def load_flavors() -> list:
    df = db.query("SELECT DISTINCT flavor FROM sales_mart WHERE flavor IS NOT NULL AND flavor != ''")
    return df["flavor"].tolist()


@lru_cache(maxsize=1)
def load_regions() -> list:
    df = db.query("SELECT DISTINCT trim(region_name) as region FROM sales_mart WHERE region_name IS NOT NULL AND trim(region_name) != ''")
    return df["region"].tolist()


@lru_cache(maxsize=1)
def load_chip_types() -> list:
    df = db.query("SELECT DISTINCT chip_type FROM sales_mart WHERE chip_type IS NOT NULL AND chip_type != ''")
    return df["chip_type"].tolist()


@lru_cache(maxsize=1)
def load_formats() -> list:
    df = db.query("SELECT DISTINCT store_format FROM sales_mart WHERE store_format IS NOT NULL AND store_format != ''")
    return df["store_format"].tolist()


# ============================================================
# Fuzzy matching
# ============================================================

def _fuzzy_match(query: str, choices: list, threshold: int = 75, limit: int = 5) -> list:
    """
    Возвращает список значений из choices, похожих на query.
    Использует rapidfuzz если доступен, иначе простой substring match.
    """
    query_lower = query.lower().strip()

    # Точное совпадение (без учёта регистра)
    exact = [c for c in choices if c.lower() == query_lower]
    if exact:
        return exact

    # Substring match (регистронезависимо)
    substring = [c for c in choices if query_lower in c.lower() or c.lower() in query_lower]
    substring = sorted(substring, key=lambda x: abs(len(x) - len(query)))[:limit]

    try:
        from rapidfuzz import process, fuzz
        # Ищем через token_set_ratio — лучше для многословных
        results = process.extract(query, choices, scorer=fuzz.token_set_ratio, limit=limit)
        fuzzy = [name for name, score, _ in results if score >= threshold]
        # Объединяем substring и fuzzy, без дублей
        seen = set()
        combined = []
        for item in substring + fuzzy:
            if item not in seen:
                seen.add(item)
                combined.append(item)
        return combined[:limit]
    except ImportError:
        return substring


# ============================================================
# Извлечение сущностей из вопроса
# ============================================================

STOP_WORDS = {
    # общие
    "чипсы", "продажи", "выручка", "количество", "штук", "штуки", "штука",
    "магазин", "магазины", "товар", "товары", "цена", "цены",
    "анализ", "отчет", "отчёт", "презентация", "детально", "детальный",
    "покажи", "дай", "сделай", "хочу", "нужно", "нужен",
    "топ", "все", "лучший", "лучшие", "худший", "самый", "самая", "самое",
    "средний", "средняя", "среднее", "общий", "общая", "общее",
    "период", "месяц", "неделя", "день", "год", "года", "лет",
    "категория", "категории", "категорий", "тип", "типы", "вкус", "вкусы",
    "бренд", "бренды", "сеть", "сети", "формат", "форматы",
    "регион", "регионы", "город", "города", "территория", "территории",
    "рублей", "рубли", "рубль", "грамм", "граммов", "гр", "кг",
    "также", "тоже", "разбей", "разбить", "группировать", "группировка",
    "какие", "какой", "какая", "какое", "сколько", "где", "когда", "что",
    "маржа", "маржинальность", "себестоимость", "оборот", "прибыль",
    "остаток", "остатки", "промо", "акция", "акции",
    "мне", "нам", "им", "их", "его", "ее", "её",
    "для", "про", "под", "над", "без", "при", "или", "либо",
    # месяцы (это отдельная категория, не бренды)
    "январь", "января", "январе", "янв",
    "февраль", "февраля", "феврале", "фев",
    "март", "марта", "марте", "мар",
    "апрель", "апреля", "апреле", "апр",
    "май", "мая", "мае",
    "июнь", "июня", "июне", "июн",
    "июль", "июля", "июле", "июл",
    "август", "августа", "августе", "авг",
    "сентябрь", "сентября", "сентябре", "сен", "сент",
    "октябрь", "октября", "октябре", "окт",
    "ноябрь", "ноября", "ноябре", "ноя",
    "декабрь", "декабря", "декабре", "дек",
    # предлоги и общие
    "excel", "эксель", "файл", "документ", "выгрузка", "выгрузку",
    "сделай", "составь", "построй", "сформируй", "напиши",
    "цена", "цены", "цен", "стоимость", "стоимости",
    "по", "за", "в", "на", "у", "с", "к", "от", "до",
    "магниту", "пятерочке", "дикси", "ленте", "ашане", "перекрестку",
    "самокату", "окею", "верному",
}


def extract_entities(question: str) -> dict:
    """
    Из вопроса пользователя извлекает возможные сущности:
    бренды, сети, вкусы, форматы, регионы, типы чипсов.
    """
    q_lower = question.lower()

    entities = {
        "brands": [],
        "chains": [],
        "flavors": [],
        "formats": [],
        "chip_types": [],
        "regions": [],
    }

    # 1. Проверяем словари синонимов (это надёжно)
    for key, values in BRAND_SYNONYMS.items():
        if key in q_lower:
            entities["brands"].extend(values)

    for key, values in CHAIN_SYNONYMS.items():
        # Целые слова для сетей
        if re.search(rf"\b{re.escape(key)}\b", q_lower):
            entities["chains"].extend(values)

    for key, values in FLAVOR_SYNONYMS.items():
        if key in q_lower:
            entities["flavors"].extend(values)

    for key, values in FORMAT_SYNONYMS.items():
        if re.search(rf"\b{re.escape(key)}\b", q_lower):
            entities["formats"].extend(values)

    for key, values in CHIP_TYPE_SYNONYMS.items():
        if key in q_lower:
            entities["chip_types"].extend(values)

    # 2. Fuzzy fallback — ТОЛЬКО для слов длиннее 4 символов и НЕ в стоп-листе
    # И ТОЛЬКО если из словарей ничего не нашли для этой категории
    words = re.findall(r"[а-яА-Яa-zA-Z']{4,}", question)
    candidate_words = [
        w for w in words
        if w.lower() not in STOP_WORDS and len(w) >= 4
    ]

    # Fuzzy для брендов — только с очень высоким порогом
    if not entities["brands"] and candidate_words:
        all_brands = load_brands()
        found = set()
        for word in candidate_words:
            matches = _fuzzy_match(word, all_brands, threshold=92, limit=3)
            found.update(matches)
        entities["brands"] = list(found)[:5]

    # Fuzzy для сетей — тоже высокий порог
    if not entities["chains"] and candidate_words:
        all_chains = load_chains()
        found = set()
        for word in candidate_words:
            matches = _fuzzy_match(word, all_chains, threshold=92, limit=2)
            found.update(matches)
        entities["chains"] = list(found)[:3]

    # Дедупликация с сохранением порядка
    for k in entities:
        entities[k] = list(dict.fromkeys(entities[k]))

    return entities


def build_entities_hint(entities: dict) -> str:
    """
    Формирует подсказку для LLM с реальными значениями из БД,
    которые LLM должна использовать в WHERE.
    """
    hints = []

    if entities["brands"]:
        vals = ", ".join(f"'{v}'" for v in entities["brands"])
        hints.append(f"Бренды в базе (используй IN): brand IN ({vals})")

    if entities["chains"]:
        vals = ", ".join(f"'{v}'" for v in entities["chains"])
        hints.append(f"Сети в базе: retail_chain IN ({vals})")

    if entities["flavors"]:
        vals = ", ".join(f"'{v}'" for v in entities["flavors"])
        hints.append(f"Вкусы в базе: flavor IN ({vals})")

    if entities["formats"]:
        vals = ", ".join(f"'{v}'" for v in entities["formats"])
        hints.append(f"Форматы магазинов: store_format IN ({vals})")

    if entities["chip_types"]:
        vals = ", ".join(f"'{v}'" for v in entities["chip_types"])
        hints.append(f"Типы чипсов: chip_type IN ({vals})")

    if not hints:
        return ""

    return "\n\n=== РАСПОЗНАННЫЕ СУЩНОСТИ ===\n" + "\n".join(hints) + \
           "\n⚠️ ОБЯЗАТЕЛЬНО используй эти значения в WHERE вместо своих вариантов!"


def enrich_question(question: str) -> tuple[str, dict]:
    """
    Обогащает вопрос подсказками о реальных значениях в БД.
    Возвращает (обогащённый_вопрос, entities).
    """
    entities = extract_entities(question)
    hint = build_entities_hint(entities)

    if hint:
        enriched = f"{question}\n{hint}"
    else:
        enriched = question

    return enriched, entities