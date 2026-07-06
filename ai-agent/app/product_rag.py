"""RAG над product_mapping — справочник товаров."""
from functools import lru_cache
from database import db


@lru_cache(maxsize=1)
def load_product_mapping():
    """Загружает таблицу product_mapping в память."""
    try:
        df = db.query("""
            SELECT
                original_name       AS product_name,
                brand_manual        AS brand,
                chip_type_manual    AS chip_type,
                package_manual      AS package_type,
                flavor_manual       AS flavor,
                weight_manual       AS weight_grams
            FROM default.product_mapping
            WHERE brand_manual != 'Не чипсы'
              AND brand_manual != ''
        """)
        print(f"[PRODUCT_RAG] Loaded {len(df)} products from mapping")
        return df
    except Exception as e:
        print(f"[PRODUCT_RAG] Failed to load: {e}")
        return None


# ============================================================
# ПОИСК ПО СПРАВОЧНИКУ
# ============================================================

def search_products(
    brand: str = None,
    flavor_keyword: str = None,
    chip_type: str = None,
    weight: int = None,
    limit: int = 20,
) -> list:
    """Ищет товары по критериям."""
    df = load_product_mapping()
    if df is None or df.empty:
        return []

    result = df.copy()

    if brand:
        result = result[result["brand"].str.lower() == brand.lower()]
    if flavor_keyword:
        result = result[result["flavor"].str.contains(flavor_keyword, case=False, na=False)]
    if chip_type:
        result = result[result["chip_type"].str.contains(chip_type, case=False, na=False)]
    if weight:
        result = result[result["weight_grams"] == weight]

    if result.empty:
        return []
    return result.head(limit).to_dict("records")


def get_flavors_for_brand(brand: str) -> list:
    """Уникальные вкусы для бренда."""
    df = load_product_mapping()
    if df is None:
        return []
    sub = df[df["brand"].str.lower() == brand.lower()]
    return sorted([f for f in sub["flavor"].dropna().unique().tolist() if f])


def get_brands_with_flavor(flavor_keyword: str) -> list:
    """Бренды у которых есть указанный вкус."""
    df = load_product_mapping()
    if df is None:
        return []
    sub = df[df["flavor"].str.contains(flavor_keyword, case=False, na=False)]
    return sorted([b for b in sub["brand"].dropna().unique().tolist() if b])


def get_all_flavors() -> list:
    df = load_product_mapping()
    if df is None:
        return []
    return sorted([f for f in df["flavor"].dropna().unique().tolist() if f])


def get_all_brands() -> list:
    df = load_product_mapping()
    if df is None:
        return []
    return sorted([b for b in df["brand"].dropna().unique().tolist() if b])


def get_all_chip_types() -> list:
    df = load_product_mapping()
    if df is None:
        return []
    return sorted([c for c in df["chip_type"].dropna().unique().tolist() if c])


def get_all_weights() -> list:
    df = load_product_mapping()
    if df is None:
        return []
    weights = df["weight_grams"].dropna().unique().tolist()
    return sorted([int(w) for w in weights if w and w > 0])


# ============================================================
# УМНЫЙ ПОИСК ВКУСОВ ПО СИНОНИМАМ
# ============================================================

# Синонимы для вкусов
FLAVOR_ALIASES = {
    "краб": ["Краб", "Камчатский краб", "Крабовый", "Крабовые палочки"],
    "камчатский краб": ["Камчатский краб"],
    "сметана и лук": ["Сметана и лук", "Сметана-лук", "Сметана+лук"],
    "сметана": ["Сметана", "Сметана и лук"],
    "лук": ["Лук", "Сметана и лук", "Зелёный лук"],
    "кетчуп": ["Кетчуп", "Томат", "Кетчуп/томат", "Томат/кетчуп"],
    "томат": ["Томат", "Кетчуп", "Помидор"],
    "паприка": ["Паприка", "Красная паприка", "Сладкая паприка"],
    "сыр": ["Сыр", "Сырный", "Чеддер", "Пармезан", "Четыре сыра"],
    "чеддер": ["Чеддер", "Сыр чеддер"],
    "бекон": ["Бекон", "Копчёный бекон"],
    "соль": ["Соль", "Морская соль", "Соленые"],
    "чеснок": ["Чеснок", "Чесночный"],
    "грибы": ["Грибы", "Белые грибы", "Шампиньоны"],
    "стейк": ["Стейк", "Мясной", "Барбекю"],
    "барбекю": ["Барбекю", "BBQ"],
    "лосось": ["Лосось", "Морепродукты"],
    "морепродукты": ["Морепродукты", "Лосось", "Краб"],
}


def find_flavors_in_mapping(user_query: str) -> list:
    """
    Ищет реальные вкусы в mapping по свободному тексту пользователя.
    """
    q_lower = user_query.lower()
    all_flavors = get_all_flavors()

    if not all_flavors:
        return []

    # === СТРАТЕГИЯ 1: точные фразы из синонимов ===
    # Если в запросе есть точная фраза-синоним, ищем только её матчи
    for phrase, synonyms in FLAVOR_ALIASES.items():
        if phrase in q_lower:
            found = set()
            for syn in synonyms:
                # Точное совпадение или синоним является подстрокой вкуса
                for flavor in all_flavors:
                    if syn.lower() == flavor.lower() or syn.lower() in flavor.lower():
                        # Дополнительная проверка: не хотим "Лук и сыр" когда искали "лук"
                        # Разрешаем только если синоним занимает > 50% длины вкуса
                        if len(syn) >= len(flavor) * 0.5:
                            found.add(flavor)
            if found:
                return sorted(found)

    # === СТРАТЕГИЯ 2: одиночные ключевые слова ===
    words = [w for w in q_lower.split() if len(w) >= 4]
    found = set()
    for word in words:
        # Слово должно быть достаточно точным маркером
        for flavor in all_flavors:
            flavor_lower = flavor.lower()
            # Слово находится в начале или как отдельное слово
            if flavor_lower.startswith(word) or f" {word}" in flavor_lower or f"{word} " in flavor_lower:
                found.add(flavor)

    # Ограничиваем результат — берём топ-10 самых коротких (обычно самые точные)
    result = sorted(found, key=lambda x: (len(x), x))[:10]
    return result


# ============================================================
# КОНТЕКСТ ДЛЯ LLM
# ============================================================

def build_context_for_llm(brands: list = None, flavors: list = None, max_items: int = 20) -> str:
    """
    Строит текстовый контекст с реальными комбинациями бренд + вкус + граммовка.
    """
    df = load_product_mapping()
    if df is None or df.empty:
        return ""

    result = df.copy()

    if brands:
        brands_lower = [b.lower() for b in brands]
        result = result[result["brand"].str.lower().isin(brands_lower)]

    if flavors:
        flavor_mask = None
        for f in flavors:
            m = result["flavor"].str.contains(f, case=False, na=False)
            flavor_mask = m if flavor_mask is None else (flavor_mask | m)
        if flavor_mask is not None:
            result = result[flavor_mask]

    if result.empty:
        return ""

    result = result.head(max_items)

    lines = ["\n=== СПРАВОЧНИК ТОВАРОВ (реальные комбинации) ==="]
    for _, row in result.iterrows():
        parts = []
        if row.get("brand"):
            parts.append(f"бренд='{row['brand']}'")
        if row.get("flavor"):
            parts.append(f"вкус='{row['flavor']}'")
        if row.get("weight_grams") and row["weight_grams"] > 0:
            parts.append(f"{int(row['weight_grams'])}г")
        if row.get("chip_type"):
            parts.append(f"тип='{row['chip_type']}'")
        if row.get("package_type"):
            parts.append(f"упак='{row['package_type']}'")
        lines.append(f"  • {' · '.join(parts)}")

    return "\n".join(lines)


def build_flavors_hint_for_query(user_query: str, brand: str = None) -> str:
    """
    Если в запросе упомянуты вкусы — возвращает реальные названия из mapping.
    """
    found = find_flavors_in_mapping(user_query)

    if not found:
        return ""

    # Если есть бренд, фильтруем только его вкусы
    if brand:
        brand_flavors = set(get_flavors_for_brand(brand))
        found = [f for f in found if f in brand_flavors]

    if not found:
        return ""

    vals = ", ".join(f"'{f}'" for f in found[:15])
    return f"\n=== РЕАЛЬНЫЕ ВКУСЫ В СПРАВОЧНИКЕ (используй ИМЕННО эти) ===\nflavor IN ({vals})\n"