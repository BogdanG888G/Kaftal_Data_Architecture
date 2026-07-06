"""Библиотека бизнес-метрик с формулами."""
import re


TARGET_GRAMS_DEFAULT = 120  # база для нормализации


BUSINESS_METRICS = {
    # === БАЗОВЫЕ ===
    "revenue": {
        "name": "Выручка",
        "synonyms": ["выручка", "оборот", "продажи", "товарооборот", "revenue"],
        "formula": "SUM(sales_amount_rub)",
        "description": "Сумма продаж в рублях",
        "format": "₽",
    },
    "qty": {
        "name": "Количество проданных единиц",
        "synonyms": ["количество", "штук", "штуки", "шт", "единиц", "проданных", "qty"],
        "formula": "SUM(sales_quantity)",
        "description": "Количество проданных упаковок (штук)",
        "format": "шт",
    },
    "cost": {
        "name": "Себестоимость",
        "synonyms": ["себестоимость", "себест", "закупка", "закупочная", "cost", "входная цена"],
        "formula": "SUM(sales_cost_price)",
        "description": "Общая себестоимость проданного товара",
        "format": "₽",
    },
    "margin_rub": {
        "name": "Маржа в рублях",
        "synonyms": ["маржа", "прибыль", "margin"],
        "formula": "SUM(coalesce(margin_rub, sales_amount_rub - sales_cost_price))",
        "description": "Маржа в рублях. Если поле margin_rub NULL, считается как выручка - себестоимость.",
        "format": "₽",
    },
    "margin_pct": {
        "name": "Маржинальность %",
        "synonyms": ["маржинальность", "маржа %", "маржа в процентах", "margin %"],
        "formula": "round(SUM(coalesce(margin_rub, sales_amount_rub - sales_cost_price)) / NULLIF(SUM(sales_amount_rub), 0) * 100, 2)",
        "description": "Маржинальность в процентах",
        "format": "%",
    },

    # === ЦЕНЫ ===
    "avg_sell_price": {
        "name": "Средняя цена продажи",
        "synonyms": ["средняя цена", "цена продажи", "средний чек", "avg price", "avg_price"],
        "formula": "round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2)",
        "description": "Средневзвешенная цена продажи одной единицы",
        "format": "₽",
    },
    "avg_cost_price": {
        "name": "Средняя себестоимость",
        "synonyms": ["средняя себестоимость", "средняя закупка", "ср себест"],
        "formula": "round(SUM(sales_cost_price) / NULLIF(SUM(sales_quantity), 0), 2)",
        "description": "Средневзвешенная себестоимость одной единицы",
        "format": "₽",
    },

    # === ЦЕНА ЗА ГРАММ ===
    "price_per_gram": {
        "name": "Цена за 1 грамм",
        "synonyms": [
            "цена за грамм", "цена за 1 грамм", "рубль за грамм", "₽/г",
            "стоимость грамма", "сколько стоит 1 грамм", "сколько стоит грамм",
            "price per gram", "ppg",
        ],
        "formula": """round(
    SUM(sales_amount_rub)
    / NULLIF(SUM(sales_quantity), 0)
    / NULLIF(toFloat64OrZero(toString(weight_grams)), 0),
    3
)""",
        "description": "Средняя цена за 1 грамм товара. Формула: (выручка / штуки) / вес в граммах.",
        "requires": ["weight_grams"],
        "format": "₽/г",
    },
    "cost_per_gram": {
        "name": "Себестоимость за 1 грамм",
        "synonyms": ["себест за грамм", "закупка за грамм", "себестоимость за 1 грамм"],
        "formula": """round(
    SUM(sales_cost_price)
    / NULLIF(SUM(sales_quantity), 0)
    / NULLIF(toFloat64OrZero(toString(weight_grams)), 0),
    3
)""",
        "description": "Средняя себестоимость 1 грамма",
        "requires": ["weight_grams"],
        "format": "₽/г",
    },

    # === НОРМАЛИЗАЦИЯ К ЦЕЛЕВОЙ ГРАММОВКЕ ===
    "price_normalized": {
        "name": "Нормализованная цена продажи",
        "synonyms": [
            "нормализованная цена", "нормализация", "пересчитать на",
            "пересчёт", "приведённая цена", "цена в пересчёте",
            "как бы цена", "эквивалентная цена", "если бы весило",
            "нормализовать к", "приведи к",
        ],
        "formula_template": """round(
    (SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0))
    / NULLIF(toFloat64OrZero(toString(weight_grams)), 0)
    * {target_grams},
    2
)""",
        "description": (
            "Нормализованная цена: сколько бы стоила упаковка, если бы её вес "
            "был равен {target_grams} грамм. Формула: цена_за_упаковку / вес * {target_grams}. "
            "Позволяет сравнивать товары разной фасовки на равных."
        ),
        "requires": ["weight_grams"],
        "format": "₽",
        "requires_target": True,
    },
    "cost_normalized": {
        "name": "Нормализованная себестоимость",
        "synonyms": ["нормализованная себестоимость", "себест нормализованная"],
        "formula_template": """round(
    (SUM(sales_cost_price) / NULLIF(SUM(sales_quantity), 0))
    / NULLIF(toFloat64OrZero(toString(weight_grams)), 0)
    * {target_grams},
    2
)""",
        "description": "Себестоимость, приведённая к весу {target_grams}г для сравнения.",
        "requires": ["weight_grams"],
        "format": "₽",
        "requires_target": True,
    },

    # === КОЛИЧЕСТВЕННЫЕ ===
    "tt_count": {
        "name": "Количество торговых точек",
        "synonyms": [
            "количество тт", "кол-во тт", "число тт", "тт", "торговые точки",
            "уникальные адреса", "магазины уникальные", "адресов",
        ],
        "formula": "COUNT(DISTINCT address)",
        "description": "Количество уникальных торговых точек (адресов)",
        "format": "ТТ",
    },
    "stores_count": {
        "name": "Количество магазинов",
        "synonyms": ["количество магазинов", "число магазинов", "магазины"],
        "formula": "COUNT(DISTINCT store_code)",
        "description": "Количество уникальных магазинов (по store_code)",
        "format": "маг",
    },
    "brands_count": {
        "name": "Количество брендов",
        "synonyms": ["количество брендов", "число брендов", "брендов"],
        "formula": "COUNT(DISTINCT brand)",
        "description": "Количество уникальных брендов",
        "format": "шт",
    },
    "products_count": {
        "name": "Количество SKU",
        "synonyms": ["sku", "товары", "количество товаров", "число товаров"],
        "formula": "COUNT(DISTINCT product_id)",
        "description": "Количество уникальных товаров (SKU)",
        "format": "шт",
    },

    # === ПРОМО ===
    "promo_revenue": {
        "name": "Промо-продажи",
        "synonyms": ["промо", "по акции", "акции", "promo", "промо продажи"],
        "formula": "SUM(coalesce(promo_sales_rub, 0))",
        "description": "Продажи по промо-акциям",
        "format": "₽",
    },
    "promo_share": {
        "name": "Доля промо в продажах",
        "synonyms": ["доля промо", "процент промо", "% промо", "промо доля"],
        "formula": "round(SUM(coalesce(promo_sales_rub, 0)) / NULLIF(SUM(sales_amount_rub), 0) * 100, 2)",
        "description": "Какой процент от выручки приходится на промо",
        "format": "%",
    },
}


# ============================================================
# Извлечение метрик из вопроса
# ============================================================

def extract_target_grams(question: str) -> int | None:
    """
    Ищет в вопросе целевую граммовку для нормализации.
    """
    patterns = [
        r"нормализ\w*\s+(?:к|на)\s*(\d{2,4})\s*(?:г|гр|грамм)",
        r"приведи\s+к\s*(\d{2,4})\s*(?:г|гр|грамм)",
        r"пересчита\w+\s+(?:к|на)\s*(\d{2,4})\s*(?:г|гр|грамм)",
        r"как\s+бы\s+(\d{2,4})\s*(?:г|гр|грамм)",
        r"эквивалент\w*\s+(\d{2,4})\s*(?:г|гр|грамм)",
        r"за\s+(\d{2,4})\s*(?:г|гр|грамм)",
    ]

    for pat in patterns:
        m = re.search(pat, question.lower())
        if m:
            return int(m.group(1))
    return None


def extract_grams_list(question: str) -> list[int]:
    """
    Ищет упомянутые граммовки.
    """
    result = set()

    # Список чисел с одним "грамм" в конце
    m = re.search(r"([\d,\s]+)\s*(?:г\b|гр\b|грамм)", question.lower())
    if m:
        nums_str = m.group(1)
        for n in re.findall(r"\d+", nums_str):
            n_int = int(n)
            if 10 <= n_int <= 2000:
                result.add(n_int)

    # Одиночные числа с "г"
    for m in re.finditer(r"\b(\d{2,4})\s*(?:г\b|гр\b|грамм)", question.lower()):
        n_int = int(m.group(1))
        if 10 <= n_int <= 2000:
            result.add(n_int)

    return sorted(result)


def extract_metrics(question: str) -> list[dict]:
    """
    Из вопроса пользователя извлекает упомянутые метрики.
    """
    q_lower = question.lower()
    found_keys = []

    for key, meta in BUSINESS_METRICS.items():
        for syn in meta.get("synonyms", []):
            if syn.lower() in q_lower:
                if key not in found_keys:
                    found_keys.append(key)
                break

    target_grams = extract_target_grams(question) or TARGET_GRAMS_DEFAULT
    result = []

    for key in found_keys:
        meta = BUSINESS_METRICS[key]

        if meta.get("requires_target"):
            formula = meta["formula_template"].format(target_grams=target_grams)
            desc = meta["description"].format(target_grams=target_grams)
        else:
            formula = meta.get("formula", "")
            desc = meta.get("description", "")

        result.append({
            "key": key,
            "name": meta["name"],
            "formula": formula.strip(),
            "description": desc,
            "format": meta.get("format", ""),
            "target_grams": target_grams if meta.get("requires_target") else None,
        })

    return result


def build_metrics_hint(metrics: list[dict], grams_list: list[int] = None) -> str:
    """Формирует блок для промпта с формулами найденных метрик."""
    if not metrics and not grams_list:
        return ""

    parts = ["=== РАСПОЗНАННЫЕ БИЗНЕС-МЕТРИКИ ==="]

    for m in metrics:
        parts.append(f"\n📊 {m['name']} ({m['key']}):")
        parts.append(f"   Формула:\n   {m['formula']}")
        parts.append(f"   Описание: {m['description']}")

    if grams_list:
        grams_str = ", ".join(str(g) for g in grams_list)
        parts.append(
            f"\n⚙️ УПОМЯНУТЫЕ ГРАММОВКИ: {grams_str}\n"
            f"   Используй фильтр: toInt32(toFloat64OrZero(toString(weight_grams))) IN ({grams_str})"
        )

    parts.append(
        "\n⚠️ ВАЖНО: используй формулы ИМЕННО так, как указано выше. "
        "Не изобретай свои варианты. "
        "Для weight_grams ВСЕГДА используй toFloat64OrZero(toString(weight_grams)) — "
        "тип колонки может быть Float64 или String."
    )

    return "\n".join(parts)


def enrich_with_metrics(question: str) -> tuple[str, dict]:
    """
    Обогащает вопрос информацией о найденных бизнес-метриках.
    """
    metrics = extract_metrics(question)
    grams_list = extract_grams_list(question)
    target_grams = extract_target_grams(question)

    hint = build_metrics_hint(metrics, grams_list)

    if hint:
        enriched = f"{question}\n\n{hint}"
    else:
        enriched = question

    return enriched, {
        "metrics": metrics,
        "grams_list": grams_list,
        "target_grams": target_grams,
    }