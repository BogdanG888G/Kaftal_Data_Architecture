"""Кэш уникальных значений колонок из ClickHouse."""
from functools import lru_cache

from database import db
from config import config


TABLE = f"{config.CH_DATABASE}.{config.CH_TABLE}"


# Колонки для которых загружаем топ значений
CATEGORICAL_COLUMNS = {
    "retail_chain": 25,
    "store_format": 10,
    "chip_type": 15,
    "package_type": 10,
    "product_category_2": 15,
    "product_category_3": 15,
}


@lru_cache(maxsize=1)
def load_column_values() -> dict:
    """
    Загружает уникальные значения для категориальных колонок.
    Возвращает: {column_name: [value1, value2, ...]}
    """
    result = {}

    for col, limit in CATEGORICAL_COLUMNS.items():
        try:
            df = db.query(f"""
                SELECT {col} AS val, count() AS cnt
                FROM {TABLE}
                WHERE {col} IS NOT NULL
                  AND toString({col}) != ''
                  AND toString({col}) != 'ᴺᵁᴸᴸ'
                GROUP BY {col}
                ORDER BY cnt DESC
                LIMIT {limit}
            """)

            values = df["val"].dropna().tolist()
            result[col] = [str(v) for v in values if v]
            print(f"[COLUMN_VALUES] Loaded {len(result[col])} values for {col}")

        except Exception as e:
            print(f"[COLUMN_VALUES] Failed for {col}: {e}")
            result[col] = []

    return result


def build_column_values_hint() -> str:
    """Формирует блок для промпта с реальными значениями."""
    values = load_column_values()

    if not values:
        return ""

    lines = ["\n=== РЕАЛЬНЫЕ ЗНАЧЕНИЯ КОЛОНОК (используй ТОЛЬКО их) ==="]

    for col, vals in values.items():
        if not vals:
            continue
        vals_str = ", ".join(f"'{v}'" for v in vals[:15])
        lines.append(f"\n{col}: {vals_str}")

    lines.append(
        "\n⚠️ ВАЖНО: используй ТОЛЬКО значения из этого списка. "
        "Если пользователь написал 'гипермаркет' — используй 'ГМ', не 'Гипермаркет'. "
        "Если 'у дома' — используй 'У'."
    )

    return "\n".join(lines)


# Дополнительные подсказки — маппинг разговорных слов
CONVERSATIONAL_MAPPING = {
    "гипермаркет": "ГМ",
    "гипер": "ГМ",
    "супермаркет": "СМ",
    "супер": "СМ",
    "у дома": "У",
    "дискаунтер": "Дискаунтер",
}


def build_conversational_hint() -> str:
    """Подсказка для маппинга разговорных слов."""
    lines = ["\n=== МАППИНГ РАЗГОВОРНЫХ СЛОВ ==="]
    for word, value in CONVERSATIONAL_MAPPING.items():
        lines.append(f"'{word}' → '{value}'")
    return "\n".join(lines)