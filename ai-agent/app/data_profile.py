"""Data profiling — проверяет какие колонки заполнены для текущих фильтров."""
from database import db
from sql_builder import build_where
from config import config


TABLE = f"{config.CH_DATABASE}.{config.CH_TABLE}"


# Колонки для проверки на заполненность
PROFILE_COLUMNS = [
    "region_name",
    "city_name",
    "brand",
    "flavor",
    "manufacturer",
    "vendor",
    "chip_type",
    "package_type",
    "store_format",
    "store_code",
    "address",
    "product_name",
    "product_category_2",
    "product_category_3",
    "weight_grams",
]


def profile_data(filters: dict) -> dict:
    """
    Проверяет процент заполненности каждой колонки для текущих фильтров.
    Возвращает: {
        "row_count": 12345,
        "columns": {
            "region_name": 0.0,      # 0% заполнено -> использовать НЕ надо
            "city_name": 0.85,       # 85% заполнено -> ок
            "brand": 1.0,
            ...
        },
        "available": ["city_name", "brand", ...],  # колонки >= 30% заполнены
        "empty": ["region_name", ...],              # колонки < 5% заполнены
    }
    """
    where = build_where(filters)

    # Собираем один запрос для всех колонок
    checks = []
    for col in PROFILE_COLUMNS:
        checks.append(
            f"round(sum(case when {col} is not null and toString({col}) != '' "
            f"and toString({col}) != 'ᴺᵁᴸᴸ' and toString({col}) != 'Не указано' "
            f"then 1 else 0 end) / count(), 3) AS {col}_pct"
        )

    checks_sql = ",\n    ".join(checks)

    sql = f"""SELECT
    count() AS row_count,
    {checks_sql}
FROM {TABLE}
{where}
LIMIT 1"""

    try:
        df = db.query(sql)
        if df.empty:
            return {"row_count": 0, "columns": {}, "available": [], "empty": []}

        row = df.iloc[0]
        row_count = int(row["row_count"])

        columns = {}
        for col in PROFILE_COLUMNS:
            pct = float(row[f"{col}_pct"] or 0)
            columns[col] = pct

        # Классификация
        available = [c for c, pct in columns.items() if pct >= 0.30]
        empty = [c for c, pct in columns.items() if pct < 0.05]

        return {
            "row_count": row_count,
            "columns": columns,
            "available": available,
            "empty": empty,
        }

    except Exception as e:
        print(f"[PROFILE] Failed: {e}")
        return {"row_count": 0, "columns": {}, "available": PROFILE_COLUMNS, "empty": []}


def filter_sections_by_profile(sections: list, profile: dict) -> list:
    """
    Убирает секции, которые ссылаются на пустые колонки.
    """
    empty = set(profile.get("empty", []))
    if not empty:
        return sections

    filtered = []
    for section in sections:
        group_by = section.get("group_by") or []
        # Проверяем: если хоть одна из group_by колонок пустая — секцию убираем
        if any(col in empty for col in group_by):
            print(f"[PROFILE] Skipping section '{section['title']}' — empty column {[c for c in group_by if c in empty]}")
            continue
        filtered.append(section)

    return filtered


def build_profile_hint(profile: dict) -> str:
    """Формирует подсказку для LLM о наличии данных."""
    if not profile.get("columns"):
        return ""

    lines = [f"\n=== ДОСТУПНОСТЬ ДАННЫХ (для этих фильтров) ==="]
    lines.append(f"Строк в БД: {profile['row_count']:,}")

    available = profile.get("available", [])
    empty = profile.get("empty", [])

    if available:
        lines.append(f"✅ Заполненные колонки (можно использовать): {', '.join(available)}")
    if empty:
        lines.append(f"❌ ПУСТЫЕ колонки (НЕ используй в group_by): {', '.join(empty)}")

    return "\n".join(lines)