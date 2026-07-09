"""Разрешение относительных периодов с учётом контекста фильтров."""
import re
from functools import lru_cache
from datetime import datetime

from database import db
from config import config


TABLE = f"{config.CH_DATABASE}.{config.CH_TABLE}"


MONTH_NAMES = {
    1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
    5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
    9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь",
}


@lru_cache(maxsize=1)
def get_data_range_global():
    """Возвращает глобальный диапазон дат (без фильтров)."""
    try:
        df = db.query(f"""
            SELECT 
                min(toDate(date)) AS min_date,
                max(toDate(date)) AS max_date
            FROM {TABLE}
        """)
        if df.empty:
            return None

        min_date = df.iloc[0]["min_date"]
        max_date = df.iloc[0]["max_date"]

        if isinstance(max_date, str):
            max_date = datetime.strptime(max_date, "%Y-%m-%d")

        return {
            "min_date": min_date,
            "max_date": max_date,
            "max_year": max_date.year,
            "max_month_num": max_date.month,
            "max_month_name": MONTH_NAMES[max_date.month],
        }
    except Exception as e:
        print(f"[PERIOD] Failed to get global range: {e}")
        return None


def get_data_range_for_filters(filters: dict) -> dict | None:
    """
    Возвращает диапазон дат С УЧЁТОМ фильтров пользователя.
    Например для Ашана вернёт даты только по Ашану.
    """
    if not filters:
        return get_data_range_global()

    # Собираем WHERE-условия
    conditions = []

    if filters.get("retail_chain"):
        val = str(filters["retail_chain"]).replace("'", "''")
        conditions.append(f"retail_chain = '{val}'")

    if filters.get("store_format"):
        val = str(filters["store_format"]).replace("'", "''")
        conditions.append(f"store_format = '{val}'")

    if filters.get("chip_type"):
        val = str(filters["chip_type"]).replace("'", "''")
        conditions.append(f"chip_type = '{val}'")

    if filters.get("brands"):
        brands = filters["brands"]
        if isinstance(brands, list) and brands:
            vals = ", ".join(f"'{str(b).replace(chr(39), chr(39)*2)}'" for b in brands)
            conditions.append(f"brand IN ({vals})")

    if filters.get("weight_grams"):
        try:
            w = int(filters["weight_grams"])
            conditions.append(f"toInt32(toFloat64OrZero(toString(weight_grams))) = {w}")
        except (ValueError, TypeError):
            pass

    where = ""
    if conditions:
        where = "WHERE " + " AND ".join(conditions)

    try:
        df = db.query(f"""
            SELECT 
                min(toDate(date)) AS min_date,
                max(toDate(date)) AS max_date,
                count() AS row_count
            FROM {TABLE}
            {where}
        """)

        if df.empty or df.iloc[0]["row_count"] == 0:
            return None

        min_date = df.iloc[0]["min_date"]
        max_date = df.iloc[0]["max_date"]

        if max_date is None:
            return None

        if isinstance(max_date, str):
            max_date = datetime.strptime(max_date, "%Y-%m-%d")

        result = {
            "min_date": min_date,
            "max_date": max_date,
            "max_year": max_date.year,
            "max_month_num": max_date.month,
            "max_month_name": MONTH_NAMES[max_date.month],
            "filters_used": {k: v for k, v in filters.items() if v},
        }

        print(f"[PERIOD] Filtered range: {min_date} .. {max_date} (filters: {result['filters_used']})")
        return result

    except Exception as e:
        print(f"[PERIOD] Failed to get filtered range: {e}, fallback to global")
        return get_data_range_global()


# ============================================================
# ПАТТЕРНЫ
# ============================================================

RELATIVE_PERIOD_PATTERNS = [
    (r"\bпоследн\w*\s+(?:период|месяц|отчётн\w+)", "last_month"),
    (r"\bза\s+последн\w*\s+(?:период|месяц)", "last_month"),
    (r"\bсвеж\w*\s+данн", "last_month"),
    (r"\bнедавн\w*", "last_month"),

    (r"\bпрошл\w+\s+месяц", "prev_month"),
    (r"\bпредыдущ\w+\s+месяц", "prev_month"),

    (r"\bпоследн\w*\s+(\d+)\s+месяц", "last_n_months"),
    (r"\bза\s+последн\w*\s+(\d+)\s+месяц", "last_n_months"),

    (r"\bпоследн\w*\s+квартал", "last_quarter"),
    (r"\bза\s+квартал\b", "last_quarter"),
    (r"\b3\s+месяц", "last_3_months"),

    (r"\bполугод", "last_6_months"),
    (r"\b6\s+месяц", "last_6_months"),
    (r"\bпоследн\w*\s+полгода", "last_6_months"),

    (r"\bгод\s*-?\s*к\s*-?\s*году", "yoy"),
    (r"\bгод\s+к\s+году", "yoy"),
    (r"\bпо\s+сравнени\w+\s+с\s+прошл\w+\s+год", "yoy"),

    (r"\bтекущ\w+\s+год", "current_year"),
    (r"\bэтот\s+год", "current_year"),
    (r"\bза\s+этот\s+год", "current_year"),
    (r"\bв\s+этом\s+году", "current_year"),

    (r"\bпрошл\w+\s+год", "prev_year"),
    (r"\bпредыдущ\w+\s+год", "prev_year"),

    (r"\bпозапрошл\w+\s+год", "prev_prev_year"),

    (r"\bза\s+вс[её]\s+время", "all_time"),
    (r"\bвс[её]\s+период", "all_time"),
]


def detect_period_phrase(text: str) -> tuple[str | None, int | None]:
    """Определяет ключевую фразу относительного периода."""
    text_lower = text.lower()

    for pattern, key in RELATIVE_PERIOD_PATTERNS:
        m = re.search(pattern, text_lower)
        if m:
            number = None
            if m.groups():
                try:
                    number = int(m.group(1))
                except (ValueError, IndexError):
                    pass
            return key, number

    return None, None


def resolve_period_from_range(matched_pattern: str, matched_number: int | None,
                                data_range: dict) -> dict:
    """По ключу и диапазону строит результат."""
    max_year = data_range["max_year"]
    max_month_num = data_range["max_month_num"]
    max_month_name = data_range["max_month_name"]

    result = {"detected_phrase": matched_pattern}

    if matched_pattern == "last_month":
        result.update({
            "year": max_year,
            "month": max_month_name,
            "description": f"последний доступный месяц ({max_month_name} {max_year})",
        })

    elif matched_pattern == "prev_month":
        prev_month = max_month_num - 1
        prev_year = max_year
        if prev_month < 1:
            prev_month = 12
            prev_year -= 1
        result.update({
            "year": prev_year,
            "month": MONTH_NAMES[prev_month],
            "description": f"предыдущий месяц ({MONTH_NAMES[prev_month]} {prev_year})",
        })

    elif matched_pattern in ("last_n_months", "last_quarter", "last_3_months", "last_6_months"):
        if matched_pattern in ("last_quarter", "last_3_months"):
            n = 3
        elif matched_pattern == "last_6_months":
            n = 6
        else:
            n = matched_number or 3

        start_month = max_month_num - n + 1
        start_year = max_year
        while start_month < 1:
            start_month += 12
            start_year -= 1

        start_date = f"{start_year}-{start_month:02d}-01"
        result.update({
            "date_from": start_date,
            "description": f"последние {n} месяцев (с {MONTH_NAMES[start_month]} {start_year} по {max_month_name} {max_year})",
            "filter_hint": f"date >= '{start_date}'",
        })

    elif matched_pattern == "current_year":
        result.update({
            "year": max_year,
            "description": f"текущий год ({max_year})",
        })

    elif matched_pattern == "prev_year":
        result.update({
            "year": max_year - 1,
            "description": f"прошлый год ({max_year - 1})",
        })

    elif matched_pattern == "prev_prev_year":
        result.update({
            "year": max_year - 2,
            "description": f"позапрошлый год ({max_year - 2})",
        })

    elif matched_pattern == "yoy":
        result.update({
            "years_list": [max_year - 1, max_year],
            "description": f"сравнение {max_year - 1} vs {max_year}",
        })

    elif matched_pattern == "all_time":
        result.update({
            "description": "за всё доступное время",
        })

    return result


def resolve_relative_period(text: str, filters: dict = None) -> dict | None:
    """
    Определяет относительный период с учётом контекста фильтров.
    Если filters переданы — использует диапазон для этих фильтров.
    """
    matched_pattern, matched_number = detect_period_phrase(text)
    if not matched_pattern:
        return None

    # Определяем диапазон дат
    if filters:
        data_range = get_data_range_for_filters(filters)
        # Если по фильтрам нет данных — падаем на глобальный
        if not data_range:
            print("[PERIOD] No data for filters, using global range")
            data_range = get_data_range_global()
    else:
        data_range = get_data_range_global()

    if not data_range:
        return None

    return resolve_period_from_range(matched_pattern, matched_number, data_range)


def build_period_hint(period_info: dict) -> str:
    """Формирует подсказку для планировщика."""
    if not period_info:
        return ""

    parts = [
        "\n=== РАЗРЕШЁННЫЙ ОТНОСИТЕЛЬНЫЙ ПЕРИОД ===",
        f"Пользователь имел в виду: {period_info['description']}",
    ]

    hints = []
    if "year" in period_info:
        hints.append(f'"year": {period_info["year"]}')
    if "month" in period_info:
        hints.append(f'"month": "{period_info["month"]}"')
    if "years_list" in period_info:
        years_str = ", ".join(str(y) for y in period_info["years_list"])
        hints.append(f'использовать несколько лет: [{years_str}]')

    if hints:
        parts.append("Используй в filters:")
        parts.append("  " + "\n  ".join(hints))

    if "filter_hint" in period_info:
        parts.append(f"Дополнительный SQL-фильтр: {period_info['filter_hint']}")

    parts.append("⚠️ Если пользователь указал конкретную дату — приоритет ей.")

    return "\n".join(parts)