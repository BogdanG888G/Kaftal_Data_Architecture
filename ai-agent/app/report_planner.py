"""Планировщик отчётов — компактный + fallback."""
import json
import re
import hashlib

from llm import llm
from entities import enrich_question, extract_entities
from metrics import enrich_with_metrics, extract_grams_list


# In-memory cache планов
_PLAN_CACHE = {}


def _cache_key(request: str) -> str:
    return hashlib.md5(request.strip().lower().encode()).hexdigest()


# ============================================================
# КОМПАКТНЫЙ ПРОМПТ (в 3 раза короче)
# ============================================================

PLANNER_SYSTEM_PROMPT = """Ты составляешь план отчёта по продажам чипсов.

Верни ТОЛЬКО JSON без markdown:
{
  "filters": {
    "retail_chain": null|"Магнит"|"Пятерочка"|"Дикси"|"Ашан"|"Лента"|"Перекресток"|"Окей"|"Верный"|"Красное и Белое"|"Чижик"|...,
    "store_format": null|"ГМ"|"СМ"|"У",
    "chip_type": null|"Картофельные чипсы"|"Кукурузные чипсы"|"Овощные чипсы",
    "year": null|2024|2025|2026,
    "month": null|"Январь"|"Февраль"|"Март"|"Апрель"|"Май"|"Июнь"|"Июль"|"Август"|"Сентябрь"|"Октябрь"|"Ноябрь"|"Декабрь",
    "weight_grams": null|int,
    "weight_grams_list": null|[70,120,140],
    "brands": null|["Lay's"],
    "flavors": null|["Сметана и лук"],
    "priority_flavors": null|["..."]
  },
  "sections": [
    {"title":"...","question":"...","group_by":[...],"chart_type":"kpi|bar|line|pie|heatmap|table","extra_filter":null|{"flavor":"..."}}
  ]
}

Правила:
- 8-12 секций
- Первая секция всегда KPI (chart_type=kpi, group_by=[])
- group_by: колонки sales_mart (retail_chain, region_name, city_name, brand, flavor, weight_grams, chip_type, year, month, date, week_num, store_format)
- Для приоритетных вкусов — отдельные секции с extra_filter={"flavor":"..."}
- НЕ дублируй фильтры внутри question — они уже в filters
- question должен быть коротким описанием: "Топ регионов", "Динамика по месяцам" и т.п.
"""


PLAN_EXAMPLE = """Пример:
Запрос: "Магнит ГМ 120г картофельные чипсы 2026 по территориям, приоритет Сметана и лук, Морепродукты"
Ответ:
{"filters":{"retail_chain":"Магнит","store_format":"ГМ","chip_type":"Картофельные чипсы","year":2026,"weight_grams":120,"priority_flavors":["Сметана и лук","Морепродукты"]},"sections":[{"title":"KPI","question":"Общие показатели","group_by":[],"chart_type":"kpi"},{"title":"Динамика по месяцам","question":"Выручка по месяцам","group_by":["year","month"],"chart_type":"line"},{"title":"Топ регионов","question":"Топ-25 регионов","group_by":["region_name"],"chart_type":"bar"},{"title":"Все вкусы","question":"Все вкусы","group_by":["flavor"],"chart_type":"bar"},{"title":"Все бренды","question":"Все бренды","group_by":["brand"],"chart_type":"bar"},{"title":"Сметана и лук по регионам","question":"Топ регионов для этого вкуса","group_by":["region_name"],"chart_type":"bar","extra_filter":{"flavor":"Сметана и лук"}},{"title":"Морепродукты по регионам","question":"Топ регионов для этого вкуса","group_by":["region_name"],"chart_type":"bar","extra_filter":{"flavor":"Морепродукты"}},{"title":"Доли вкусов","question":"Топ-10 вкусов","group_by":["flavor"],"chart_type":"pie"}]}
"""


def clean_json(text: str) -> str:
    text = text.strip()
    text = re.sub(r"```json", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```", "", text)
    match = re.search(r"\{[\s\S]+\}", text)
    if match:
        return match.group(0)
    return text.strip()


# ============================================================
# FALLBACK: план из regex + entities (если LLM недоступна)
# ============================================================

def plan_fallback(user_request: str) -> dict:
    """
    Строит базовый план БЕЗ LLM, только на основе распознанных сущностей.
    """
    entities = extract_entities(user_request)
    grams_list = extract_grams_list(user_request)
    year = _extract_year(user_request)
    month = _extract_month(user_request)

    filters = {
        "retail_chain": entities["chains"][0] if entities["chains"] else None,
        "store_format": entities["formats"][0] if entities["formats"] else None,
        "chip_type": entities["chip_types"][0] if entities["chip_types"] else None,
        "year": year,
        "month": month,  # ← новое поле
        "weight_grams": grams_list[0] if len(grams_list) == 1 else None,
        "weight_grams_list": grams_list if len(grams_list) > 1 else None,
        "brands": entities["brands"] or None,
        "flavors": entities["flavors"] or None,
        "priority_flavors": entities["flavors"] if len(entities["flavors"]) >= 2 else None,
    }

    sections = [
        {"title": "Общие KPI", "question": "Общие показатели", "group_by": [], "chart_type": "kpi"},
        {"title": "Динамика по дням" if month else "Динамика по месяцам",
         "question": "Выручка по времени",
         "group_by": ["date"] if month else ["year", "month"],
         "chart_type": "line"},
        {"title": "Топ регионов", "question": "Топ-25 регионов", "group_by": ["region_name"], "chart_type": "bar"},
        {"title": "Топ городов", "question": "Топ-20 городов", "group_by": ["city_name"], "chart_type": "bar"},
        {"title": "Все бренды", "question": "Все бренды", "group_by": ["brand"], "chart_type": "bar"},
        {"title": "Все вкусы", "question": "Все вкусы", "group_by": ["flavor"], "chart_type": "bar"},
        {"title": "Доли брендов", "question": "Топ-10 брендов", "group_by": ["brand"], "chart_type": "pie"},
        {"title": "Доли вкусов", "question": "Топ-10 вкусов", "group_by": ["flavor"], "chart_type": "pie"},
    ]

    if filters.get("priority_flavors"):
        for flav in filters["priority_flavors"]:
            sections.append({
                "title": f"Вкус: {flav} по регионам",
                "question": f"Топ регионов для вкуса {flav}",
                "group_by": ["region_name"],
                "chart_type": "bar",
                "extra_filter": {"flavor": flav},
            })

    if filters.get("weight_grams_list"):
        sections.append({
            "title": "По граммовкам",
            "question": "Выручка по граммовкам",
            "group_by": ["weight_grams"],
            "chart_type": "bar",
        })

    return {
        "filters": filters,
        "sections": sections,
        "planner_model": "fallback (regex)",
    }

def _extract_year(text: str) -> int | None:
    m = re.search(r"\b(202[3-6])\b", text)
    return int(m.group(1)) if m else None


MONTH_MAP = {
    "январ": "Январь",
    "феврал": "Февраль",
    "март": "Март",
    "марта": "Март",
    "марте": "Март",
    "апрел": "Апрель",
    "май": "Май",
    "мая": "Май",
    "мае": "Май",
    "июн": "Июнь",
    "июл": "Июль",
    "август": "Август",
    "сентябр": "Сентябрь",
    "октябр": "Октябрь",
    "ноябр": "Ноябрь",
    "декабр": "Декабрь",
}


def _extract_month(text: str) -> str | None:
    """Ищет месяц в тексте, возвращает капитализированное название."""
    text_lower = text.lower()
    for key, month_name in MONTH_MAP.items():
        # Ищем как отдельное слово (окончание может быть)
        if re.search(rf"\b{key}\w*", text_lower):
            return month_name
    return None


# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def plan_report(user_request: str) -> dict:
    """
    По запросу пользователя генерирует план отчёта.
    1. Проверяет кэш
    2. Пробует LLM
    3. Если LLM недоступна — fallback на regex
    """
    # Кэш
    cache_key = _cache_key(user_request)
    if cache_key in _PLAN_CACHE:
        print(f"[PLANNER] Cache hit for: {user_request[:60]}...")
        return _PLAN_CACHE[cache_key]

    # Обогащаем сущностями для лучшего понимания
    q1, _entities = enrich_question(user_request)
    enriched, _metrics = enrich_with_metrics(q1)

    system = PLANNER_SYSTEM_PROMPT + "\n\n" + PLAN_EXAMPLE
    user_msg = f"Запрос:\n{enriched}"

    # Пробуем LLM
    try:
        raw, model = llm.ask(system, user_msg)
        cleaned = clean_json(raw)
        plan = json.loads(cleaned)

        if not isinstance(plan, dict) or "sections" not in plan:
            raise ValueError("bad structure")

        plan.setdefault("filters", {})
        _validate_sections(plan["sections"])
        plan["planner_model"] = model

        _PLAN_CACHE[cache_key] = plan
        print(f"[PLANNER] LLM plan built with {model}")
        return plan

    except Exception as e:
        # Fallback: строим план без LLM
        print(f"[PLANNER] LLM failed ({e}), using fallback")
        plan = plan_fallback(user_request)
        _PLAN_CACHE[cache_key] = plan
        return plan


def _validate_sections(sections: list):
    valid_types = {"kpi", "bar", "line", "pie", "heatmap", "table"}
    for i, section in enumerate(sections):
        if not isinstance(section, dict):
            raise ValueError(f"section {i} not dict")
        if "title" not in section or "question" not in section:
            raise ValueError(f"section {i} missing title/question")
        if section.get("chart_type") not in valid_types:
            section["chart_type"] = "bar"
        if "group_by" not in section:
            section["group_by"] = []