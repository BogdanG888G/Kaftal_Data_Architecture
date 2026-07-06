"""Планировщик отчётов — компактный + fallback + product RAG."""
import json
import re
import hashlib

from llm import llm
from entities import (
    enrich_question,
    extract_entities,
    _has_marker,
    _has_flavor_keyword,
    FLAVOR_MARKERS,
)
from metrics import enrich_with_metrics, extract_grams_list
from product_rag import build_context_for_llm, build_flavors_hint_for_query


# In-memory cache планов
_PLAN_CACHE = {}


def _cache_key(request: str) -> str:
    return hashlib.md5(request.strip().lower().encode()).hexdigest()


# ============================================================
# КОМПАКТНЫЙ ПРОМПТ
# ============================================================

PLANNER_SYSTEM_PROMPT = """Ты составляешь план отчёта по продажам чипсов.

Верни ТОЛЬКО JSON без markdown-обёрток:
{
  "filters": {
    "retail_chain": null или "Магнит"|"Пятерочка"|"Дикси"|"Ашан"|"Лента"|"Перекресток"|"Окей"|"Верный"|"Красное и Белое"|"Чижик"|"Глобус"|"Самокат"|"Бристоль"|"ВкусВилл"|...,
    "store_format": null или "ГМ"|"СМ"|"У"|"Дискаунтер",
    "chip_type": null или "Картофельные чипсы"|"Кукурузные чипсы"|"Овощные чипсы"|"Пшеничные чипсы",
    "year": null или 2024|2025|2026,
    "month": null или "Январь"|"Февраль"|"Март"|"Апрель"|"Май"|"Июнь"|"Июль"|"Август"|"Сентябрь"|"Октябрь"|"Ноябрь"|"Декабрь",
    "weight_grams": null или int,
    "weight_grams_list": null или [70,120,140],
    "brands": null или ["Lay's"],
    "flavors": null или ["Сметана и лук"],
    "priority_flavors": null или ["..."]
  },
  "sections": [
    {"title":"...","question":"...","group_by":[...],"chart_type":"kpi|bar|line|pie|lollipop|grouped_bar","extra_filter":null или {"flavor":"..."}}
  ]
}

ПРАВИЛА СЕКЦИЙ:
- 8-12 секций
- Первая секция ВСЕГДА KPI (chart_type="kpi", group_by=[])
- Вторая секция ВСЕГДА динамика по времени (chart_type="line", group_by=["year","month"] или ["date"] если задан month)

ТИПЫ ГРАФИКОВ (chart_type):
- "kpi" — только для первой обзорной секции
- "line" — динамика по времени (автоматически разделится на 3 секции: выручка/количество/цена)
- "bar" — топ регионов, городов, брендов, вкусов, производителей
- "lollipop" — ОБЯЗАТЕЛЬНО для секций "Топ SKU" / "Топ товаров" (group_by содержит product_name)
- "grouped_bar" — только для сравнения цены vs себестоимости (group_by=["brand"])
- "pie" — доли (топ-10 брендов, топ-10 вкусов)

ЗАПРЕЩЕНО:
- НЕ используй chart_type "heatmap" (убран)
- НЕ используй chart_type "table"
- НЕ добавляй секции по chip_type (тип чипсов)
- НЕ добавляй секции по weight_grams если пользователь про них не спрашивал
- НЕ добавляй разбивки по неделям (week_num)
- НЕ дублируй фильтры внутри question (они уже в filters)
- НЕ придумывай вкусы, которых нет в запросе пользователя

КОЛОНКИ для group_by:
retail_chain, region_name, city_name, store_code, store_format,
brand, flavor, product_name, weight_grams, manufacturer,
year, month, date

ПРИОРИТЕТНЫЕ РАЗРЕЗЫ (в такой последовательности):
1. KPI (обзор)
2. Динамика по месяцам (line)
3. Топ регионов (если данные заполнены)
4. Топ городов (если данные заполнены)
5. Топ брендов
6. Топ вкусов
7. Топ-20 SKU (chart_type="lollipop", group_by=["brand","product_name"])
8. Цена vs себестоимость (chart_type="grouped_bar", group_by=["brand"])
9. Доли брендов (pie)
10. Доли вкусов (pie)
11. Топ производителей (если данные заполнены)
12. Для каждого priority_flavor — отдельная секция с extra_filter
"""


PLAN_EXAMPLE = """Пример:
Запрос: "Магнит ГМ 120г картофельные чипсы 2026 по территориям, приоритет Сметана и лук, Морепродукты"

Ответ (валидный JSON):
{"filters":{"retail_chain":"Магнит","store_format":"ГМ","chip_type":"Картофельные чипсы","year":2026,"weight_grams":120,"priority_flavors":["Сметана и лук","Морепродукты"]},"sections":[{"title":"KPI","question":"Общие показатели","group_by":[],"chart_type":"kpi"},{"title":"Динамика по месяцам","question":"Выручка, количество и средняя цена по месяцам","group_by":["year","month"],"chart_type":"line"},{"title":"Топ регионов","question":"Топ-25 регионов","group_by":["region_name"],"chart_type":"bar"},{"title":"Топ городов","question":"Топ-20 городов","group_by":["city_name"],"chart_type":"bar"},{"title":"Топ брендов","question":"Все бренды","group_by":["brand"],"chart_type":"bar"},{"title":"Топ вкусов","question":"Все вкусы","group_by":["flavor"],"chart_type":"bar"},{"title":"Топ-20 SKU","question":"Топ-20 товаров","group_by":["brand","product_name"],"chart_type":"lollipop"},{"title":"Цена vs себестоимость","question":"Цена продажи и себестоимость по брендам","group_by":["brand"],"chart_type":"grouped_bar"},{"title":"Доли брендов","question":"Топ-10 брендов","group_by":["brand"],"chart_type":"pie"},{"title":"Доли вкусов","question":"Топ-10 вкусов","group_by":["flavor"],"chart_type":"pie"},{"title":"Сметана и лук по регионам","question":"Топ регионов для этого вкуса","group_by":["region_name"],"chart_type":"bar","extra_filter":{"flavor":"Сметана и лук"}},{"title":"Морепродукты по регионам","question":"Топ регионов для этого вкуса","group_by":["region_name"],"chart_type":"bar","extra_filter":{"flavor":"Морепродукты"}}]}
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
# FALLBACK: план из regex + entities
# ============================================================

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
    """Ищет месяц в тексте."""
    text_lower = text.lower()
    for key, month_name in MONTH_MAP.items():
        if re.search(rf"\b{key}\w*", text_lower):
            return month_name
    return None


# ============================================================
# ИЗВЛЕЧЕНИЕ ПОЖЕЛАНИЙ ПО СТРУКТУРЕ EXCEL
# ============================================================

EXCEL_COLUMN_KEYWORDS = {
    "retail_chain": ["сеть", "сети", "ритейлер"],
    "region_name": ["регион", "область", "территори"],
    "city_name": ["город"],
    "store_format": ["формат магазина", "формат"],
    "brand": ["бренд", "брендам", "brand"],
    "flavor": ["вкус", "вкусам", "flavor"],
    "weight_grams": ["граммовка", "грамм", "вес"],
    "manufacturer": ["производитель", "manufacturer"],
    "vendor": ["поставщик", "vendor"],
    "product_name": ["товар", "продукт", "sku", "продукт", "название товара"],
    "package_type": ["упаковка", "package"],
    "store_code": ["код магазина"],
    "address": ["адрес", "торговая точка", "тт"],
    "chip_type": ["тип чипсов"],
}


def extract_excel_hierarchy(user_request: str) -> list:
    """
    Извлекает пожелания пользователя по структуре Excel-отчёта.
    Работает только если в запросе есть явная фраза-триггер:
    "excel-отчёт по", "excel по", "выгрузка по", "таблица по" и т.п.
    """
    q_lower = user_request.lower()

    # Триггеры того что пользователь описывает структуру Excel
    triggers = [
        r"excel[-\s]*отч[её]т\s*по[:\s]",
        r"excel\s*по[:\s]",
        r"выгрузк[аиу]\s*по[:\s]",
        r"таблиц[аеу]\s*по[:\s]",
        r"отч[её]т\s*по\s*(?:колонкам|полям)[:\s]",
        r"структура[:\s]",
        r"колонки[:\s]",
    ]

    trigger_pos = -1
    for pattern in triggers:
        m = re.search(pattern, q_lower)
        if m:
            trigger_pos = m.end()
            break

    if trigger_pos == -1:
        return []  # Нет триггера → пользователь не указывал структуру

    # Берём часть текста ПОСЛЕ триггера
    tail = q_lower[trigger_pos:]

    # Ищем ключевые слова колонок в порядке появления в этом хвосте
    found = []

    for col_key, keywords in EXCEL_COLUMN_KEYWORDS.items():
        best_pos = -1
        for kw in keywords:
            # Только целые слова, чтобы "грамм" не срабатывал в "программе"
            m = re.search(rf"\b{re.escape(kw)}", tail)
            if m:
                pos = m.start()
                if best_pos == -1 or pos < best_pos:
                    best_pos = pos

        if best_pos != -1:
            found.append((best_pos, col_key))

    # Сортируем по позиции
    found.sort(key=lambda x: x[0])
    return [col for _, col in found]


def plan_fallback(user_request: str) -> dict:
    """Строит базовый план БЕЗ LLM."""
    entities = extract_entities(user_request)
    grams_list = extract_grams_list(user_request)
    year = _extract_year(user_request)
    month = _extract_month(user_request)

    # Проверяем есть ли явное упоминание вкусов
    has_flavor_context = (
        _has_marker(user_request, FLAVOR_MARKERS)
        or _has_flavor_keyword(user_request)
    )

    # Вкусы берём только если пользователь их упомянул
    flavors = entities["flavors"] if has_flavor_context else None

    filters = {
        "retail_chain": entities["chains"][0] if entities["chains"] else None,
        "store_format": entities["formats"][0] if entities["formats"] else None,
        "chip_type": entities["chip_types"][0] if entities["chip_types"] else None,
        "year": year,
        "month": month,
        "weight_grams": grams_list[0] if len(grams_list) == 1 else None,
        "weight_grams_list": grams_list if len(grams_list) > 1 else None,
        "brands": entities["brands"] or None,
        "flavors": flavors,
        "priority_flavors": flavors if flavors and len(flavors) >= 2 else None,
    }

    sections = [
        {"title": "KPI обзор", "question": "Общие показатели",
         "group_by": [], "chart_type": "kpi"},

        {"title": "Динамика по дням" if month else "Динамика по месяцам",
         "question": "Выручка, количество и средняя цена по времени",
         "group_by": ["date"] if month else ["year", "month"],
         "chart_type": "line"},

        {"title": "Топ регионов", "question": "Топ-25 регионов",
         "group_by": ["region_name"], "chart_type": "bar"},

        {"title": "Топ городов", "question": "Топ-20 городов",
         "group_by": ["city_name"], "chart_type": "bar"},

        {"title": "Топ брендов", "question": "Все бренды",
         "group_by": ["brand"], "chart_type": "bar"},

        {"title": "Топ вкусов", "question": "Все вкусы",
         "group_by": ["flavor"], "chart_type": "bar"},

        {"title": "Топ-20 SKU", "question": "Топ-20 товаров",
         "group_by": ["brand", "product_name"], "chart_type": "lollipop"},

        {"title": "Цена vs себестоимость", "question": "Цена и себестоимость по брендам",
         "group_by": ["brand"], "chart_type": "grouped_bar"},

        {"title": "Доли брендов", "question": "Топ-10 брендов",
         "group_by": ["brand"], "chart_type": "pie"},

        {"title": "Доли вкусов", "question": "Топ-10 вкусов",
         "group_by": ["flavor"], "chart_type": "pie"},

        {"title": "Топ производителей", "question": "Топ-15 производителей",
         "group_by": ["manufacturer"], "chart_type": "bar"},
    ]

    # Приоритетные вкусы — отдельные секции
    if filters.get("priority_flavors"):
        for flav in filters["priority_flavors"]:
            sections.append({
                "title": f"Вкус: {flav} по регионам",
                "question": f"Топ регионов для вкуса {flav}",
                "group_by": ["region_name"],
                "chart_type": "bar",
                "extra_filter": {"flavor": flav},
            })

    # Граммовки
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


# ============================================================
# ВАЛИДАЦИЯ
# ============================================================

VALID_CHART_TYPES = {"kpi", "bar", "line", "pie", "lollipop", "grouped_bar"}

FORBIDDEN_GROUP_BY = {"chip_type", "week_num"}


def _validate_sections(sections: list):
    """Приводит секции к корректному виду."""
    cleaned = []

    for i, section in enumerate(sections):
        if not isinstance(section, dict):
            print(f"[PLANNER] Section {i} is not dict, skipping")
            continue

        if "title" not in section or "question" not in section:
            print(f"[PLANNER] Section {i} missing title/question, skipping")
            continue

        chart_type = section.get("chart_type", "bar")
        if chart_type in ("heatmap", "table"):
            print(f"[PLANNER] Section '{section['title']}': {chart_type} → bar")
            chart_type = "bar"
        if chart_type not in VALID_CHART_TYPES:
            chart_type = "bar"
        section["chart_type"] = chart_type

        group_by = section.get("group_by") or []
        if not isinstance(group_by, list):
            group_by = []

        group_by = [g for g in group_by if g not in FORBIDDEN_GROUP_BY]

        if not group_by and chart_type != "kpi":
            print(f"[PLANNER] Section '{section['title']}' has empty group_by, skipping")
            continue

        section["group_by"] = group_by

        if "product_name" in group_by and chart_type == "bar":
            print(f"[PLANNER] Section '{section['title']}': product_name → lollipop")
            section["chart_type"] = "lollipop"

        cleaned.append(section)

    sections.clear()
    sections.extend(cleaned)


# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def plan_report(user_request: str) -> dict:
    """
    Генерирует план отчёта:
    1. Кэш
    2. Обогащение (entities + metrics + product RAG)
    3. LLM
    4. Fallback на regex
    """
    cache_key = _cache_key(user_request)
    if cache_key in _PLAN_CACHE:
        print(f"[PLANNER] Cache hit for: {user_request[:60]}...")
        return _PLAN_CACHE[cache_key]

    # Извлекаем структуру Excel-отчёта из запроса
    excel_hierarchy = extract_excel_hierarchy(user_request)
    if excel_hierarchy:
        print(f"[PLANNER] Excel hierarchy from user: {excel_hierarchy}")

    q1, entities = enrich_question(user_request)
    q2, _metrics = enrich_with_metrics(q1)

    brands = entities.get("brands") or []

    try:
        flavors_hint = build_flavors_hint_for_query(
            user_request,
            brand=brands[0] if brands else None,
        )
        if flavors_hint:
            q2 = q2 + flavors_hint
    except Exception as e:
        print(f"[PLANNER] flavor RAG failed: {e}")

    try:
        if brands or entities.get("flavors"):
            ctx = build_context_for_llm(
                brands=brands,
                flavors=entities.get("flavors"),
                max_items=10,
            )
            if ctx:
                q2 = q2 + ctx
    except Exception as e:
        print(f"[PLANNER] product context failed: {e}")

    enriched = q2
    system = PLANNER_SYSTEM_PROMPT + "\n\n" + PLAN_EXAMPLE
    user_msg = f"Запрос:\n{enriched}"

    try:
        raw, model = llm.ask(system, user_msg)
        cleaned = clean_json(raw)
        plan = json.loads(cleaned)

        if not isinstance(plan, dict) or "sections" not in plan:
            raise ValueError("bad structure")

        plan.setdefault("filters", {})
        _validate_sections(plan["sections"])

        if not plan["sections"]:
            raise ValueError("no valid sections after validation")

        plan["planner_model"] = model
        plan["excel_hierarchy"] = excel_hierarchy
        _PLAN_CACHE[cache_key] = plan
        print(f"[PLANNER] LLM plan built with {model}, sections: {len(plan['sections'])}")
        return plan

    except Exception as e:
        print(f"[PLANNER] LLM failed ({e}), using fallback")
        plan = plan_fallback(user_request)
        _validate_sections(plan["sections"])
        plan["excel_hierarchy"] = excel_hierarchy
        _PLAN_CACHE[cache_key] = plan
        return plan