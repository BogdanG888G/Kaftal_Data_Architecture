"""Планировщик отчётов — компактный + fallback + product RAG + period resolver + column values."""
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
from period_resolver import resolve_relative_period, build_period_hint


# In-memory cache планов
_PLAN_CACHE = {}


def _cache_key(request: str) -> str:
    return hashlib.md5(request.strip().lower().encode()).hexdigest()


# ============================================================
# ЛОЖНО-ПОЛОЖИТЕЛЬНЫЕ БРЕНДЫ (не должны попадать в фильтры)
# ============================================================

FALSE_POSITIVE_BRANDS = {
    # Части составных слов
    "маркет", "супер", "гипер",
    # Общие слова
    "фуд", "food", "market",
    # Слишком короткие для точного match
    "лайк", "the", "and",
}


def _clean_false_positive_brands(brands: list, user_request: str) -> list:
    """
    Убирает бренды которые скорее всего попали из ложного match.
    Например 'Маркет' из 'гипермаркет'.
    """
    if not brands:
        return brands

    q_lower = user_request.lower()
    cleaned = []

    for brand in brands:
        brand_lower = brand.lower()

        # Если бренд в списке ложных
        if brand_lower in FALSE_POSITIVE_BRANDS:
            print(f"[PLANNER] Removing false-positive brand: {brand}")
            continue

        # Если бренд — часть слова "гипермаркет" / "супермаркет" и т.п.
        # проверяем что пользователь его явно не написал отдельным словом
        if brand_lower in ("маркет", "супер", "гипер"):
            # Проверим есть ли он как отдельное слово в запросе
            if not re.search(rf"\b{re.escape(brand_lower)}\b", q_lower):
                print(f"[PLANNER] Removing brand '{brand}' — not a standalone word")
                continue
            # Или если он часть большего слова
            for compound in ["гипермаркет", "супермаркет"]:
                if compound in q_lower and brand_lower in compound:
                    print(f"[PLANNER] Removing brand '{brand}' — part of '{compound}'")
                    break
            else:
                cleaned.append(brand)
                continue

        cleaned.append(brand)

    return cleaned


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
    {"title":"...","question":"...","group_by":[...],"chart_type":"kpi|bar|line|pie|lollipop|grouped_bar|small_multiples","extra_filter":null или {"flavor":"..."}}
  ]
}

ПРАВИЛА СЕКЦИЙ:
- 10-14 секций
- Первая секция ВСЕГДА KPI (chart_type="kpi", group_by=[])
- Вторая секция ВСЕГДА динамика по времени (chart_type="line", group_by=["year","month"] или ["date"] если задан month)

ТИПЫ ГРАФИКОВ (chart_type):
- "kpi" — только для первой обзорной секции
- "line" — динамика по времени (автоматически разделится на 3 секции: выручка/количество/цена)
- "bar" — топ регионов, городов, брендов, вкусов, производителей
- "lollipop" — ОБЯЗАТЕЛЬНО для секций "Топ SKU" / "Топ товаров" (group_by содержит product_name)
- "grouped_bar" — только для сравнения цены vs себестоимости (group_by=["brand"])
- "pie" — доли (топ-10 брендов, топ-10 вкусов)
- "small_multiples" — сетка мини-графиков: топ вкусов внутри каждого топ-бренда (group_by=["brand","flavor"])

ЗАПРЕЩЕНО:
- НЕ используй chart_type "heatmap" (убран)
- НЕ используй chart_type "table"
- НЕ добавляй секции по chip_type (тип чипсов)
- НЕ добавляй секции по weight_grams если пользователь про них не спрашивал
- НЕ добавляй разбивки по неделям (week_num)
- НЕ дублируй фильтры внутри question (они уже в filters)
- НЕ придумывай вкусы, которых нет в запросе пользователя
- НЕ используй brand="Маркет"/"Супер"/"Гипер" — это части слов "гипермаркет", "супермаркет"

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
9. Топ вкусов по каждому бренду (chart_type="small_multiples", group_by=["brand","flavor"])
10. Доли брендов (pie)
11. Доли вкусов (pie)
12. Топ производителей (если данные заполнены)
13. Для каждого priority_flavor — отдельная секция с extra_filter

ВАЖНО ПРО ПЕРИОДЫ:
- Если пользователь пишет "последний период" / "прошлый месяц" / "за квартал" — используй значения из блока "РАЗРЕШЁННЫЙ ОТНОСИТЕЛЬНЫЙ ПЕРИОД".
- Если конкретный год/месяц указан явно — приоритет ему.
- Не оставляй year=null если это не оправдано.

ВАЖНО ПРО ЗНАЧЕНИЯ КОЛОНОК:
- Используй ТОЛЬКО реальные значения из блока "РЕАЛЬНЫЕ ЗНАЧЕНИЯ КОЛОНОК" ниже.
- Разговорные слова (гипермаркет, супермаркет и т.п.) резолвь через "МАППИНГ РАЗГОВОРНЫХ СЛОВ".
- "гипермаркет" — это НЕ бренд, это store_format="ГМ". НЕ пиши brand="Маркет".
- "супермаркет" — это НЕ бренд, это store_format="СМ".
"""


PLAN_EXAMPLE = """Пример:
Запрос: "Магнит ГМ 120г картофельные чипсы 2026 по территориям, приоритет Сметана и лук, Морепродукты"

Ответ (валидный JSON):
{"filters":{"retail_chain":"Магнит","store_format":"ГМ","chip_type":"Картофельные чипсы","year":2026,"weight_grams":120,"priority_flavors":["Сметана и лук","Морепродукты"]},"sections":[{"title":"KPI","question":"Общие показатели","group_by":[],"chart_type":"kpi"},{"title":"Динамика по месяцам","question":"Выручка, количество и средняя цена по месяцам","group_by":["year","month"],"chart_type":"line"},{"title":"Топ регионов","question":"Топ-25 регионов","group_by":["region_name"],"chart_type":"bar"},{"title":"Топ городов","question":"Топ-20 городов","group_by":["city_name"],"chart_type":"bar"},{"title":"Топ брендов","question":"Все бренды","group_by":["brand"],"chart_type":"bar"},{"title":"Топ вкусов","question":"Все вкусы","group_by":["flavor"],"chart_type":"bar"},{"title":"Топ-20 SKU","question":"Топ-20 товаров","group_by":["brand","product_name"],"chart_type":"lollipop"},{"title":"Цена vs себестоимость","question":"Цена продажи и себестоимость по брендам","group_by":["brand"],"chart_type":"grouped_bar"},{"title":"Топ вкусов по каждому бренду","question":"Топ вкусов у топ-9 брендов","group_by":["brand","flavor"],"chart_type":"small_multiples"},{"title":"Доли брендов","question":"Топ-10 брендов","group_by":["brand"],"chart_type":"pie"},{"title":"Доли вкусов","question":"Топ-10 вкусов","group_by":["flavor"],"chart_type":"pie"},{"title":"Сметана и лук по регионам","question":"Топ регионов для этого вкуса","group_by":["region_name"],"chart_type":"bar","extra_filter":{"flavor":"Сметана и лук"}},{"title":"Морепродукты по регионам","question":"Топ регионов для этого вкуса","group_by":["region_name"],"chart_type":"bar","extra_filter":{"flavor":"Морепродукты"}}]}
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
# EXTRACT YEAR / MONTH
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
    "brand": ["бренд"],
    "flavor": ["вкус"],
    "weight_grams": ["граммовк", "грамм"],
    "manufacturer": ["производитель"],
    "vendor": ["поставщик"],
    "product_name": ["название товара", "sku"],
    "package_type": ["упаковк"],
    "store_code": ["код магазина"],
    "chip_type": ["тип чипсов"],
}


def extract_excel_hierarchy(user_request: str) -> list:
    q_lower = user_request.lower()

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
        return []

    tail = q_lower[trigger_pos:]

    found = []
    for col_key, keywords in EXCEL_COLUMN_KEYWORDS.items():
        best_pos = -1
        for kw in keywords:
            m = re.search(rf"\b{re.escape(kw)}", tail)
            if m:
                pos = m.start()
                if best_pos == -1 or pos < best_pos:
                    best_pos = pos

        if best_pos != -1:
            found.append((best_pos, col_key))

    found.sort(key=lambda x: x[0])
    return [col for _, col in found]


# ============================================================
# FALLBACK
# ============================================================

def plan_fallback(user_request: str) -> dict:
    """Строит базовый план БЕЗ LLM."""
    entities = extract_entities(user_request)

    # Чистим ложно-положительные бренды
    if entities.get("brands"):
        entities["brands"] = _clean_false_positive_brands(
            entities["brands"], user_request
        )

    grams_list = extract_grams_list(user_request)
    year = _extract_year(user_request)
    month = _extract_month(user_request)

    has_flavor_context = (
        _has_marker(user_request, FLAVOR_MARKERS)
        or _has_flavor_keyword(user_request)
    )
    flavors = entities["flavors"] if has_flavor_context else None

    # Разрешаем относительные периоды с учётом фильтров
    preview_filters = {
        "retail_chain": entities["chains"][0] if entities["chains"] else None,
        "store_format": entities["formats"][0] if entities["formats"] else None,
        "chip_type": entities["chip_types"][0] if entities["chip_types"] else None,
        "brands": entities["brands"] or None,
    }
    period_info = resolve_relative_period(user_request, filters=preview_filters)
    if period_info:
        print(f"[FALLBACK] Resolved period: {period_info.get('description')}")
        if not year and "year" in period_info:
            year = period_info["year"]
        if not month and "month" in period_info:
            month = period_info["month"]

    filters = {
        "retail_chain": entities["chains"][0] if entities["chains"] else None,
        "store_format": entities["formats"][0] if entities["formats"] else None,
        # ВСЕГДА фильтр по картофельным чипсам, если пользователь не указал другой тип
        "chip_type": entities["chip_types"][0] if entities["chip_types"] else "Картофельные чипсы",
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

        {"title": "Топ-25 регионов", "question": "Топ-25 регионов",
         "group_by": ["region_name"], "chart_type": "bar"},

        {"title": "Топ-20 городов", "question": "Топ-20 городов",
         "group_by": ["city_name"], "chart_type": "bar"},

        {"title": "Топ-25 брендов", "question": "Топ-25 брендов по выручке",
         "group_by": ["brand"], "chart_type": "bar"},

        {"title": "Топ-25 вкусов", "question": "Топ-25 вкусов по выручке",
         "group_by": ["flavor"], "chart_type": "bar"},

        {"title": "Топ-20 SKU", "question": "Топ-20 товаров",
         "group_by": ["brand", "product_name"], "chart_type": "lollipop"},

        {"title": "Цена vs себестоимость по топ-15 брендам",
         "question": "Цена и себестоимость по брендам",
         "group_by": ["brand"], "chart_type": "grouped_bar"},

        {"title": "Топ вкусов по каждому из топ-9 брендов",
         "question": "Топ вкусов внутри топ-брендов",
         "group_by": ["brand", "flavor"], "chart_type": "small_multiples"},

        {"title": "Выручка по граммовкам",
         "question": "Выручка и количество по граммовкам",
         "group_by": ["weight_grams"], "chart_type": "bar"},

        {"title": "Доли топ-10 брендов", "question": "Топ-10 брендов",
         "group_by": ["brand"], "chart_type": "pie"},

        {"title": "Доли топ-10 вкусов", "question": "Топ-10 вкусов",
         "group_by": ["flavor"], "chart_type": "pie"},

        {"title": "Топ-15 производителей", "question": "Топ-15 производителей",
         "group_by": ["manufacturer"], "chart_type": "bar"},
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
        "period_info": period_info,
    }


# ============================================================
# ВАЛИДАЦИЯ
# ============================================================

VALID_CHART_TYPES = {"kpi", "bar", "line", "pie", "lollipop", "grouped_bar", "small_multiples"}
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

        if chart_type == "small_multiples" and len(group_by) < 2:
            print(f"[PLANNER] Section '{section['title']}': small_multiples без 2 колонок → bar")
            section["chart_type"] = "bar"

        cleaned.append(section)

    sections.clear()
    sections.extend(cleaned)


# ============================================================
# ОСНОВНАЯ ФУНКЦИЯ
# ============================================================

def _get_column_values_hints() -> str:
    hints = []
    try:
        from column_values import build_column_values_hint, build_conversational_hint
        cv = build_column_values_hint()
        if cv:
            hints.append(cv)
        conv = build_conversational_hint()
        if conv:
            hints.append(conv)
    except Exception as e:
        print(f"[PLANNER] column_values failed: {e}")
    return "\n".join(hints)


def plan_report(user_request: str) -> dict:
    """
    Генерирует план отчёта:
    1. Кэш
    2. Обогащение (entities + metrics + product RAG + period + column values)
    3. LLM
    4. Fallback на regex
    """
    cache_key = _cache_key(user_request)
    if cache_key in _PLAN_CACHE:
        print(f"[PLANNER] Cache hit for: {user_request[:60]}...")
        return _PLAN_CACHE[cache_key]

    excel_hierarchy = extract_excel_hierarchy(user_request)
    if excel_hierarchy:
        print(f"[PLANNER] Excel hierarchy from user: {excel_hierarchy}")

    # === Шаг 1: сначала извлекаем сущности (без периода) ===
    entities_preview = extract_entities(user_request)

    # Чистим ложно-положительные бренды в preview
    preview_brands = _clean_false_positive_brands(
        entities_preview.get("brands") or [], user_request
    )

    preview_filters = {
        "retail_chain": entities_preview["chains"][0] if entities_preview["chains"] else None,
        "store_format": entities_preview["formats"][0] if entities_preview["formats"] else None,
        "chip_type": entities_preview["chip_types"][0] if entities_preview["chip_types"] else None,
        "brands": preview_brands or None,
    }

    # === Шаг 2: разрешаем период с учётом фильтров ===
    period_info = resolve_relative_period(user_request, filters=preview_filters)
    if period_info:
        print(f"[PLANNER] Resolved period (context-aware): {period_info.get('description')}")

    # === Шаг 3: полное обогащение ===
    q1, entities = enrich_question(user_request)

    # Чистим ложно-положительные бренды в основном enrichment
    if entities.get("brands"):
        entities["brands"] = _clean_false_positive_brands(
            entities["brands"], user_request
        )

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

    column_hints = _get_column_values_hints()
    if column_hints:
        q2 = q2 + column_hints

    if period_info:
        try:
            period_hint = build_period_hint(period_info)
            if period_hint:
                q2 = q2 + period_hint
        except Exception as e:
            print(f"[PLANNER] period hint failed: {e}")

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

        # Чистим ложно-положительные бренды и в ответе LLM
        if plan["filters"].get("brands"):
            plan["filters"]["brands"] = _clean_false_positive_brands(
                plan["filters"]["brands"], user_request
            )
            if not plan["filters"]["brands"]:
                plan["filters"]["brands"] = None

        # Если LLM пропустила период — накатываем из resolver
        if period_info:
            filters = plan["filters"]
            if not filters.get("year") and "year" in period_info:
                filters["year"] = period_info["year"]
                print(f"[PLANNER] Injected year from period_info: {period_info['year']}")
            if not filters.get("month") and "month" in period_info:
                filters["month"] = period_info["month"]
                print(f"[PLANNER] Injected month from period_info: {period_info['month']}")

        _validate_sections(plan["sections"])

        if not plan["sections"]:
            raise ValueError("no valid sections after validation")

        plan["planner_model"] = model
        plan["excel_hierarchy"] = excel_hierarchy
        plan["period_info"] = period_info

        _PLAN_CACHE[cache_key] = plan
        print(f"[PLANNER] LLM plan built with {model}, sections: {len(plan['sections'])}")
        return plan

    except Exception as e:
        print(f"[PLANNER] LLM failed ({e}), using fallback")
        plan = plan_fallback(user_request)
        _validate_sections(plan["sections"])
        plan["excel_hierarchy"] = excel_hierarchy
        plan["period_info"] = period_info
        _PLAN_CACHE[cache_key] = plan
        return plan