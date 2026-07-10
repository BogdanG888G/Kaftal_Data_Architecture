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


_PLAN_CACHE = {}

FALSE_POSITIVE_BRANDS = {
    "маркет", "супер", "гипер", "фуд", "food", "market", "лайк", "the", "and",
}


def _cache_key(request: str) -> str:
    return hashlib.md5(request.strip().lower().encode()).hexdigest()


def _clean_false_positive_brands(brands: list, user_request: str) -> list:
    if not brands:
        return brands
    q_lower = user_request.lower()
    cleaned = []
    for brand in brands:
        brand_lower = brand.lower()
        if brand_lower in FALSE_POSITIVE_BRANDS:
            continue
        if brand_lower in ("маркет", "супер", "гипер"):
            for compound in ["гипермаркет", "супермаркет"]:
                if compound in q_lower and brand_lower in compound:
                    break
            else:
                cleaned.append(brand)
                continue
            continue
        cleaned.append(brand)
    return cleaned


PLANNER_SYSTEM_PROMPT = """Ты составляешь план отчёта по продажам чипсов.

Верни ТОЛЬКО JSON без markdown-обёрток:
{
  "filters": {
    "retail_chain": null или строка,
    "store_format": null или "ГМ"|"СМ"|"У"|"Дискаунтер",
    "chip_type": "Картофельные чипсы",
    "year": null или 2024|2025|2026,
    "month": null или "Январь"|...|"Декабрь",
    "weight_grams": null или int,
    "weight_grams_list": null или [70,120,140],
    "brands": null или ["Lay's"],
    "flavors": null или ["Сметана и лук"],
    "priority_flavors": null или ["..."]
  },
  "sections": [...]
}

ПРАВИЛА:
- 10-15 секций
- Первая ВСЕГДА KPI (chart_type="kpi", group_by=[])
- Вторая ВСЕГДА динамика (chart_type="line")
- ВСЕГДА добавляй "chip_type": "Картофельные чипсы" если пользователь не указал другой тип
- В названиях секций ВСЕГДА указывай цифру: "Топ-25 брендов", "Топ-20 SKU"
- НЕ используй heatmap, table, chip_type в group_by, week_num
- НЕ придумывай вкусы
- НЕ используй brand="Маркет"/"Супер"/"Гипер"

ТИПЫ chart_type:
- "kpi", "line", "bar", "lollipop", "grouped_bar", "pie", "small_multiples"

ПРИОРИТЕТ секций:
1. KPI
2. Динамика (line)
3. Топ-25 регионов (bar)
4. Топ-20 городов (bar)
5. Топ-25 брендов (bar)
6. Топ-25 вкусов (bar)
7. Топ-20 SKU (lollipop, group_by=["brand","product_name"])
8. Цена vs себестоимость топ-15 брендов (grouped_bar)
9. Топ вкусов по каждому из топ-9 брендов (small_multiples)
10. Выручка по граммовкам (bar, group_by=["weight_grams"])
11. Доли топ-10 брендов (pie)
12. Доли топ-10 вкусов (pie)
13. Топ-15 производителей (bar)
14. Для каждого priority_flavor — секция с extra_filter

ВАЖНО ПРО ПЕРИОДЫ:
- Используй значения из блока "РАЗРЕШЁННЫЙ ОТНОСИТЕЛЬНЫЙ ПЕРИОД" если есть.
- Если конкретный год/месяц указан явно — приоритет ему.

ВАЖНО ПРО ЗНАЧЕНИЯ КОЛОНОК:
- Используй ТОЛЬКО реальные значения из блока "РЕАЛЬНЫЕ ЗНАЧЕНИЯ КОЛОНОК".
- гипермаркет → ГМ, супермаркет → СМ.
"""

PLAN_EXAMPLE = """Пример:
Запрос: "Магнит ГМ 120г 2026 по территориям, приоритет Сметана и лук"
Ответ:
{"filters":{"retail_chain":"Магнит","store_format":"ГМ","chip_type":"Картофельные чипсы","year":2026,"weight_grams":120,"priority_flavors":["Сметана и лук"]},"sections":[{"title":"KPI","question":"Общие показатели","group_by":[],"chart_type":"kpi"},{"title":"Динамика по месяцам","question":"Динамика","group_by":["year","month"],"chart_type":"line"},{"title":"Топ-25 регионов","question":"Регионы","group_by":["region_name"],"chart_type":"bar"},{"title":"Топ-25 брендов","question":"Бренды","group_by":["brand"],"chart_type":"bar"},{"title":"Топ-25 вкусов","question":"Вкусы","group_by":["flavor"],"chart_type":"bar"},{"title":"Топ-20 SKU","question":"Товары","group_by":["brand","product_name"],"chart_type":"lollipop"},{"title":"Цена vs себестоимость топ-15 брендов","question":"Цены","group_by":["brand"],"chart_type":"grouped_bar"},{"title":"Топ вкусов по каждому из топ-9 брендов","question":"Вкусы брендов","group_by":["brand","flavor"],"chart_type":"small_multiples"},{"title":"Выручка по граммовкам","question":"Граммовки","group_by":["weight_grams"],"chart_type":"bar"},{"title":"Доли топ-10 брендов","question":"Доли","group_by":["brand"],"chart_type":"pie"},{"title":"Доли топ-10 вкусов","question":"Доли","group_by":["flavor"],"chart_type":"pie"},{"title":"Сметана и лук по регионам","question":"Вкус","group_by":["region_name"],"chart_type":"bar","extra_filter":{"flavor":"Сметана и лук"}}]}
"""


def clean_json(text: str) -> str:
    text = text.strip()
    text = re.sub(r"```json", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```", "", text)
    match = re.search(r"\{[\s\S]+\}", text)
    if match:
        return match.group(0)
    return text.strip()


def _extract_year(text: str) -> int | None:
    m = re.search(r"\b(202[3-7])\b", text)
    return int(m.group(1)) if m else None


MONTH_MAP = {
    "январ": "Январь", "феврал": "Февраль", "март": "Март", "марта": "Март",
    "марте": "Март", "апрел": "Апрель", "май": "Май", "мая": "Май", "мае": "Май",
    "июн": "Июнь", "июл": "Июль", "август": "Август", "сентябр": "Сентябрь",
    "октябр": "Октябрь", "ноябр": "Ноябрь", "декабр": "Декабрь",
}


def _extract_month(text: str) -> str | None:
    text_lower = text.lower()
    for key, month_name in MONTH_MAP.items():
        if re.search(rf"\b{key}\w*", text_lower):
            return month_name
    return None


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
        r"excel[-\s]*отч[её]т\s*по[:\s]", r"excel\s*по[:\s]",
        r"выгрузк[аиу]\s*по[:\s]", r"таблиц[аеу]\s*по[:\s]",
        r"отч[её]т\s*по\s*(?:колонкам|полям)[:\s]",
        r"структура[:\s]", r"колонки[:\s]",
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


def plan_fallback(user_request: str) -> dict:
    entities = extract_entities(user_request)
    if entities.get("brands"):
        entities["brands"] = _clean_false_positive_brands(entities["brands"], user_request)
    grams_list = extract_grams_list(user_request)
    year = _extract_year(user_request)
    month = _extract_month(user_request)

    has_flavor_context = _has_marker(user_request, FLAVOR_MARKERS) or _has_flavor_keyword(user_request)
    flavors = entities["flavors"] if has_flavor_context else None

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
        {"title": "KPI обзор", "question": "Общие показатели", "group_by": [], "chart_type": "kpi"},
        {"title": "Динамика по дням" if month else "Динамика по месяцам",
         "question": "Динамика", "group_by": ["date"] if month else ["year", "month"], "chart_type": "line"},
        {"title": "Топ-25 регионов", "question": "Регионы", "group_by": ["region_name"], "chart_type": "bar"},
        {"title": "Топ-20 городов", "question": "Города", "group_by": ["city_name"], "chart_type": "bar"},
        {"title": "Топ-25 брендов", "question": "Бренды", "group_by": ["brand"], "chart_type": "bar"},
        {"title": "Топ-25 вкусов", "question": "Вкусы", "group_by": ["flavor"], "chart_type": "bar"},
        {"title": "Топ-20 SKU", "question": "Товары", "group_by": ["brand", "product_name"], "chart_type": "lollipop"},
        {"title": "Цена vs себестоимость топ-15 брендов", "question": "Цены", "group_by": ["brand"], "chart_type": "grouped_bar"},
        {"title": "Топ вкусов по каждому из топ-9 брендов", "question": "Вкусы брендов", "group_by": ["brand", "flavor"], "chart_type": "small_multiples"},
        {"title": "Выручка по граммовкам", "question": "Граммовки", "group_by": ["weight_grams"], "chart_type": "bar"},
        {"title": "Доли топ-10 брендов", "question": "Доли", "group_by": ["brand"], "chart_type": "pie"},
        {"title": "Доли топ-10 вкусов", "question": "Доли", "group_by": ["flavor"], "chart_type": "pie"},
        {"title": "Топ-15 производителей", "question": "Производители", "group_by": ["manufacturer"], "chart_type": "bar"},
    ]

    if filters.get("priority_flavors"):
        for flav in filters["priority_flavors"]:
            sections.append({
                "title": f"Вкус: {flav} по регионам", "question": f"Регионы для {flav}",
                "group_by": ["region_name"], "chart_type": "bar", "extra_filter": {"flavor": flav},
            })

    if filters.get("weight_grams_list"):
        sections.append({
            "title": "По граммовкам (детально)", "question": "Граммовки",
            "group_by": ["weight_grams"], "chart_type": "bar",
        })

    return {"filters": filters, "sections": sections, "planner_model": "fallback (regex)", "period_info": period_info}


VALID_CHART_TYPES = {"kpi", "bar", "line", "pie", "lollipop", "grouped_bar", "small_multiples"}
FORBIDDEN_GROUP_BY = {"chip_type", "week_num"}


def _validate_sections(sections: list):
    cleaned = []
    for i, section in enumerate(sections):
        if not isinstance(section, dict):
            continue
        if "title" not in section or "question" not in section:
            continue
        chart_type = section.get("chart_type", "bar")
        if chart_type in ("heatmap", "table"):
            chart_type = "bar"
        if chart_type not in VALID_CHART_TYPES:
            chart_type = "bar"
        section["chart_type"] = chart_type
        group_by = section.get("group_by") or []
        if not isinstance(group_by, list):
            group_by = []
        group_by = [g for g in group_by if g not in FORBIDDEN_GROUP_BY]
        if not group_by and chart_type != "kpi":
            continue
        section["group_by"] = group_by
        if "product_name" in group_by and chart_type == "bar":
            section["chart_type"] = "lollipop"
        if chart_type == "small_multiples" and len(group_by) < 2:
            section["chart_type"] = "bar"
        cleaned.append(section)
    sections.clear()
    sections.extend(cleaned)


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
    cache_key = _cache_key(user_request)
    if cache_key in _PLAN_CACHE:
        print(f"[PLANNER] Cache hit for: {user_request[:60]}...")
        return _PLAN_CACHE[cache_key]

    excel_hierarchy = extract_excel_hierarchy(user_request)
    if excel_hierarchy:
        print(f"[PLANNER] Excel hierarchy: {excel_hierarchy}")

    entities_preview = extract_entities(user_request)
    preview_brands = _clean_false_positive_brands(entities_preview.get("brands") or [], user_request)
    preview_filters = {
        "retail_chain": entities_preview["chains"][0] if entities_preview["chains"] else None,
        "store_format": entities_preview["formats"][0] if entities_preview["formats"] else None,
        "chip_type": entities_preview["chip_types"][0] if entities_preview["chip_types"] else None,
        "brands": preview_brands or None,
    }

    period_info = resolve_relative_period(user_request, filters=preview_filters)
    if period_info:
        print(f"[PLANNER] Period: {period_info.get('description')}")

    q1, entities = enrich_question(user_request)
    if entities.get("brands"):
        entities["brands"] = _clean_false_positive_brands(entities["brands"], user_request)
    q2, _metrics = enrich_with_metrics(q1)

    brands = entities.get("brands") or []

    try:
        fh = build_flavors_hint_for_query(user_request, brand=brands[0] if brands else None)
        if fh:
            q2 = q2 + fh
    except Exception:
        pass

    try:
        if brands or entities.get("flavors"):
            ctx = build_context_for_llm(brands=brands, flavors=entities.get("flavors"), max_items=10)
            if ctx:
                q2 = q2 + ctx
    except Exception:
        pass

    column_hints = _get_column_values_hints()
    if column_hints:
        q2 = q2 + column_hints

    if period_info:
        try:
            ph = build_period_hint(period_info)
            if ph:
                q2 = q2 + ph
        except Exception:
            pass

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

        if plan["filters"].get("brands"):
            plan["filters"]["brands"] = _clean_false_positive_brands(plan["filters"]["brands"], user_request)
            if not plan["filters"]["brands"]:
                plan["filters"]["brands"] = None

        # Гарантируем chip_type
        if not plan["filters"].get("chip_type"):
            plan["filters"]["chip_type"] = "Картофельные чипсы"

        if period_info:
            f = plan["filters"]
            if not f.get("year") and "year" in period_info:
                f["year"] = period_info["year"]
            if not f.get("month") and "month" in period_info:
                f["month"] = period_info["month"]

        _validate_sections(plan["sections"])
        if not plan["sections"]:
            raise ValueError("no valid sections")

        plan["planner_model"] = model
        plan["excel_hierarchy"] = excel_hierarchy
        plan["period_info"] = period_info
        _PLAN_CACHE[cache_key] = plan
        print(f"[PLANNER] LLM plan: {model}, sections: {len(plan['sections'])}")
        return plan

    except Exception as e:
        print(f"[PLANNER] LLM failed ({e}), using fallback")
        plan = plan_fallback(user_request)
        _validate_sections(plan["sections"])
        plan["excel_hierarchy"] = excel_hierarchy
        plan["period_info"] = period_info
        _PLAN_CACHE[cache_key] = plan
        return plan