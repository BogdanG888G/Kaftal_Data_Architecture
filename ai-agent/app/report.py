"""Сборка мультисекционного отчёта."""
import time

from report_planner import plan_report
from sql_builder import build_sql_for_section
from database import db
from charts import build_chart, chart_line
from data_profile import profile_data, filter_sections_by_profile


SECTION_DELAY = 1.2


def expand_line_sections(sections_plan: list) -> list:
    """
    Раскладывает каждую line-секцию на 3:
    - Выручка
    - Количество проданного
    - Средняя цена
    """
    expanded = []
    for section in sections_plan:
        if section.get("chart_type") == "line":
            base_title = section["title"]
            base_group = section.get("group_by", ["year", "month"])
            extra = section.get("extra_filter")

            for metric_key, metric_label in [
                ("revenue", "Выручка"),
                ("qty", "Количество проданного"),
                ("avg_price", "Средняя цена"),
            ]:
                new_section = {
                    "title": f"{base_title} — {metric_label}",
                    "question": f"{metric_label} по времени",
                    "group_by": base_group,
                    "chart_type": "line",
                    "extra_filter": extra,
                    "metric_filter": metric_key,
                }
                expanded.append(new_section)
        else:
            expanded.append(section)
    return expanded


def build_report(user_request: str, progress_callback=None) -> dict:
    """
    Полный цикл сборки отчёта с детальным прогрессом.
    """
    start_time = time.time()
    timings = {}

    # === Шаг 1: Планирование ===
    if progress_callback:
        progress_callback("🧠 Читаю запрос и планирую структуру отчёта...", 0.03)

    t0 = time.time()
    plan = plan_report(user_request)
    timings["planning"] = time.time() - t0

    filters = plan.get("filters", {})
    sections_plan = plan.get("sections", [])
    excel_hierarchy = plan.get("excel_hierarchy") or []

    # === Шаг 2: Профилирование ===
    if progress_callback:
        progress_callback("🔍 Анализирую структуру данных в ClickHouse...", 0.08)

    t0 = time.time()
    profile = profile_data(filters)
    timings["profiling"] = time.time() - t0

    print(f"[REPORT] Profile: {profile['row_count']} rows, empty: {profile['empty']}")

    # === Шаг 3: Фильтрация секций ===
    if progress_callback:
        empty_str = ", ".join(profile["empty"][:3]) if profile["empty"] else "нет"
        progress_callback(
            f"🧹 Убираю нерелевантные секции (пустые: {empty_str})...",
            0.12,
        )

    sections_plan = filter_sections_by_profile(sections_plan, profile)

    if not sections_plan:
        raise RuntimeError("После фильтрации по профилю не осталось ни одной секции.")

    # === Шаг 4: Разворачиваем line на 3 секции ===
    sections_plan = expand_line_sections(sections_plan)

    # === Шаг 5: Выполнение секций ===
    total = len(sections_plan)
    sections = []

    t0 = time.time()

    for i, section in enumerate(sections_plan):
        title = section["title"]
        chart_type = section.get("chart_type", "bar")
        metric_filter = section.get("metric_filter")

        if progress_callback:
            progress = 0.15 + 0.80 * (i / total)
            progress_callback(
                f"📊 [{i + 1}/{total}] Строю секцию «{title}»...",
                progress,
            )

        try:
            section_start = time.time()

            sql = build_sql_for_section(section, filters)
            df = db.query(sql)

            # Для line с metric_filter — специальный чарт
            if chart_type == "line" and metric_filter:
                fig = chart_line(df, title=title, metric_filter=metric_filter)
            else:
                fig = build_chart(df, chart_type=chart_type, title=title)

            section_time = time.time() - section_start

            sections.append({
                "title": title,
                "question": section.get("question", ""),
                "group_by": section.get("group_by", []),
                "extra_filter": section.get("extra_filter"),
                "chart_type": chart_type,
                "metric_filter": metric_filter,
                "sql": sql,
                "data": df,
                "fig": fig,
                "time_sec": section_time,
                "error": None,
            })
        except Exception as e:
            sections.append({
                "title": title,
                "question": section.get("question", ""),
                "group_by": section.get("group_by", []),
                "extra_filter": section.get("extra_filter"),
                "chart_type": chart_type,
                "metric_filter": metric_filter,
                "sql": None,
                "data": None,
                "fig": None,
                "time_sec": 0,
                "error": str(e),
            })

        if i < total - 1:
            time.sleep(SECTION_DELAY)

    timings["sections"] = time.time() - t0
    timings["total"] = time.time() - start_time

    if progress_callback:
        progress_callback(f"✅ Готово! Время: {timings['total']:.1f}с", 1.0)

    return {
        "request": user_request,
        "filters": filters,
        "profile": profile,
        "plan": sections_plan,
        "planner_model": plan.get("planner_model", "unknown"),
        "excel_hierarchy": excel_hierarchy,
        "sections": sections,
        "timings": timings,
    }