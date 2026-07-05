"""Сборка мультисекционного отчёта."""
import time

from report_planner import plan_report
from sql_builder import build_sql_for_section
from database import db
from charts import build_chart


# Задержка между запросами секций (секунды)
# Помогает избежать rate limit на бесплатных моделях
SECTION_DELAY = 1.5


def build_report(user_request: str, progress_callback=None) -> dict:
    """
    Полный цикл:
    1. Планировщик → filters + sections
    2. Для каждой секции → SQL из шаблона → выполнение
    3. Строим графики
    """
    if progress_callback:
        progress_callback("🧠 Планирую отчёт...", 0.03)

    plan = plan_report(user_request)
    filters = plan.get("filters", {})
    sections_plan = plan.get("sections", [])
    total = len(sections_plan)

    sections = []
    for i, section in enumerate(sections_plan):
        title = section["title"]
        chart_type = section.get("chart_type", "bar")

        if progress_callback:
            progress_callback(
                f"📊 {i + 1}/{total}: {title}",
                0.05 + 0.9 * (i / total),
            )

        try:
            sql = build_sql_for_section(section, filters)
            df = db.query(sql)
            fig = build_chart(df, chart_type=chart_type, title=title)

            sections.append({
                "title": title,
                "question": section.get("question", ""),
                "group_by": section.get("group_by", []),
                "extra_filter": section.get("extra_filter"),
                "chart_type": chart_type,
                "sql": sql,
                "data": df,
                "fig": fig,
                "error": None,
            })
        except Exception as e:
            sections.append({
                "title": title,
                "question": section.get("question", ""),
                "group_by": section.get("group_by", []),
                "extra_filter": section.get("extra_filter"),
                "chart_type": chart_type,
                "sql": None,
                "data": None,
                "fig": None,
                "error": str(e),
            })

        # Задержка между секциями кроме последней
        if i < total - 1:
            time.sleep(SECTION_DELAY)

    if progress_callback:
        progress_callback("✅ Готово!", 1.0)

    return {
        "request": user_request,
        "filters": filters,
        "plan": sections_plan,
        "planner_model": plan.get("planner_model", "unknown"),
        "sections": sections,
    }