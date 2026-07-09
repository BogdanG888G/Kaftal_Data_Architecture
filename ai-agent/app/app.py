"""Streamlit UI — Sales Analytics AI Agent (Report Mode only)."""
import time

import streamlit as st
import pandas as pd

from report import build_report
from charts import build_chart, fig_to_png_bytes
from utils import to_excel
from excel_report import build_excel_bytes
from kpi_cards import render_kpi_cards
from pptx_report import build_pptx_bytes
from auth import login_form, logout_button


st.set_page_config(
    page_title="Sales AI Agent",
    page_icon="📊",
    layout="wide",
)


# ============================================================
# АВТОРИЗАЦИЯ
# ============================================================

if not login_form():
    st.stop()


# ============================================================
# STATE INIT
# ============================================================

if "reports" not in st.session_state:
    st.session_state["reports"] = []
if "question_input" not in st.session_state:
    st.session_state["question_input"] = ""


# ============================================================
# HEADER
# ============================================================

st.title("📊 Sales Analytics AI Agent")
st.caption(
    "AI-агент для аналитики продаж чипсов и снеков в 19 розничных сетях. "
    "Опиши что тебе нужно на русском — я построю подробный отчёт с графиками, "
    "Excel-выгрузкой и PPTX-презентацией."
)


# Toast о готовности
if st.session_state.pop("show_success_toast", False):
    st.toast("✅ Готово! Прокрути вниз ⬇️", icon="🎉")


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.header("💡 Примеры запросов")

    examples = [
        "Магнит ГМ 120г картофельные чипсы 2026, полный анализ по территориям с приоритетными вкусами (Сметана и лук, Морепродукты, Томат)",
        "Полный отчёт по продажам Lay's за последний год",
        "Аналитика всех сетей по картофельным чипсам за последний период",
        "Детальный анализ бренда Pringles по регионам и вкусам",
        "Отчёт по сети Пятерочка: динамика, топ товаров, категории",
        "Картофельные чипсы 70 120 140 180 220 225 250 грамм: цена, себестоимость, цена за грамм, нормализация к 120г",
        "Ашан за последний месяц",
        "Магнит за прошлый квартал",
    ]

    for ex in examples:
        if st.button(ex, use_container_width=True, key=f"ex_{hash(ex)}"):
            st.session_state["question_input"] = ex
            st.rerun()

    st.divider()
    st.caption(
        "⚠️ Агент выполняет только SELECT-запросы.\n\n"
        "📅 Умеет понимать: 'последний период', 'прошлый квартал', "
        "'этот год', 'год к году'.\n\n"
        "🎯 Умеет извлекать структуру Excel: напиши 'excel-отчёт по: сеть, регион, ...'."
    )

    if st.button("🗑 Очистить историю", use_container_width=True):
        st.session_state["reports"] = []
        st.rerun()

    # Кнопка выхода
    logout_button()


# ============================================================
# ФОРМА ВВОДА
# ============================================================

with st.form(key="query_form", clear_on_submit=False):
    question = st.text_area(
        "Опиши какой отчёт тебе нужен:",
        value=st.session_state.get("question_input", ""),
        placeholder="Например: Магнит ГМ 120г картофельные чипсы за последний период",
        height=100,
        key="question_textarea",
    )
    submitted = st.form_submit_button("🚀 Построить отчёт", type="primary")


# ============================================================
# ОБРАБОТКА
# ============================================================

if submitted:
    if not question.strip():
        st.warning("Введи вопрос.")
    else:
        run_question = question.strip()
        success = False

        progress = st.progress(0.0)
        status = st.empty()

        def cb(msg, pct):
            try:
                progress.progress(min(pct, 1.0))
                status.markdown(f"### {msg}")
            except Exception:
                pass

        try:
            report = build_report(run_question, progress_callback=cb)
            st.session_state["reports"].insert(0, report)
            progress.empty()
            status.empty()
            st.session_state["question_input"] = ""
            success = True
        except Exception as e:
            progress.empty()
            status.error(f"❌ Ошибка построения отчёта: {e}")

        if success:
            st.session_state["show_success_toast"] = True
            st.rerun()


# ============================================================
# РЕНДЕР ОТЧЁТОВ
# ============================================================

st.divider()


def render_report(report, r_idx):
    st.header(f"📊 Отчёт: {report['request']}")

    n_sections = len(report["sections"])
    n_success = sum(1 for s in report["sections"] if not s.get("error"))
    timings = report.get("timings", {})

    info_line = (
        f"✅ Секций: {n_success}/{n_sections} · "
        f"🤖 Планировщик: `{report.get('planner_model', 'unknown')}` · "
        f"⏱ Время: {timings.get('total', 0):.1f}с "
        f"(план: {timings.get('planning', 0):.1f}с, "
        f"профиль: {timings.get('profiling', 0):.1f}с, "
        f"секции: {timings.get('sections', 0):.1f}с)"
    )
    st.caption(info_line)

    # Разрешённый относительный период
    period_info = report.get("period_info")
    if period_info and period_info.get("description"):
        st.info(f"📅 Разрешённый период: **{period_info['description']}**")

    filters = report.get("filters") or {}

    # === КНОПКИ ГЕНЕРАЦИИ ===
    gen_col1, gen_col2, _ = st.columns([2, 2, 3])

    with gen_col1:
        excel_key = f"gen_excel_{r_idx}"
        if st.button(
            "📥 Сформировать Excel",
            key=excel_key,
            type="primary",
            use_container_width=True,
        ):
            with st.spinner("Формирую Excel..."):
                try:
                    hierarchy = report.get("excel_hierarchy") or []
                    excel_bytes = build_excel_bytes(filters, excel_hierarchy=hierarchy)
                    st.session_state[f"excel_bytes_{r_idx}"] = excel_bytes
                    hint = f"структура: {' → '.join(hierarchy)}" if hierarchy else "стандартная структура"
                    st.success(
                        f"✅ Готово! Размер: {len(excel_bytes) / 1024:.1f} KB · {hint}"
                    )
                except Exception as e:
                    st.error(f"Ошибка: {e}")

        if f"excel_bytes_{r_idx}" in st.session_state:
            st.download_button(
                label="⬇️ Скачать Excel",
                data=st.session_state[f"excel_bytes_{r_idx}"],
                file_name=f"report_{r_idx}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_excel_{r_idx}",
                use_container_width=True,
            )

    with gen_col2:
        pptx_key = f"gen_pptx_{r_idx}"
        if st.button(
            "🎨 Сформировать презентацию",
            key=pptx_key,
            type="primary",
            use_container_width=True,
        ):
            with st.spinner("Формирую PPTX-презентацию..."):
                try:
                    pptx_bytes = build_pptx_bytes(report)
                    st.session_state[f"pptx_bytes_{r_idx}"] = pptx_bytes
                    st.success(f"✅ Готово! Размер: {len(pptx_bytes) / 1024:.1f} KB")
                except Exception as e:
                    st.error(f"Ошибка: {e}")

        if f"pptx_bytes_{r_idx}" in st.session_state:
            st.download_button(
                label="⬇️ Скачать PPTX",
                data=st.session_state[f"pptx_bytes_{r_idx}"],
                file_name=f"report_{r_idx}.pptx",
                mime="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                key=f"dl_pptx_{r_idx}",
                use_container_width=True,
            )

    # Применённые фильтры
    active = {k: v for k, v in filters.items() if v is not None and v != []}
    if active:
        with st.expander("🎯 Применённые фильтры", expanded=True):
            filters_df = pd.DataFrame([active])
            render_kpi_cards(filters_df, columns_per_row=4)

    # Секции
    for s_idx, section in enumerate(report["sections"]):
        with st.container(border=True):
            st.subheader(f"{s_idx + 1}. {section['title']}")

            if section.get("time_sec"):
                st.caption(f"⏱ {section['time_sec']:.2f}с")

            if section.get("error"):
                st.error(f"Ошибка: {section['error']}")
                with st.expander("Вопрос секции"):
                    st.write(section["question"])
                continue

            df = section["data"]

            if section["chart_type"] == "kpi" and df is not None and len(df) == 1:
                render_kpi_cards(df, columns_per_row=4)
            else:
                if df is not None:
                    st.write(f"📊 Строк: **{len(df)}**")
                    st.dataframe(df, use_container_width=True, hide_index=True)

                if section.get("fig") is not None:
                    st.plotly_chart(
                        section["fig"],
                        use_container_width=True,
                        key=f"report_chart_{r_idx}_{s_idx}",
                    )

                    png_bytes = fig_to_png_bytes(section["fig"])
                    if png_bytes:
                        st.download_button(
                            label="🖼 Скачать PNG",
                            data=png_bytes,
                            file_name=f"{r_idx}_{s_idx}_{section['title'][:30]}.png",
                            mime="image/png",
                            key=f"png_r_{r_idx}_{s_idx}",
                        )

            if section.get("sql"):
                with st.expander("🧾 SQL"):
                    st.code(section["sql"], language="sql")

            if df is not None and not df.empty:
                st.download_button(
                    label="📥 Excel",
                    data=to_excel(df),
                    file_name=f"section_{r_idx}_{s_idx}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_r_{r_idx}_{s_idx}",
                )


# ============================================================
# ОТРИСОВКА С АВТОСКРОЛЛОМ
# ============================================================

if st.session_state["reports"]:
    st.markdown('<div id="latest-report"></div>', unsafe_allow_html=True)

    for r_idx, report in enumerate(st.session_state["reports"]):
        render_report(report, r_idx)
        if r_idx < len(st.session_state["reports"]) - 1:
            st.divider()

    st.markdown(
        """
        <script>
            setTimeout(function() {
                const el = window.parent.document.getElementById('latest-report');
                if (el) {
                    el.scrollIntoView({behavior: 'smooth', block: 'start'});
                }
            }, 500);
        </script>
        """,
        unsafe_allow_html=True,
    )
else:
    st.info(
        "👆 Опиши какой отчёт тебе нужен и нажми **Построить отчёт**.\n\n"
        "Примеры:\n"
        "- 📅 «Магнит за последний месяц»\n"
        "- 🎯 «Ашан ГМ 120г картофельные чипсы, приоритет вкус краб»\n"
        "- 📊 «Полный отчёт по Пятерочке за 2026 год»"
    )