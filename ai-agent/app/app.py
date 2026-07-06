"""Streamlit UI — Sales Analytics AI Agent."""
import time

import streamlit as st
import pandas as pd

from agent import ask
from report import build_report
from charts import build_chart, fig_to_png_bytes
from utils import to_excel
from excel_report import build_excel_bytes
from kpi_cards import render_kpi_cards


st.set_page_config(
    page_title="Sales AI Agent",
    page_icon="📊",
    layout="wide",
)


# ============================================================
# STATE INIT
# ============================================================

if "history" not in st.session_state:
    st.session_state["history"] = []
if "reports" not in st.session_state:
    st.session_state["reports"] = []
if "mode" not in st.session_state:
    st.session_state["mode"] = "💬 Chat — быстрый ответ"
if "question_input" not in st.session_state:
    st.session_state["question_input"] = ""


# ============================================================
# HEADER
# ============================================================

st.title("📊 Sales Analytics AI Agent")
st.caption(
    "AI-агент для аналитики продаж чипсов и снеков в 19 розничных сетях. "
    "Задай вопрос на русском — я сгенерирую SQL, выполню его в ClickHouse и покажу результат."
)


# Toast о готовности
if st.session_state.pop("show_success_toast", False):
    st.toast("✅ Готово! Прокрути вниз ⬇️", icon="🎉")


# ============================================================
# РЕЖИМ
# ============================================================

mode = st.radio(
    "Режим",
    ["💬 Chat — быстрый ответ", "📊 Report — детальный отчёт"],
    horizontal=True,
    label_visibility="collapsed",
    key="mode_selector",
    index=0 if st.session_state["mode"].startswith("💬") else 1,
)
st.session_state["mode"] = mode
is_report_mode = mode.startswith("📊")


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:
    st.header("💡 Примеры")

    if is_report_mode:
        examples = [
            "Магнит ГМ 120г картофельные чипсы 2026, полный анализ по территориям с приоритетными вкусами (Сметана и лук, Морепродукты, Томат)",
            "Полный отчёт по продажам Lay's за 2025 год",
            "Аналитика всех сетей по картофельным чипсам за 2026 год",
            "Детальный анализ бренда Pringles по регионам и вкусам",
            "Отчёт по сети Пятерочка: динамика, топ товаров, категории",
            "Картофельные чипсы 70 120 140 180 220 225 250 грамм: цена, себестоимость, цена за грамм, нормализация к 120г",
        ]
    else:
        examples = [
            "Топ-10 товаров по продажам",
            "Топ-10 брендов по выручке",
            "Топ-5 сетей по обороту",
            "Продажи по месяцам",
            "Средний чек по сетям",
            "Продажи Lay's в Москве",
            "Топ-10 товаров в Пятерочке",
            "Промо-продажи по сетям",
            "Магнит ГМ 120г картофельные чипсы вкус сметана и лук",
            "Цена за 1 грамм по брендам для картофельных чипсов",
            "Нормализованная цена к 120 грамм по брендам",
            "Картофельные чипсы 70 120 140 грамм по вкусам",
        ]

    for ex in examples:
        if st.button(ex, use_container_width=True, key=f"ex_{hash(ex)}"):
            st.session_state["question_input"] = ex
            st.rerun()

    st.divider()
    st.caption("⚠️ Агент выполняет только SELECT-запросы.")

    if st.button("🗑 Очистить историю", use_container_width=True):
        st.session_state["history"] = []
        st.session_state["reports"] = []
        st.rerun()


# ============================================================
# ФОРМА ВВОДА
# ============================================================

with st.form(key="query_form", clear_on_submit=False):
    question = st.text_area(
        "Задай вопрос:",
        value=st.session_state.get("question_input", ""),
        placeholder=(
            "Например: Магнит ГМ 120г картофельные чипсы полный анализ"
            if is_report_mode
            else "Например: Топ-10 товаров по продажам"
        ),
        height=80,
        key="question_textarea",
    )
    submitted = st.form_submit_button("🚀 Выполнить", type="primary")


# ============================================================
# ОБРАБОТКА
# ============================================================

if submitted:
    if not question.strip():
        st.warning("Введи вопрос.")
    else:
        run_as_report = is_report_mode
        run_question = question.strip()
        success = False

        if run_as_report:
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

        else:
            with st.spinner("🤖 Читаю запрос → генерирую SQL → выполняю в ClickHouse..."):
                chat_start = time.time()
                try:
                    result = ask(run_question)
                    result["time_sec"] = time.time() - chat_start
                    st.session_state["history"].insert(0, result)
                    st.session_state["question_input"] = ""
                    success = True
                except Exception as e:
                    st.session_state["history"].insert(0, {
                        "question": run_question,
                        "error": str(e),
                        "time_sec": time.time() - chat_start,
                    })
                    success = True

        if success:
            st.session_state["show_success_toast"] = True
            st.rerun()


# ============================================================
# РЕНДЕР
# ============================================================

st.divider()


def render_chat_item(item, idx):
    with st.container(border=True):
        st.subheader(f"❓ {item['question']}")

        if "error" in item:
            st.error(item["error"])
            if item.get("time_sec"):
                st.caption(f"⏱ {item['time_sec']:.1f}с")
            return

        badges = [f"🤖 `{item['model']}`"]
        if item.get("corrected"):
            badges.append("🔧 Self-corrected")
        if item.get("attempts"):
            badges.append(f"🔁 Попыток: {len(item['attempts'])}")
        if item.get("time_sec"):
            badges.append(f"⏱ {item['time_sec']:.1f}с")
        st.caption(" · ".join(badges))

        ents = item.get("entities") or {}
        found = []
        if ents.get("brands"):
            found.append(f"🏷 Бренды: {', '.join(ents['brands'][:5])}")
        if ents.get("chains"):
            found.append(f"🏪 Сети: {', '.join(ents['chains'])}")
        if ents.get("flavors"):
            found.append(f"👅 Вкусы: {', '.join(ents['flavors'][:5])}")
        if ents.get("formats"):
            found.append(f"📦 Форматы: {', '.join(ents['formats'])}")
        if ents.get("chip_types"):
            found.append(f"🥔 Типы: {', '.join(ents['chip_types'])}")

        if found:
            with st.expander("🔍 Распознанные сущности"):
                for f in found:
                    st.write(f)

        metrics_info = item.get("metrics_info") or {}
        if metrics_info.get("metrics") or metrics_info.get("grams_list") or metrics_info.get("target_grams"):
            with st.expander("📐 Распознанные метрики"):
                for m in metrics_info.get("metrics", []):
                    st.markdown(f"**{m['name']}** (`{m['key']}`)")
                    st.code(m["formula"], language="sql")
                    st.caption(m["description"])
                    st.divider()
                if metrics_info.get("grams_list"):
                    st.info(f"⚙️ Граммовки: {', '.join(str(g) for g in metrics_info['grams_list'])}г")
                if metrics_info.get("target_grams"):
                    st.info(f"🎯 Целевая граммовка: {metrics_info['target_grams']}г")

        with st.expander("🧾 SQL"):
            st.code(item["sql"], language="sql")

        if item.get("attempts") and len(item["attempts"]) > 1:
            with st.expander("🐛 История попыток"):
                for a in item["attempts"]:
                    st.write(f"**{a['step']}** — модель `{a['model']}`")
                    st.code(a["sql"], language="sql")
                    if a.get("error"):
                        st.warning(a["error"][:300])
                    st.divider()

        df = item["data"]
        st.write(f"📊 Строк: **{len(df)}**")
        st.dataframe(df, use_container_width=True, hide_index=True)

        col1, col2 = st.columns([1, 5])
        with col1:
            st.download_button(
                label="📥 Excel",
                data=to_excel(df),
                file_name=f"result_{idx}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"dl_{idx}",
            )

        fig = build_chart(df, chart_type="auto")
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True, key=f"chat_chart_{idx}")
            png_bytes = fig_to_png_bytes(fig)
            if png_bytes:
                st.download_button(
                    label="🖼 Скачать PNG",
                    data=png_bytes,
                    file_name=f"chart_{idx}.png",
                    mime="image/png",
                    key=f"png_chat_{idx}",
                )


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

    filters = report.get("filters") or {}
    excel_col1, _ = st.columns([2, 5])
    with excel_col1:
        excel_key = f"gen_excel_{r_idx}"
        if st.button(
            "📥 Сформировать полный Excel-отчёт",
            key=excel_key,
            type="primary",
            use_container_width=True,
        ):
            with st.spinner("Формирую Excel..."):
                try:
                    # Передаём иерархию из пожеланий пользователя
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

    active = {k: v for k, v in filters.items() if v is not None and v != []}
    if active:
        with st.expander("🎯 Применённые фильтры", expanded=True):
            filters_df = pd.DataFrame([active])
            render_kpi_cards(filters_df, columns_per_row=4)

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

if is_report_mode:
    if st.session_state["reports"]:
        st.markdown('<div id="latest-report"></div>', unsafe_allow_html=True)

        for r_idx, report in enumerate(st.session_state["reports"]):
            render_report(report, r_idx)
            if r_idx < len(st.session_state["reports"]) - 1:
                st.divider()

        # Автоскролл к последнему отчёту
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
        st.info("👆 Задай вопрос выше и нажми **Выполнить**, чтобы построить отчёт.")
else:
    if st.session_state["history"]:
        st.markdown('<div id="latest-chat"></div>', unsafe_allow_html=True)

        for idx, item in enumerate(st.session_state["history"]):
            render_chat_item(item, idx)

        st.markdown(
            """
            <script>
                setTimeout(function() {
                    const el = window.parent.document.getElementById('latest-chat');
                    if (el) {
                        el.scrollIntoView({behavior: 'smooth', block: 'start'});
                    }
                }, 500);
            </script>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.info("👆 Задай вопрос выше и нажми **Выполнить**, чтобы получить ответ.")