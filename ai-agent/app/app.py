"""Streamlit UI — Sales Analytics AI Agent."""
import streamlit as st

from agent import ask
from report import build_report
from charts import build_chart
from utils import to_excel
from excel_report import build_excel_bytes


# ============================================================
# КОНФИГ
# ============================================================

st.set_page_config(
    page_title="Sales AI Agent",
    page_icon="📊",
    layout="wide",
)


# ============================================================
# ЗАГОЛОВОК
# ============================================================

st.title("📊 Sales Analytics AI Agent")
st.caption(
    "AI-агент для аналитики продаж чипсов и снеков в 19 розничных сетях. "
    "Задай вопрос на русском — я сгенерирую SQL, выполню его в ClickHouse и покажу результат."
)


# ============================================================
# РЕЖИМ РАБОТЫ
# ============================================================

mode = st.radio(
    "Режим",
    ["💬 Chat — быстрый ответ", "📊 Report — детальный отчёт"],
    horizontal=True,
    label_visibility="collapsed",
)

is_report_mode = mode.startswith("📊")


# ============================================================
# SIDEBAR — ПРИМЕРЫ
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

    st.divider()

    st.caption("⚠️ Агент выполняет только SELECT-запросы.")

    if st.button("🗑 Очистить историю", use_container_width=True):
        st.session_state["history"] = []
        st.session_state["reports"] = []
        st.rerun()


# ============================================================
# ИНИЦИАЛИЗАЦИЯ STATE
# ============================================================

if "history" not in st.session_state:
    st.session_state["history"] = []

if "reports" not in st.session_state:
    st.session_state["reports"] = []


# ============================================================
# ПОЛЕ ВВОДА
# ============================================================

question = st.text_area(
    "Задай вопрос:",
    value=st.session_state.get("question_input", ""),
    placeholder=(
        "Например: Магнит ГМ 120г картофельные чипсы полный анализ"
        if is_report_mode
        else "Например: Топ-10 товаров по продажам"
    ),
    height=80,
)


# ============================================================
# КНОПКА ВЫПОЛНЕНИЯ
# ============================================================

if st.button("🚀 Выполнить", type="primary"):
    if not question.strip():
        st.warning("Введи вопрос.")
    else:
        if is_report_mode:
            # === REPORT MODE ===
            progress = st.progress(0.0)
            status = st.empty()

            def cb(msg, pct):
                progress.progress(pct)
                status.info(msg)

            try:
                report = build_report(question, progress_callback=cb)
                st.session_state["reports"].insert(0, report)
                progress.empty()
                status.empty()
            except Exception as e:
                progress.empty()
                status.error(f"Ошибка построения отчёта: {e}")
        else:
            # === CHAT MODE ===
            with st.spinner("🤖 Генерирую SQL..."):
                try:
                    result = ask(question)
                    st.session_state["history"].insert(0, result)
                except Exception as e:
                    st.session_state["history"].insert(0, {
                        "question": question,
                        "error": str(e),
                    })


# ============================================================
# CHAT MODE — ВЫВОД ИСТОРИИ
# ============================================================

if not is_report_mode:
    for idx, item in enumerate(st.session_state["history"]):
        with st.container(border=True):
            st.subheader(f"❓ {item['question']}")

            # Ошибка
            if "error" in item:
                st.error(item["error"])
                continue

            # Бейджи
            badges = [f"🤖 `{item['model']}`"]
            if item.get("corrected"):
                badges.append("🔧 Self-corrected")
            if item.get("attempts"):
                badges.append(f"🔁 Попыток: {len(item['attempts'])}")
            st.caption(" · ".join(badges))

            # Распознанные сущности
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

            # Распознанные бизнес-метрики
            metrics_info = item.get("metrics_info") or {}
            if metrics_info.get("metrics") or metrics_info.get("grams_list") or metrics_info.get("target_grams"):
                with st.expander("📐 Распознанные метрики"):
                    for m in metrics_info.get("metrics", []):
                        st.markdown(f"**{m['name']}** (`{m['key']}`)")
                        st.code(m["formula"], language="sql")
                        st.caption(m["description"])
                        st.divider()
                    if metrics_info.get("grams_list"):
                        st.info(
                            f"⚙️ Граммовки: "
                            f"{', '.join(str(g) for g in metrics_info['grams_list'])}г"
                        )
                    if metrics_info.get("target_grams"):
                        st.info(f"🎯 Целевая граммовка: {metrics_info['target_grams']}г")

            # SQL
            with st.expander("🧾 SQL"):
                st.code(item["sql"], language="sql")

            # История попыток self-correction
            if item.get("attempts") and len(item["attempts"]) > 1:
                with st.expander("🐛 История попыток"):
                    for a in item["attempts"]:
                        st.write(f"**{a['step']}** — модель `{a['model']}`")
                        st.code(a["sql"], language="sql")
                        if a.get("error"):
                            st.warning(a["error"][:300])
                        st.divider()

            # Таблица
            df = item["data"]
            st.write(f"📊 Строк: **{len(df)}**")
            st.dataframe(df, use_container_width=True, hide_index=True)

            # Кнопки
            col1, col2 = st.columns([1, 5])
            with col1:
                st.download_button(
                    label="📥 Excel",
                    data=to_excel(df),
                    file_name=f"result_{idx}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_{idx}",
                )

            # График
            fig = build_chart(df, chart_type="auto")
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True, key=f"chat_chart_{idx}")


# ============================================================
# REPORT MODE — ВЫВОД ОТЧЁТОВ
# ============================================================

else:
    for r_idx, report in enumerate(st.session_state["reports"]):
        st.divider()
        st.header(f"📊 Отчёт: {report['request']}")

        n_sections = len(report["sections"])
        n_success = sum(1 for s in report["sections"] if not s.get("error"))
        st.caption(
            f"Секций: {n_success}/{n_sections} успешно · "
            f"Планировщик: `{report.get('planner_model', 'unknown')}`"
        )

        # === КНОПКА ПОЛНОГО EXCEL-ОТЧЁТА ===
        filters = report.get("filters") or {}
        excel_col1, excel_col2 = st.columns([2, 5])
        with excel_col1:
            # Генерируем Excel по запросу пользователя
            excel_key = f"gen_excel_{r_idx}"
            if st.button("📥 Сформировать полный Excel-отчёт", key=excel_key, type="primary", use_container_width=True):
                with st.spinner("Формирую Excel..."):
                    try:
                        excel_bytes = build_excel_bytes(filters)
                        st.session_state[f"excel_bytes_{r_idx}"] = excel_bytes
                        st.success(f"✅ Готово! Размер: {len(excel_bytes) / 1024:.1f} KB")
                    except Exception as e:
                        st.error(f"Ошибка: {e}")

            # Кнопка скачивания появляется после генерации
            if f"excel_bytes_{r_idx}" in st.session_state:
                st.download_button(
                    label="⬇️ Скачать Excel",
                    data=st.session_state[f"excel_bytes_{r_idx}"],
                    file_name=f"report_{r_idx}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    key=f"dl_excel_{r_idx}",
                    use_container_width=True,
                )

        # Показываем какие фильтры применились
        active = {k: v for k, v in filters.items() if v is not None and v != []}
        if active:
            with st.expander("🎯 Применённые фильтры", expanded=True):
                cols = st.columns(3)
                items = list(active.items())
                for i, (k, v) in enumerate(items):
                    with cols[i % 3]:
                        st.metric(k, str(v))

        for s_idx, section in enumerate(report["sections"]):
            with st.container(border=True):
                st.subheader(f"{s_idx + 1}. {section['title']}")

                # Ошибка секции
                if section.get("error"):
                    st.error(f"Ошибка: {section['error']}")
                    with st.expander("Вопрос секции"):
                        st.write(section["question"])
                    continue

                df = section["data"]

                # KPI-режим: показываем как метрики
                if section["chart_type"] == "kpi" and df is not None and len(df) == 1:
                    n_cols = min(len(df.columns), 5)
                    cols = st.columns(n_cols)
                    for i, col_name in enumerate(df.columns):
                        with cols[i % n_cols]:
                            val = df.iloc[0, i]
                            if isinstance(val, (int, float)):
                                try:
                                    st.metric(col_name, f"{val:,.2f}".replace(",", " "))
                                except Exception:
                                    st.metric(col_name, str(val))
                            else:
                                st.metric(col_name, str(val))
                else:
                    # Таблица + график
                    if df is not None:
                        st.write(f"📊 Строк: **{len(df)}**")
                        st.dataframe(df, use_container_width=True, hide_index=True)

                    if section.get("fig") is not None:
                        st.plotly_chart(
                            section["fig"],
                            use_container_width=True,
                            key=f"report_chart_{r_idx}_{s_idx}",
                        )

                # SQL
                if section.get("sql"):
                    with st.expander("🧾 SQL"):
                        st.code(section["sql"], language="sql")

                # Excel
                if df is not None and not df.empty:
                    st.download_button(
                        label="📥 Excel",
                        data=to_excel(df),
                        file_name=f"section_{r_idx}_{s_idx}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        key=f"dl_r_{r_idx}_{s_idx}",
                    )