import streamlit as st
from agent import ask
from charts import auto_chart
from utils import to_excel

st.set_page_config(
    page_title="Sales AI Agent",
    page_icon="📊",
    layout="wide",
)

st.title("📊 Sales Analytics AI Agent")
st.caption("AI-агент для аналитики продаж: вопрос → SQL → ClickHouse → таблица/график.")

with st.sidebar:
    st.header("Примеры вопросов")

    examples = [
        "Топ-10 товаров по продажам",
        "Продажи по городам",
        "Продажи по брендам",
        "Топ-10 городов по количеству продаж",
        "Средняя цена продажи по категориям product_category_3",
        "Продажи по дням",
        "Продажи по неделям",
        "Топ-10 магазинов по продажам",
    ]

    for ex in examples:
        if st.button(ex, use_container_width=True):
            st.session_state["question_input"] = ex

    st.divider()
    st.caption("⚠️ Агент выполняет только SELECT-запросы.")

if "history" not in st.session_state:
    st.session_state["history"] = []

question = st.text_input(
    "Задай вопрос:",
    value=st.session_state.get("question_input", ""),
    placeholder="Например: Топ-10 товаров по продажам",
)

if st.button("🚀 Выполнить", type="primary"):
    if not question.strip():
        st.warning("Введи вопрос.")
    else:
        with st.spinner("Генерирую SQL и выполняю запрос..."):
            try:
                result = ask(question)
                st.session_state["history"].insert(0, result)
            except Exception as e:
                st.session_state["history"].insert(0, {
                    "question": question,
                    "error": str(e),
                })

for idx, item in enumerate(st.session_state["history"]):
    with st.container(border=True):
        st.subheader(f"❓ {item['question']}")

        if "error" in item:
            st.error(item["error"])
            continue

        st.caption(f"Модель: `{item['model']}`")

        with st.expander("SQL", expanded=False):
            st.code(item["sql"], language="sql")

        df = item["data"]

        st.write(f"Строк: **{len(df)}**")
        st.dataframe(df, use_container_width=True, hide_index=True)

        col1, col2 = st.columns([1, 4])

        with col1:
            st.download_button(
                label="📥 Excel",
                data=to_excel(df),
                file_name=f"result_{idx}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key=f"download_{idx}",
            )

        fig = auto_chart(df)
        if fig is not None:
            st.plotly_chart(fig, use_container_width=True)