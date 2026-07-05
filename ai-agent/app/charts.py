"""Построение графиков разных типов по DataFrame."""
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def build_chart(df: pd.DataFrame, chart_type: str = "auto", title: str = ""):
    """Строит график указанного типа."""
    if df is None or df.empty:
        return None

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    non_numeric_cols = [c for c in df.columns if c not in numeric_cols]

    if chart_type == "kpi":
        return None  # KPI рендерим отдельно как метрики в UI

    if not numeric_cols:
        return None

    # Auto = определяем тип по данным
    if chart_type == "auto":
        chart_type = _guess_chart_type(df, numeric_cols, non_numeric_cols)

    try:
        if chart_type == "line" and non_numeric_cols:
            x_col = _find_time_col(df) or non_numeric_cols[0]
            y_col = numeric_cols[0]
            fig = px.line(df, x=x_col, y=y_col, title=title, markers=True)
            fig.update_layout(template="plotly_dark", height=400)
            return fig

        if chart_type == "pie" and non_numeric_cols:
            names_col = non_numeric_cols[0]
            values_col = numeric_cols[0]
            top = df.nlargest(15, values_col) if len(df) > 15 else df
            fig = px.pie(top, names=names_col, values=values_col, title=title, hole=0.4)
            fig.update_layout(template="plotly_dark", height=500)
            return fig

        if chart_type == "heatmap":
            # Нужно pivot: 2 категории + 1 число
            if len(non_numeric_cols) >= 2 and numeric_cols:
                pivot = df.pivot_table(
                    index=non_numeric_cols[0],
                    columns=non_numeric_cols[1],
                    values=numeric_cols[0],
                    aggfunc="sum",
                    fill_value=0,
                )
                fig = px.imshow(pivot, aspect="auto", title=title, color_continuous_scale="Viridis")
                fig.update_layout(template="plotly_dark", height=500)
                return fig
            # fallback → bar
            chart_type = "bar"

        if chart_type == "table":
            return None  # таблицу и так показываем

        # Дефолт — bar
        if non_numeric_cols and numeric_cols:
            x_col = non_numeric_cols[0]
            y_col = numeric_cols[0]
            if df[x_col].nunique() > 50:
                return None
            plot_df = df.head(30)
            fig = px.bar(plot_df, x=x_col, y=y_col, title=title)
            fig.update_layout(template="plotly_dark", height=400, xaxis_tickangle=-30)
            return fig

    except Exception as e:
        print(f"[CHART] Failed to build {chart_type}: {e}")
        return None

    return None


def _find_time_col(df: pd.DataFrame) -> str | None:
    """Ищет колонку с датой/временем."""
    for col in df.columns:
        lower = col.lower()
        if any(k in lower for k in ["date", "month", "year", "day", "week", "period"]):
            return col
    return None


def _guess_chart_type(df, numeric_cols, non_numeric_cols) -> str:
    if _find_time_col(df):
        return "line"
    if non_numeric_cols and numeric_cols and df[non_numeric_cols[0]].nunique() <= 15:
        return "pie" if len(df) <= 10 else "bar"
    return "bar"


# Обратная совместимость
def auto_chart(df):
    return build_chart(df, chart_type="auto")