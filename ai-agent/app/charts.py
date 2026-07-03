import pandas as pd
import plotly.express as px


def auto_chart(df: pd.DataFrame):
    if df is None or df.empty:
        return None

    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
    non_numeric_cols = [c for c in df.columns if c not in numeric_cols]

    if not numeric_cols:
        return None

    # Если есть дата
    date_like_cols = [c for c in df.columns if "date" in c.lower() or "month" in c.lower()]
    if date_like_cols and numeric_cols:
        x_col = date_like_cols[0]
        y_col = numeric_cols[0]
        return px.line(df, x=x_col, y=y_col, title=f"{y_col} по {x_col}")

    # Категория + число
    if non_numeric_cols and numeric_cols:
        x_col = non_numeric_cols[0]
        y_col = numeric_cols[0]

        chart_df = df.head(30)
        return px.bar(chart_df, x=x_col, y=y_col, title=f"{y_col} по {x_col}")

    return None