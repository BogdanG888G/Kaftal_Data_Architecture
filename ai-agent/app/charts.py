"""Построение красивых графиков в стиле dark BI-отчётов."""
import io
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


# ============================================================
# ЦВЕТОВАЯ ТЕМА (та же что в твоих matplotlib отчётах)
# ============================================================

BG_DARK = "#0d1117"
CARD_BG = "#161b22"
GRID_COLOR = "#21262d"
TEXT_MAIN = "#f0f6fc"
TEXT_SUB = "#8b949e"

ACCENT = {
    "red": "#ff6b6b",
    "teal": "#00d2ff",
    "yellow": "#ffd93d",
    "purple": "#c084fc",
    "orange": "#ff9f43",
    "green": "#6bcb77",
    "blue": "#4d9de0",
    "pink": "#ff6eb4",
}

# Градиентные палитры по типу графика
GRADIENT_REVENUE = ["#4d9de0", "#00d2ff", "#6bcb77", "#ffd93d"]
GRADIENT_QTY = ["#c084fc", "#ff6eb4", "#ff6b6b", "#ff9f43"]
GRADIENT_GRAMS = ["#c084fc", "#4d9de0", "#00d2ff", "#6bcb77"]
GRADIENT_FLAVORS = ["#1a73e8", "#ffd93d", "#6bcb77", "#00d2ff"]
GRADIENT_CITIES = ["#ff6b6b", "#1a73e8", "#ffd93d"]

# Фирменные цвета сетей
CHAIN_COLORS = {
    "Магнит": "#e30613",
    "Пятерочка": "#ff8c00",
    "Пятёрочка": "#ff8c00",
    "Самокат": "#ff3366",
    "Лента": "#0d68b0",
    "Перекресток": "#00b14f",
    "Перекрёсток": "#00b14f",
    "Дикси": "#d4145a",
    "Окей": "#ffc107",
    "Ашан": "#9c27b0",
    "Глобус": "#1a73e8",
    "Красное и Белое": "#c2185b",
    "Чижик": "#ffeb3b",
    "Верный": "#2196f3",
    "ВкусВилл": "#4caf50",
    "Бристоль": "#795548",
}


# ============================================================
# ФОРМАТИРОВАНИЕ ЧИСЕЛ
# ============================================================

def fmt_rub_full(x):
    if pd.isna(x) or x == 0:
        return "0 ₽"
    if x >= 1_000_000_000:
        return f"{x/1_000_000_000:.2f} млрд ₽"
    if x >= 1_000_000:
        return f"{x/1_000_000:.2f} млн ₽"
    if x >= 1_000:
        return f"{x/1_000:.1f} тыс ₽"
    return f"{x:.0f} ₽"


def fmt_int(x):
    if pd.isna(x) or x == 0:
        return "0"
    if x >= 1_000_000:
        return f"{x/1_000_000:.1f} млн"
    if x >= 1_000:
        return f"{x/1_000:.1f} тыс"
    return f"{x:.0f}"


def cut_label(s, n=45):
    s = str(s)
    return s if len(s) <= n else s[: n - 1] + "…"


# ============================================================
# УТИЛИТЫ
# ============================================================

def _gradient_colors(base_palette, n):
    """Создаёт градиент из n цветов на основе палитры."""
    if n <= 0:
        return []
    if n == 1:
        return [base_palette[0]]

    # Линейная интерполяция между цветами палитры
    def hex_to_rgb(h):
        h = h.lstrip("#")
        return tuple(int(h[i : i + 2], 16) for i in (0, 2, 4))

    def rgb_to_hex(rgb):
        return "#{:02x}{:02x}{:02x}".format(
            max(0, min(255, int(rgb[0]))),
            max(0, min(255, int(rgb[1]))),
            max(0, min(255, int(rgb[2]))),
        )

    result = []
    seg = (len(base_palette) - 1) / max(n - 1, 1)
    for i in range(n):
        pos = i * seg
        lo = int(pos)
        hi = min(lo + 1, len(base_palette) - 1)
        t = pos - lo
        c1 = hex_to_rgb(base_palette[lo])
        c2 = hex_to_rgb(base_palette[hi])
        mix = tuple(c1[j] * (1 - t) + c2[j] * t for j in range(3))
        result.append(rgb_to_hex(mix))
    return result


def _apply_dark_layout(fig, title=""):
    """Применяет тёмную тему к фигуре."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=BG_DARK,
        plot_bgcolor=CARD_BG,
        font=dict(family="Arial, sans-serif", color=TEXT_MAIN, size=12),
        title=dict(
            text=title,
            font=dict(size=18, color=TEXT_MAIN, family="Arial Black"),
            x=0.02,
            xanchor="left",
        ),
        margin=dict(l=20, r=20, t=60, b=40),
        xaxis=dict(
            gridcolor=GRID_COLOR,
            zerolinecolor=GRID_COLOR,
            tickcolor=TEXT_SUB,
            tickfont=dict(color=TEXT_SUB),
        ),
        yaxis=dict(
            gridcolor=GRID_COLOR,
            zerolinecolor=GRID_COLOR,
            tickcolor=TEXT_SUB,
            tickfont=dict(color=TEXT_SUB),
        ),
        legend=dict(
            bgcolor=CARD_BG,
            bordercolor=GRID_COLOR,
            font=dict(color=TEXT_MAIN),
        ),
    )
    return fig


def _find_time_col(df):
    for col in df.columns:
        lower = col.lower()
        if any(k in lower for k in ["date", "month", "year", "day", "week", "period"]):
            return col
    return None


def _find_metric_col(df, prefer=None):
    """Находит первую числовую колонку. prefer — приоритетная."""
    numeric = df.select_dtypes(include=["number"]).columns.tolist()
    if not numeric:
        return None
    if prefer:
        for p in prefer:
            for col in numeric:
                if p.lower() in col.lower():
                    return col
    return numeric[0]


# ============================================================
# ТИПЫ ГРАФИКОВ
# ============================================================

def chart_bar_horizontal(df, title="", category_col=None, value_col=None, palette=None):
    """
    Горизонтальный бар-чарт с подписями стоимости и процентов.
    Стиль как в matplotlib-отчётах.
    """
    if df.empty:
        return None

    if category_col is None:
        non_num = [c for c in df.columns if c not in df.select_dtypes(include=["number"]).columns]
        if not non_num:
            return None
        category_col = non_num[0]

    if value_col is None:
        value_col = _find_metric_col(df, prefer=["revenue", "sales", "amount"])
        if value_col is None:
            return None

    # Ограничим количество строк
    plot_df = df.head(25).copy()

    # Сортируем по возрастанию (чтобы самый большой был вверху при horizontal)
    plot_df = plot_df.sort_values(value_col, ascending=True)

    # Обрезаем длинные названия
    plot_df[category_col] = plot_df[category_col].apply(lambda x: cut_label(x, 45))

    # Цвета
    if palette is None:
        palette = GRADIENT_REVENUE
    colors = _gradient_colors(palette, len(plot_df))

    # Подписи на барах
    total = plot_df[value_col].sum()
    is_money = "revenue" in value_col.lower() or "rub" in value_col.lower() or "amount" in value_col.lower()

    labels = []
    for _, row in plot_df.iterrows():
        val = row[value_col]
        pct = val / total * 100 if total > 0 else 0
        val_str = fmt_rub_full(val) if is_money else fmt_int(val)

        # Дополнительные метрики
        extras = []
        if "qty" in plot_df.columns and value_col != "qty":
            extras.append(f"{fmt_int(row['qty'])} шт")
        if "tt_count" in plot_df.columns:
            extras.append(f"{int(row['tt_count'])} ТТ")

        extras_str = "  •  " + "  •  ".join(extras) if extras else ""
        labels.append(f"  {val_str}  •  {pct:.1f}%{extras_str}")

    fig = go.Figure()
    fig.add_trace(
        go.Bar(
            y=plot_df[category_col],
            x=plot_df[value_col],
            orientation="h",
            marker=dict(color=colors, line=dict(color=BG_DARK, width=1)),
            text=labels,
            textposition="outside",
            textfont=dict(color=TEXT_MAIN, size=11),
            hovertemplate="<b>%{y}</b><br>Значение: %{x:,.0f}<extra></extra>",
        )
    )

    max_val = plot_df[value_col].max()
    fig.update_xaxes(range=[0, max_val * 1.5], showgrid=True)
    fig.update_yaxes(showgrid=False)

    _apply_dark_layout(fig, title=title.upper())
    fig.update_layout(
        height=max(400, 30 * len(plot_df) + 100),
        showlegend=False,
    )
    return fig


def chart_line(df, title="", x_col=None, y_col=None, metric_filter=None):
    """
    Линейный график.
    Если metric_filter указан ('revenue' / 'qty' / 'avg_price') — рисует одну линию.
    Иначе — все метрики на одном графике.
    """
    if df.empty:
        return None

    if x_col is None:
        x_col = _find_time_col(df) or df.columns[0]

    plot_df = df.copy()

    # Склеиваем year + month
    if "year" in plot_df.columns and "month" in plot_df.columns:
        month_order = {
            "Январь": 1, "Февраль": 2, "Март": 3, "Апрель": 4,
            "Май": 5, "Июнь": 6, "Июль": 7, "Август": 8,
            "Сентябрь": 9, "Октябрь": 10, "Ноябрь": 11, "Декабрь": 12,
        }
        plot_df["_month_num"] = plot_df["month"].map(month_order).fillna(0).astype(int)
        plot_df = plot_df.sort_values(["year", "_month_num"])
        plot_df["_period"] = plot_df["month"].astype(str) + " " + plot_df["year"].astype(str)
        x_col = "_period"

    # Ищем колонки
    revenue_col = None
    qty_col = None
    price_col = None

    for col in plot_df.columns:
        lc = col.lower()
        if not revenue_col and ("revenue" in lc or "amount_rub" in lc):
            revenue_col = col
        if not qty_col and lc in ("qty", "quantity", "sales_quantity", "total_qty"):
            qty_col = col
        if not price_col and ("avg_price" in lc or "average_price" in lc):
            price_col = col

    # Определяем что рисовать
    if metric_filter == "revenue" and revenue_col:
        return _single_line_chart(
            plot_df, x_col, revenue_col, "Выручка",
            ACCENT["red"], "money", title,
        )
    if metric_filter == "qty" and qty_col:
        return _single_line_chart(
            plot_df, x_col, qty_col, "Количество",
            ACCENT["teal"], "int", title,
        )
    if metric_filter == "avg_price" and price_col:
        return _single_line_chart(
            plot_df, x_col, price_col, "Средняя цена",
            ACCENT["yellow"], "money_short", title,
        )

    # Иначе все на одном — но лучше отдельно
    if revenue_col:
        return _single_line_chart(
            plot_df, x_col, revenue_col, "Выручка",
            ACCENT["red"], "money", title,
        )

    y_col = _find_metric_col(plot_df, prefer=["revenue", "sales"])
    if y_col is None:
        return None
    return _single_line_chart(plot_df, x_col, y_col, y_col, ACCENT["blue"], "money", title)


def _single_line_chart(plot_df, x_col, y_col, name, color, fmt_type, title):
    """Один линейный график с одной метрикой."""

    def fmt(val):
        if fmt_type == "money":
            return fmt_rub_full(val)
        if fmt_type == "money_short":
            return f"{val:.0f} ₽"
        if fmt_type == "int":
            return fmt_int(val)
        return str(val)

    values = plot_df[y_col]
    text_labels = [fmt(v) for v in values]

    rgb = tuple(int(color.lstrip("#")[j : j + 2], 16) for j in (0, 2, 4))

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=plot_df[x_col],
            y=values,
            mode="lines+markers+text",
            name=name,
            line=dict(color=color, width=3),
            marker=dict(
                size=14,
                color=color,
                line=dict(color=BG_DARK, width=2),
            ),
            fill="tozeroy",
            fillcolor=f"rgba({rgb[0]},{rgb[1]},{rgb[2]},0.18)",
            text=text_labels,
            textposition="top center",
            textfont=dict(color=color, size=11, family="Arial Black"),
            hovertemplate=f"<b>%{{x}}</b><br>{name}: %{{y:,.2f}}<extra></extra>",
        )
    )

    _apply_dark_layout(fig, title=title.upper())

    # Много места по бокам чтобы влезли подписи
    fig.update_layout(
        height=450,
        showlegend=False,
        margin=dict(l=80, r=80, t=90, b=100),
    )
    fig.update_xaxes(tickangle=-30, automargin=True)
    # Расширим Y-ось чтобы подписи над точками влезли
    max_y = values.max()
    fig.update_yaxes(range=[0, max_y * 1.25])
    return fig


def chart_pie(df, title="", names_col=None, values_col=None):
    """Donut-чарт с процентами и общей суммой в центре."""
    if df.empty:
        return None

    if names_col is None:
        non_num = [c for c in df.columns if c not in df.select_dtypes(include=["number"]).columns]
        if not non_num:
            return None
        names_col = non_num[0]

    if values_col is None:
        values_col = _find_metric_col(df, prefer=["revenue", "sales", "amount"])
        if values_col is None:
            return None

    # Топ-10 + "Прочие"
    df_sorted = df.sort_values(values_col, ascending=False)
    if len(df_sorted) > 10:
        top10 = df_sorted.head(10).copy()
        other_val = df_sorted.iloc[10:][values_col].sum()
        top10 = pd.concat(
            [top10, pd.DataFrame({names_col: ["Прочие"], values_col: [other_val]})],
            ignore_index=True,
        )
        plot_df = top10
    else:
        plot_df = df_sorted

    total = plot_df[values_col].sum()

    palette = [
        ACCENT["red"], ACCENT["orange"], ACCENT["yellow"], ACCENT["green"],
        ACCENT["teal"], ACCENT["blue"], ACCENT["purple"], ACCENT["pink"],
        "#9ca3af", "#64748b", "#374151",
    ]
    colors = palette[: len(plot_df)]

    fig = go.Figure()
    fig.add_trace(
        go.Pie(
            labels=plot_df[names_col],
            values=plot_df[values_col],
            hole=0.55,
            marker=dict(colors=colors, line=dict(color=BG_DARK, width=2)),
            textposition="inside",
            textinfo="percent",
            textfont=dict(color="white", size=13, family="Arial Black"),
            hovertemplate="<b>%{label}</b><br>%{value:,.0f} ₽<br>%{percent}<extra></extra>",
        )
    )

    # Текст в центре
    fig.add_annotation(
        text=f"<b>{fmt_rub_full(total)}</b>",
        x=0.5, y=0.5,
        font=dict(size=22, color=ACCENT["orange"], family="Arial Black"),
        showarrow=False,
    )

    _apply_dark_layout(fig, title=title.upper())
    fig.update_layout(
        height=550,
        showlegend=True,
        legend=dict(
            orientation="v",
            yanchor="middle",
            y=0.5,
            xanchor="left",
            x=1.02,
            font=dict(size=11),
        ),
    )
    return fig


def chart_lollipop(df, title="", category_col=None, value_col=None):
    """
    Lollipop chart для ТОП SKU.
    Подписи справа от точек, много места по бокам.
    """
    if df.empty:
        return None

    non_num = [c for c in df.columns if c not in df.select_dtypes(include=["number"]).columns]

    if category_col is None:
        if len(non_num) >= 2:
            category_col = "_label"
            df = df.copy()
            df[category_col] = df[non_num[0]].astype(str) + " · " + df[non_num[1]].astype(str)
        elif non_num:
            category_col = non_num[0]
        else:
            return None

    if value_col is None:
        value_col = _find_metric_col(df, prefer=["revenue", "sales"])
        if value_col is None:
            return None

    plot_df = df.head(20).copy()
    plot_df = plot_df.sort_values(value_col, ascending=True)
    plot_df[category_col] = plot_df[category_col].apply(lambda x: cut_label(x, 80))

    colors = _gradient_colors(
        [ACCENT["blue"], ACCENT["purple"], ACCENT["pink"], "#1a73e8"],
        len(plot_df),
    )
    is_money = "revenue" in value_col.lower() or "rub" in value_col.lower()

    fig = go.Figure()

    # Линии от 0 до точки
    for i, (_, row) in enumerate(plot_df.iterrows()):
        fig.add_shape(
            type="line",
            x0=0, x1=row[value_col],
            y0=row[category_col], y1=row[category_col],
            line=dict(color=GRID_COLOR, width=2),
        )

    # Подписи справа от точек
    labels = []
    total = plot_df[value_col].sum()
    for _, row in plot_df.iterrows():
        val_str = fmt_rub_full(row[value_col]) if is_money else fmt_int(row[value_col])
        pct = row[value_col] / total * 100 if total > 0 else 0
        extras = []
        if "qty" in plot_df.columns and value_col != "qty":
            extras.append(f"{fmt_int(row['qty'])} шт")
        extras_str = " · " + " · ".join(extras) if extras else ""
        labels.append(f"  {val_str} · {pct:.1f}%{extras_str}")

    fig.add_trace(
        go.Scatter(
            x=plot_df[value_col],
            y=plot_df[category_col],
            mode="markers+text",
            marker=dict(
                size=20,
                color=colors,
                line=dict(color="white", width=1.5),
                symbol="circle",
            ),
            text=labels,
            textposition="middle right",
            textfont=dict(color=TEXT_MAIN, size=11, family="Arial Black"),
            hovertemplate="<b>%{y}</b><br>%{x:,.0f}<extra></extra>",
            cliponaxis=False,  # разрешаем текст выходить за оси
        )
    )

    max_val = plot_df[value_col].max()

    # Существенно расширяем x-ось чтобы подписи справа влезли
    fig.update_xaxes(
        range=[0, max_val * 2.5],
        showgrid=True,
        automargin=True,
    )
    fig.update_yaxes(
        showgrid=False,
        automargin=True,
        tickfont=dict(size=11),
    )

    _apply_dark_layout(fig, title=title.upper())

    # КЛЮЧЕВОЕ: большая высота, широкие margin слева и справа
    fig.update_layout(
        height=max(750, 42 * len(plot_df) + 200),
        showlegend=False,
        margin=dict(l=40, r=100, t=100, b=60),
        uniformtext_minsize=10,
        uniformtext_mode='show',
    )
    return fig


def chart_grouped_bar_prices(df, title=""):
    """
    Grouped bar: цена продажи vs себестоимость по брендам + подписи маржи.
    """
    if df.empty:
        return None

    cat_col = None
    for c in df.columns:
        if c.lower() in ("brand", "бренд", "product", "product_name"):
            cat_col = c
            break
    if cat_col is None:
        non_num = [c for c in df.columns if c not in df.select_dtypes(include=["number"]).columns]
        if not non_num:
            return None
        cat_col = non_num[0]

    sell_col = None
    cost_col = None
    for c in df.columns:
        lc = c.lower()
        if "sell" in lc or "цена" in lc or "продаж" in lc:
            sell_col = c
        if "cost" in lc or "себест" in lc or "закупк" in lc:
            cost_col = c

    if sell_col is None:
        return chart_bar_horizontal(df, title=title, category_col=cat_col)

    plot_df = df.head(15).copy()
    plot_df = plot_df.sort_values(sell_col, ascending=True)

    fig = go.Figure()

    # Бары себестоимости
    if cost_col:
        fig.add_trace(
            go.Bar(
                y=plot_df[cat_col],
                x=plot_df[cost_col],
                orientation="h",
                name="Себестоимость",
                marker=dict(color=ACCENT["red"], line=dict(color=BG_DARK, width=1)),
                text=[f"{v:.0f} ₽" for v in plot_df[cost_col]],
                textposition="outside",
                textfont=dict(color=ACCENT["red"], size=10),
            )
        )

    # Бары цены продажи + подписи маржи
    sell_labels = []
    for _, row in plot_df.iterrows():
        sell = row[sell_col]
        label = f"{sell:.0f} ₽"
        if cost_col:
            cost = row[cost_col] or 0
            if cost > 0 and sell > cost:
                margin = sell - cost
                margin_pct = margin / sell * 100
                label = f"{sell:.0f} ₽  ·  💚 маржа {margin:.0f} ₽ ({margin_pct:.0f}%)"
        sell_labels.append(label)

    fig.add_trace(
        go.Bar(
            y=plot_df[cat_col],
            x=plot_df[sell_col],
            orientation="h",
            name="Цена продажи",
            marker=dict(color=ACCENT["teal"], line=dict(color=BG_DARK, width=1)),
            text=sell_labels,
            textposition="outside",
            textfont=dict(color=ACCENT["green"], size=11, family="Arial Black"),
        )
    )

    max_val = plot_df[sell_col].max()
    fig.update_xaxes(range=[0, max_val * 1.9], showgrid=True)
    fig.update_yaxes(showgrid=False, automargin=True)

    _apply_dark_layout(fig, title=title.upper())
    fig.update_layout(
        height=max(600, 45 * len(plot_df) + 120),
        barmode="group",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        margin=dict(l=20, r=280, t=100, b=40),
    )
    return fig


def chart_heatmap(df, title=""):
    """Heatmap для двух категорий + одна метрика."""
    if df.empty:
        return None

    non_num = [c for c in df.columns if c not in df.select_dtypes(include=["number"]).columns]
    numeric = df.select_dtypes(include=["number"]).columns.tolist()

    if len(non_num) < 2 or not numeric:
        return None

    idx, cols, val = non_num[0], non_num[1], numeric[0]

    pivot = df.pivot_table(index=idx, columns=cols, values=val, aggfunc="sum", fill_value=0)
    pivot = pivot.loc[pivot.sum(1).sort_values(ascending=False).index]
    pivot = pivot[pivot.sum(0).sort_values(ascending=False).index]

    # Отображаем в тыс. ₽ если очень большие числа
    divisor = 1000 if pivot.values.max() > 100_000 else 1
    unit = "тыс ₽" if divisor > 1 else "₽"

    fig = go.Figure(
        data=go.Heatmap(
            z=pivot.values / divisor,
            x=pivot.columns,
            y=pivot.index,
            colorscale=[
                [0, CARD_BG],
                [0.2, ACCENT["blue"]],
                [0.5, ACCENT["teal"]],
                [0.75, ACCENT["yellow"]],
                [1, "#1a73e8"],
            ],
            text=[[f"{v:.0f}" if v > 0 else "" for v in row] for row in pivot.values / divisor],
            texttemplate="%{text}",
            textfont=dict(color="white", size=10, family="Arial Black"),
            colorbar=dict(title=unit, tickfont=dict(color=TEXT_SUB)),
            hovertemplate="<b>%{y}</b> × <b>%{x}</b><br>%{z:.0f} " + unit + "<extra></extra>",
        )
    )

    _apply_dark_layout(fig, title=title.upper())
    fig.update_layout(
        height=max(500, 30 * len(pivot) + 150),
        xaxis=dict(tickangle=-30),
    )
    return fig


# ============================================================
# УМНЫЙ ВЫБОР ГРАФИКА
# ============================================================

def build_chart(df, chart_type="auto", title=""):
    """Строит график указанного типа."""
    if df is None or df.empty:
        return None

    if chart_type == "kpi":
        return None  # KPI рендерится как metrics в UI

    non_num = [c for c in df.columns if c not in df.select_dtypes(include=["number"]).columns]
    numeric = df.select_dtypes(include=["number"]).columns.tolist()

    if not numeric:
        return None

    # Определим палитру по контексту заголовка
    title_lower = title.lower()
    if "город" in title_lower or "cit" in title_lower:
        palette = GRADIENT_CITIES
    elif "вкус" in title_lower or "flavor" in title_lower:
        palette = GRADIENT_FLAVORS
    elif "грамм" in title_lower or "gram" in title_lower:
        palette = GRADIENT_GRAMS
    elif "штук" in title_lower or "qty" in title_lower or "количеств" in title_lower:
        palette = GRADIENT_QTY
    else:
        palette = GRADIENT_REVENUE

    try:
        if chart_type == "auto":
            time_col = _find_time_col(df)
            if time_col:
                chart_type = "line"
            elif non_num and numeric and df[non_num[0]].nunique() <= 12:
                chart_type = "pie" if len(df) <= 12 else "bar"
            else:
                chart_type = "bar"

        if chart_type == "line":
            return chart_line(df, title=title)

        if chart_type == "pie":
            return chart_pie(df, title=title)

        if chart_type == "heatmap":
            return chart_heatmap(df, title=title)

        if chart_type == "lollipop":
            return chart_lollipop(df, title=title)

        if chart_type == "grouped_bar":
            return chart_grouped_bar_prices(df, title=title)

        # Default — bar
        return chart_bar_horizontal(df, title=title, palette=palette)

    except Exception as e:
        print(f"[CHART] Failed to build {chart_type}: {e}")
        # Fallback на bar
        try:
            return chart_bar_horizontal(df, title=title, palette=palette)
        except Exception:
            return None


# Обратная совместимость
def auto_chart(df):
    return build_chart(df, chart_type="auto")


# ============================================================
# ЭКСПОРТ В PNG
# ============================================================

def fig_to_png_bytes(fig, width=1400, height=None):
    """Конвертирует Plotly-фигуру в PNG bytes."""
    if fig is None:
        return None
    try:
        h = height or fig.layout.height or 600
        return fig.to_image(format="png", width=width, height=h, scale=2)
    except Exception as e:
        print(f"[CHART] PNG export failed: {e}")
        return None