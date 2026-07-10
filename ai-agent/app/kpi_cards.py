"""HTML KPI-карточки в стиле dark BI-отчётов."""
import streamlit as st
import pandas as pd


BG_DARK = "#0d1117"
CARD_BG = "#161b22"
TEXT_MAIN = "#f0f6fc"
TEXT_SUB = "#8b949e"

ACCENT_COLORS = [
    "#ff6b6b", "#00d2ff", "#ffd93d", "#c084fc",
    "#ff9f43", "#6bcb77", "#4d9de0", "#ff6eb4", "#1a73e8",
]

COLUMN_TRANSLATIONS = {
    "revenue": "Выручка", "revenue_rub": "Выручка", "total_revenue": "Общая выручка",
    "sales_amount_rub": "Выручка", "qty": "Штук", "quantity": "Штук",
    "sales_quantity": "Штук", "total_qty": "Штук",
    "avg_price": "Средняя цена", "avg_price_per_unit": "Средняя цена за шт",
    "average_price": "Средняя цена", "avg_sell_price": "Средняя цена продажи",
    "avg_cost_price": "Средняя себестоимость", "avg_cost": "Средняя себестоимость",
    "cost": "Себестоимость", "cost_rub": "Себестоимость",
    "margin": "Маржа", "margin_rub": "Маржа", "margin_pct": "Маржа %",
    "tt_count": "Торговых точек", "stores": "Магазинов", "stores_count": "Магазинов",
    "brands": "Брендов", "brands_count": "Брендов",
    "flavors": "Вкусов", "flavors_count": "Вкусов",
    "products": "Товаров", "products_count": "Товаров", "sku_count": "SKU",
    "regions": "Регионов", "cities": "Городов",
    "retail_chain": "Сеть", "store_format": "Формат магазина",
    "chip_type": "Тип чипсов", "year": "Год", "month": "Месяц",
    "weight_grams": "Граммовка", "weight_grams_list": "Граммовки",
    "priority_flavors": "Приоритетные вкусы", "brand": "Бренд", "flavor": "Вкус",
    "top_brand": "Топ-1 бренд", "top_flavor": "Топ-1 вкус",
    "top_chain": "Топ-1 сеть", "top_region": "Топ-1 регион", "top_city": "Топ-1 город",
}


def translate(col_name: str) -> str:
    return COLUMN_TRANSLATIONS.get(col_name.lower(), col_name)


def _should_skip_kpi(col_name: str, value) -> bool:
    try:
        if value is None or pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass

    col_lower = col_name.lower()

    skip_if_one = ("tt_count", "stores", "stores_count", "brands", "brands_count",
                   "flavors", "flavors_count", "products", "products_count")
    if col_lower in skip_if_one:
        try:
            if float(value) <= 1:
                return True
        except (ValueError, TypeError):
            pass

    if isinstance(value, str) and value.strip() in ("", "Не указано", "None"):
        return True

    return False


def fmt_value(val, col_name=""):
    if isinstance(val, (list, tuple)):
        if len(val) == 0:
            return "—"
        items = [str(v) for v in val[:3]]
        result = ", ".join(items)
        if len(val) > 3:
            result += f" +{len(val) - 3}"
        return result

    try:
        if val is None or pd.isna(val):
            return "—"
    except (TypeError, ValueError):
        pass

    col_lower = col_name.lower()

    if any(k in col_lower for k in ["revenue", "rub", "amount", "cost", "margin_rub", "price"]) and "pct" not in col_lower:
        try:
            n = float(val)
            if n >= 1_000_000_000:
                return f"{n/1_000_000_000:.2f} млрд ₽"
            if n >= 1_000_000:
                return f"{n/1_000_000:.2f} млн ₽"
            if n >= 1_000:
                return f"{n/1_000:.1f} тыс ₽"
            return f"{n:.2f} ₽"
        except (ValueError, TypeError):
            return str(val)

    if "pct" in col_lower or col_lower.endswith("_pct"):
        try:
            return f"{float(val):.1f}%"
        except (ValueError, TypeError):
            return str(val)

    if any(k in col_lower for k in ["qty", "quantity", "count", "stores", "brands", "flavors", "products", "regions", "cities"]):
        try:
            n = float(val)
            if n >= 1_000_000:
                return f"{n/1_000_000:.1f} млн"
            if n >= 1_000:
                return f"{n:,.0f}".replace(",", " ")
            return f"{int(n)}"
        except (ValueError, TypeError):
            return str(val)

    if isinstance(val, (int, float)):
        try:
            if val >= 1000:
                return f"{val:,.2f}".replace(",", " ")
            return f"{val:.2f}" if isinstance(val, float) else str(val)
        except Exception:
            return str(val)

    return str(val)


def render_kpi_cards(df: pd.DataFrame, columns_per_row: int = 4):
    if df is None or df.empty:
        st.info("Нет данных для KPI")
        return

    row = df.iloc[0]
    cols_list = [c for c in df.columns if not _should_skip_kpi(c, row[c])]

    if not cols_list:
        st.info("Нет значимых KPI для отображения")
        return

    for start in range(0, len(cols_list), columns_per_row):
        chunk = cols_list[start : start + columns_per_row]
        cols = st.columns(len(chunk))

        for i, col_name in enumerate(chunk):
            with cols[i]:
                val = row[col_name]
                color = ACCENT_COLORS[(start + i) % len(ACCENT_COLORS)]
                formatted = fmt_value(val, col_name)
                label = translate(col_name)

                st.markdown(
                    f"""
                    <div style="
                        background: {CARD_BG};
                        border: 2px solid {color};
                        border-radius: 14px;
                        padding: 0;
                        margin: 6px 0;
                        overflow: hidden;
                        box-shadow: 0 2px 8px rgba(0,0,0,0.3);
                    ">
                        <div style="background: {color}; height: 8px; width: 100%;"></div>
                        <div style="padding: 20px 18px 16px 18px;">
                            <div style="
                                color: {TEXT_SUB};
                                font-size: 12px;
                                text-transform: uppercase;
                                letter-spacing: 0.5px;
                                margin-bottom: 8px;
                                font-weight: 500;
                            ">{label}</div>
                            <div style="
                                color: {color};
                                font-size: 26px;
                                font-weight: 800;
                                line-height: 1.2;
                                word-break: break-word;
                            ">{formatted}</div>
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )