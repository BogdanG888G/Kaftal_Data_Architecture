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


# Переводы названий колонок на русский
COLUMN_TRANSLATIONS = {
    # Метрики
    "revenue": "Выручка",
    "revenue_rub": "Выручка",
    "total_revenue": "Общая выручка",
    "sales_amount_rub": "Выручка",
    "qty": "Штук",
    "quantity": "Штук",
    "sales_quantity": "Штук",
    "total_qty": "Штук",
    "avg_price": "Средняя цена",
    "avg_price_per_unit": "Средняя цена за шт",
    "average_price": "Средняя цена",
    "avg_sell_price": "Средняя цена продажи",
    "avg_cost_price": "Средняя себестоимость",
    "cost": "Себестоимость",
    "cost_rub": "Себестоимость",
    "total_cost": "Общая себестоимость",
    "margin": "Маржа",
    "margin_rub": "Маржа",
    "total_margin": "Общая маржа",
    "total_margin_rub": "Маржа",
    "margin_pct": "Маржа %",

    # Количественные
    "tt_count": "Торговых точек",
    "stores": "Магазинов",
    "stores_count": "Магазинов",
    "brands": "Брендов",
    "brands_count": "Брендов",
    "flavors": "Вкусов",
    "flavors_count": "Вкусов",
    "products": "Товаров",
    "products_count": "Товаров",
    "sku_count": "SKU",
    "regions": "Регионов",
    "cities": "Городов",

    # Фильтры
    "retail_chain": "Сеть",
    "store_format": "Формат магазина",
    "chip_type": "Тип чипсов",
    "year": "Год",
    "month": "Месяц",
    "weight_grams": "Граммовка",
    "weight_grams_list": "Граммовки",
    "priority_flavors": "Приоритетные вкусы",
    "brand": "Бренд",
    "flavor": "Вкус",
}


def translate(col_name: str) -> str:
    """Переводит имя колонки на русский, если есть перевод."""
    return COLUMN_TRANSLATIONS.get(col_name.lower(), col_name)


def fmt_value(val, col_name=""):
    """Умное форматирование значения."""
    # Список / массив — форматируем как список
    if isinstance(val, (list, tuple)):
        if len(val) == 0:
            return "—"
        # Показываем первые 3 элемента, дальше — многоточие
        items = [str(v) for v in val[:3]]
        result = ", ".join(items)
        if len(val) > 3:
            result += f" +{len(val) - 3}"
        return result

    # NaN / None
    try:
        if val is None:
            return "—"
        if pd.isna(val):
            return "—"
    except (TypeError, ValueError):
        # pd.isna не работает с массивами/списками
        pass

    col_lower = col_name.lower()

    # Деньги
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

    # Проценты
    if "pct" in col_lower or col_lower.endswith("_pct"):
        try:
            return f"{float(val):.1f}%"
        except (ValueError, TypeError):
            return str(val)

    # Штуки / количества
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

    # Число вообще
    if isinstance(val, (int, float)):
        try:
            if val >= 1000:
                return f"{val:,.2f}".replace(",", " ")
            return f"{val:.2f}" if isinstance(val, float) else str(val)
        except Exception:
            return str(val)

    return str(val)


def render_kpi_cards(df: pd.DataFrame, columns_per_row: int = 4):
    """Рендерит красивые KPI-карточки из первой строки DataFrame."""
    if df is None or df.empty:
        st.info("Нет данных для KPI")
        return

    row = df.iloc[0]
    cols_list = list(df.columns)

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