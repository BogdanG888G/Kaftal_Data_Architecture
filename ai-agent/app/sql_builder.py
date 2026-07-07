"""Построение SQL из структурированных параметров плана."""
from config import config


TABLE = f"{config.CH_DATABASE}.{config.CH_TABLE}"


def build_where(filters: dict, extra_filter: dict = None) -> str:
    """Строит WHERE-условие из dict фильтров."""
    conditions = []

    # Объединяем базовые и extra фильтры
    all_filters = dict(filters or {})
    if extra_filter:
        for k, v in extra_filter.items():
            all_filters[k] = v

    if all_filters.get("retail_chain"):
        conditions.append(f"retail_chain = '{escape(all_filters['retail_chain'])}'")

    if all_filters.get("store_format"):
        conditions.append(f"store_format = '{escape(all_filters['store_format'])}'")

    if all_filters.get("chip_type"):
        conditions.append(f"chip_type = '{escape(all_filters['chip_type'])}'")

    if all_filters.get("year") is not None:
        conditions.append(f"year = {int(all_filters['year'])}")

    # НОВОЕ: месяц
    if all_filters.get("month"):
        conditions.append(f"month = '{escape(all_filters['month'])}'")

    if all_filters.get("weight_grams") is not None:
        w = int(all_filters["weight_grams"])
        conditions.append(f"toInt32(toFloat64OrZero(toString(weight_grams))) = {w}")

    if all_filters.get("weight_grams_list"):
        grams = ", ".join(str(int(g)) for g in all_filters["weight_grams_list"])
        conditions.append(f"toInt32(toFloat64OrZero(toString(weight_grams))) IN ({grams})")

    if all_filters.get("brands"):
        vals = ", ".join(f"'{escape(b)}'" for b in all_filters["brands"])
        conditions.append(f"brand IN ({vals})")

    if all_filters.get("flavors"):
        vals = ", ".join(f"'{escape(f)}'" for f in all_filters["flavors"])
        conditions.append(f"flavor IN ({vals})")

    # extra_filter: одиночное значение
    if extra_filter:
        for k, v in extra_filter.items():
            if k in ("retail_chain", "store_format", "chip_type", "year",
                     "weight_grams", "brand", "flavor"):
                if isinstance(v, str):
                    conditions.append(f"{k} = '{escape(v)}'")
                elif isinstance(v, int):
                    conditions.append(f"{k} = {v}")

    if not conditions:
        return ""

    return "WHERE " + "\n  AND ".join(conditions)


def escape(value: str) -> str:
    """Простая экранизация одинарных кавычек."""
    return str(value).replace("'", "''")


def build_filters_hint(filters: dict, extra_filter: dict = None) -> str:
    """
    Возвращает подсказку для LLM в человеческом виде.
    Используется когда мы всё равно генерим SQL через LLM.
    """
    where = build_where(filters, extra_filter)
    if not where:
        return ""
    return f"\n\n=== ГОТОВЫЕ ФИЛЬТРЫ (ОБЯЗАТЕЛЬНО ИСПОЛЬЗУЙ ИМЕННО ЭТИ) ===\n{where}\n"


# ============================================================
# Готовые SQL-шаблоны для типовых секций
# ============================================================

def sql_kpi(filters: dict) -> str:
    """Универсальный KPI-запрос."""
    where = build_where(filters)
    return f"""SELECT
    round(SUM(sales_amount_rub), 2) AS revenue,
    SUM(sales_quantity) AS qty,
    COUNT(DISTINCT address) AS tt_count,
    COUNT(DISTINCT store_code) AS stores,
    COUNT(DISTINCT brand) AS brands,
    COUNT(DISTINCT flavor) AS flavors,
    round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_price
FROM {TABLE}
{where}
LIMIT 1"""


def sql_group_bar(
    filters: dict,
    group_col: str,
    limit: int = 25,
    extra_filter: dict = None,
) -> str:
    """SELECT по одному разрезу с базовыми метриками."""
    where = build_where(filters, extra_filter)
    # Убираем NULL/пустые в group_col
    null_filter = f"AND {group_col} IS NOT NULL AND toString({group_col}) != ''"
    if where:
        where = where + "\n  " + null_filter
    else:
        where = "WHERE " + null_filter.lstrip("AND ").strip()

    return f"""SELECT
    {group_col},
    round(SUM(sales_amount_rub), 2) AS revenue,
    SUM(sales_quantity) AS qty,
    COUNT(DISTINCT address) AS tt_count
FROM {TABLE}
{where}
GROUP BY {group_col}
ORDER BY revenue DESC
LIMIT {limit}"""


def sql_time_series(
    filters: dict,
    time_col: str = "year_month",
    extra_filter: dict = None,
) -> str:
    """Динамика по времени с несколькими метриками."""
    where = build_where(filters, extra_filter)

    if time_col == "year_month":
        select_time = "year, month"
        group_time = "year, month"
        order_time = "year, min(toDate(date))"
    elif time_col == "date":
        select_time = "date"
        group_time = "date"
        order_time = "date"
    elif time_col == "week_num":
        select_time = "year, week_num"
        group_time = "year, week_num"
        order_time = "year, week_num"
    elif time_col == "month_start":
        select_time = "toStartOfMonth(toDate(date)) AS month_start"
        group_time = "month_start"
        order_time = "month_start"
    else:
        select_time = time_col
        group_time = time_col
        order_time = time_col

    return f"""SELECT
    {select_time},
    round(SUM(sales_amount_rub), 2) AS revenue,
    SUM(sales_quantity) AS qty,
    round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_price
FROM {TABLE}
{where}
GROUP BY {group_time}
ORDER BY {order_time}
LIMIT 500"""


def sql_multi_group(
    filters: dict,
    group_cols: list,
    limit: int = 200,
    extra_filter: dict = None,
) -> str:
    """SELECT по нескольким разрезам одновременно."""
    where = build_where(filters, extra_filter)
    group_str = ", ".join(group_cols)

    # Фильтр NULL для основной колонки
    if group_cols:
        main_col = group_cols[0]
        null_filter = f"{main_col} IS NOT NULL AND toString({main_col}) != ''"
        if where:
            where = where + f"\n  AND {null_filter}"
        else:
            where = f"WHERE {null_filter}"

    return f"""SELECT
    {group_str},
    round(SUM(sales_amount_rub), 2) AS revenue,
    SUM(sales_quantity) AS qty,
    COUNT(DISTINCT address) AS tt_count
FROM {TABLE}
{where}
GROUP BY {group_str}
ORDER BY revenue DESC
LIMIT {limit}"""

def build_sql_for_section(section: dict, filters: dict) -> str:
    """
    Строит SQL для секции. Использует готовый шаблон.
    """
    chart_type = section.get("chart_type", "bar")
    group_by = section.get("group_by") or []
    extra_filter = section.get("extra_filter") or {}

    # === KPI ===
    if chart_type == "kpi" or not group_by:
        return sql_kpi({**filters, **extra_filter})

    # === Small multiples — топ вкусов по каждому бренду ===
    if chart_type == "small_multiples":
        return sql_small_multiples_brand_flavor(filters, extra_filter)

    # === Grouped bar — цена vs себестоимость по бренду ===
    if chart_type == "grouped_bar":
        return sql_price_vs_cost(filters, group_by[0], extra_filter)

    # === Lollipop — топ SKU (обычно brand + product_name) ===
    if chart_type == "lollipop":
        return sql_top_sku(filters, group_by, limit=20, extra_filter=extra_filter)

    # === Time series ===
    time_cols = {"date", "year", "month", "week_num", "month_start"}
    if group_by and any(g in time_cols for g in group_by):
        if "date" in group_by:
            return sql_time_series(filters, "date", extra_filter)
        if "week_num" in group_by:
            return sql_time_series(filters, "week_num", extra_filter)
        if "month_start" in group_by:
            return sql_time_series(filters, "month_start", extra_filter)
        if "year" in group_by and "month" in group_by:
            return sql_time_series(filters, "year_month", extra_filter)
        return sql_time_series(filters, group_by[0], extra_filter)

    # === Один разрез ===
    if len(group_by) == 1:
        limit = 30 if chart_type == "pie" else 50
        return sql_group_bar(filters, group_by[0], limit=limit, extra_filter=extra_filter)

    # === Несколько разрезов ===
    return sql_multi_group(filters, group_by, limit=200, extra_filter=extra_filter)


def sql_price_vs_cost(filters: dict, group_col: str, extra_filter: dict = None) -> str:
    """Grouped bar: цена продажи vs себестоимость."""
    where = build_where(filters, extra_filter)
    null_filter = f"AND {group_col} IS NOT NULL AND toString({group_col}) != ''"
    if where:
        where = where + "\n  " + null_filter
    else:
        where = "WHERE " + null_filter.lstrip("AND ").strip()

    return f"""SELECT
    {group_col},
    round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_sell_price,
    round(SUM(sales_cost_price) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_cost_price,
    round(SUM(sales_amount_rub), 2) AS revenue
FROM {TABLE}
{where}
GROUP BY {group_col}
ORDER BY revenue DESC
LIMIT 15"""


def sql_top_sku(filters: dict, group_by: list, limit: int = 20, extra_filter: dict = None) -> str:
    """Lollipop: топ SKU с несколькими группировками."""
    where = build_where(filters, extra_filter)
    group_str = ", ".join(group_by)

    if group_by:
        main_col = group_by[0]
        null_filter = f"{main_col} IS NOT NULL AND toString({main_col}) != ''"
        if where:
            where = where + f"\n  AND {null_filter}"
        else:
            where = f"WHERE {null_filter}"

    return f"""SELECT
    {group_str},
    round(SUM(sales_amount_rub), 2) AS revenue,
    SUM(sales_quantity) AS qty
FROM {TABLE}
{where}
GROUP BY {group_str}
ORDER BY revenue DESC
LIMIT {limit}"""

def sql_small_multiples_brand_flavor(filters: dict, extra_filter: dict = None) -> str:
    """
    SQL для small multiples: топ-9 брендов, у каждого топ-7 вкусов.
    """
    where = build_where(filters, extra_filter)

    # Общий фильтр непустых brand/flavor
    extra_conds = [
        "brand IS NOT NULL",
        "toString(brand) != ''",
        "toString(brand) != 'Не указано'",
        "flavor IS NOT NULL",
        "toString(flavor) != ''",
        "toString(flavor) != 'Не указано'",
    ]

    if where:
        where_full = where + "\n  AND " + "\n  AND ".join(extra_conds)
    else:
        where_full = "WHERE " + "\n  AND ".join(extra_conds)

    return f"""WITH top_brands AS (
    SELECT brand
    FROM {TABLE}
    {where_full}
    GROUP BY brand
    ORDER BY SUM(sales_amount_rub) DESC
    LIMIT 9
)
SELECT
    brand,
    flavor,
    round(SUM(sales_amount_rub), 2) AS revenue,
    SUM(sales_quantity) AS qty
FROM {TABLE}
{where_full}
  AND brand IN (SELECT brand FROM top_brands)
GROUP BY brand, flavor
ORDER BY brand, revenue DESC
LIMIT 500"""