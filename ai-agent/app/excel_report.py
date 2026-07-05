"""Формирование полного Excel-отчёта по фильтрам plan'а."""
import io
import pandas as pd
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

from database import db
from sql_builder import build_where
from config import config


TABLE = f"{config.CH_DATABASE}.{config.CH_TABLE}"


# Иерархия колонок: приоритет сверху вниз
# Формат: (клюъ в БД, отображаемое имя)
HIERARCHY_COLUMNS = [
    ("retail_chain", "Сеть"),
    ("region_name", "Регион"),
    ("city_name", "Город"),
    ("brand", "Бренд"),
    ("flavor", "Вкус"),
    ("weight_grams", "Граммовка"),
]

# Метрики
METRIC_COLUMNS = [
    ("avg_cost", "Средняя себестоимость"),
    ("avg_sell", "Средняя цена продажи"),
    ("turnover_rub", "Товарооборот руб (с НДС)"),
    ("turnover_pcs", "Товарооборот шт"),
]


def build_report_sql(filters: dict) -> str:
    """
    Строит SQL для полного отчёта.
    Группируем по всей иерархии, считаем метрики.
    """
    where = build_where(filters)

    # Формируем список колонок для SELECT и GROUP BY
    dim_cols_sql = []
    dim_cols_names = []

    for col_key, col_name in HIERARCHY_COLUMNS:
        if col_key == "weight_grams":
            # Приводим String → Float
            dim_cols_sql.append(f"toFloat64OrNull(toString({col_key})) AS {col_key}")
        else:
            # Для строковых — просто as-is
            dim_cols_sql.append(f"ifNull({col_key}, 'Не указано') AS {col_key}")
        dim_cols_names.append(col_key)

    dim_select = ",\n    ".join(dim_cols_sql)
    dim_group = ", ".join(dim_cols_names)

    return f"""SELECT
    {dim_select},
    avg(average_cost_price)                              AS avg_cost,
    avg(average_sell_price)                              AS avg_sell,
    round(sum(sales_amount_rub), 2)                      AS turnover_rub,
    sum(sales_quantity)                                  AS turnover_pcs
FROM {TABLE}
{where}
GROUP BY {dim_group}
ORDER BY turnover_rub DESC"""


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Чистит DataFrame:
    - Убирает NULL/пустые в служебные заглушки
    - Фильтрует битые цены
    """
    NULL_VALS = {"", "nan", "None", "NULL", "ᴺᵁᴸᴸ", "None"}

    # Строковые колонки
    for col_key, _ in HIERARCHY_COLUMNS:
        if col_key == "weight_grams":
            continue
        if col_key in df.columns:
            df[col_key] = df[col_key].astype(str).str.strip()
            df[col_key] = df[col_key].where(~df[col_key].isin(NULL_VALS), "Не указано")

    # Граммовка
    if "weight_grams" in df.columns:
        df["weight_grams"] = pd.to_numeric(df["weight_grams"], errors="coerce")

    # Фильтр битых цен: реальная цена от 5 до 5000 ₽
    if "turnover_rub" in df.columns and "turnover_pcs" in df.columns:
        real_price = df["turnover_rub"] / df["turnover_pcs"].replace(0, pd.NA)
        mask = (real_price >= 5) & (real_price <= 5000)
        df = df[mask].copy()

    return df


def remove_empty_hierarchy_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, list]:
    """
    Убирает из DataFrame колонки иерархии, где все значения = 'Не указано' или NULL.
    Возвращает (обновлённый df, список оставленных колонок).
    """
    kept_columns = []

    for col_key, col_name in HIERARCHY_COLUMNS:
        if col_key not in df.columns:
            continue

        # Проверяем есть ли реальные значения
        if col_key == "weight_grams":
            has_data = df[col_key].notna().any() and (df[col_key] > 0).any()
        else:
            unique_vals = df[col_key].dropna().unique()
            meaningful = [v for v in unique_vals if str(v).strip() not in ("", "Не указано")]
            has_data = len(meaningful) > 0

        if has_data:
            kept_columns.append((col_key, col_name))
        else:
            df = df.drop(columns=[col_key])

    return df, kept_columns


def build_excel_bytes(filters: dict) -> bytes:
    """
    Главная функция: строит полный Excel-отчёт по фильтрам.
    Возвращает bytes для скачивания.
    """
    # 1. Запрос
    sql = build_report_sql(filters)
    print(f"[EXCEL] SQL:\n{sql}")
    df = db.query(sql)
    print(f"[EXCEL] Rows: {len(df)}")

    if df.empty:
        raise ValueError("Нет данных по указанным фильтрам")

    # 2. Чистка
    df = clean_dataframe(df)

    # 3. Убираем пустые иерархические колонки
    df, kept_hierarchy = remove_empty_hierarchy_columns(df)

    # 4. Формируем финальный DataFrame с русскими названиями
    rename_map = {}
    final_cols = []

    for col_key, col_name in kept_hierarchy:
        rename_map[col_key] = col_name
        final_cols.append(col_name)

    for col_key, col_name in METRIC_COLUMNS:
        if col_key in df.columns:
            rename_map[col_key] = col_name
            final_cols.append(col_name)

    df = df.rename(columns=rename_map)
    df = df[final_cols]

    # 5. Сортировка по иерархии
    sort_by = [col_name for _, col_name in kept_hierarchy]
    if "Товарооборот руб (с НДС)" in df.columns:
        sort_by.append("Товарооборот руб (с НДС)")
        ascending = [True] * len(kept_hierarchy) + [False]
    else:
        ascending = [True] * len(kept_hierarchy)

    df = df.sort_values(sort_by, ascending=ascending).reset_index(drop=True)

    # 6. Пишем в Excel
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        sheet_name = "Отчёт"
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        ws = writer.sheets[sheet_name]

        _apply_formatting(ws, df)

    buffer.seek(0)
    return buffer.getvalue()


def _apply_formatting(ws, df: pd.DataFrame):
    """Красивое форматирование листа."""
    from openpyxl.styles import Font, Alignment, PatternFill, Border, Side

    # Цвета для шапки в стиле твоего скрина
    header_fill = PatternFill("solid", fgColor="2F5496")  # синий
    header_font = Font(bold=True, size=11, color="FFFFFF")
    border_side = Side(style="thin", color="BFBFBF")
    thin_border = Border(
        left=border_side, right=border_side,
        top=border_side, bottom=border_side,
    )

    col_names = list(df.columns)

    # === ШАПКА ===
    for col_idx, col_name in enumerate(col_names, start=1):
        cell = ws.cell(row=1, column=col_idx)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(
            horizontal="center", vertical="center", wrap_text=True
        )
        cell.border = thin_border

    ws.row_dimensions[1].height = 32

    # === ДАННЫЕ ===
    money_cols = {"Средняя себестоимость", "Средняя цена продажи", "Товарооборот руб (с НДС)"}
    qty_cols = {"Товарооборот шт"}

    for row_idx in range(2, ws.max_row + 1):
        for col_idx, col_name in enumerate(col_names, start=1):
            cell = ws.cell(row=row_idx, column=col_idx)

            if col_name == "Товарооборот руб (с НДС)":
                cell.number_format = '#,##0.00 ₽'
                cell.alignment = Alignment(horizontal="right", vertical="center")
            elif col_name in ("Средняя себестоимость", "Средняя цена продажи"):
                cell.number_format = '#,##0.00'
                cell.alignment = Alignment(horizontal="right", vertical="center")
            elif col_name == "Товарооборот шт":
                cell.number_format = '#,##0'
                cell.alignment = Alignment(horizontal="right", vertical="center")
            elif col_name == "Граммовка":
                cell.number_format = '0'
                cell.alignment = Alignment(horizontal="center", vertical="center")
            else:
                cell.alignment = Alignment(horizontal="left", vertical="center")

            cell.border = thin_border

    # === ШИРИНА КОЛОНОК ===
    col_widths = {
        "Сеть": 14,
        "Регион": 24,
        "Город": 22,
        "Бренд": 22,
        "Вкус": 30,
        "Граммовка": 12,
        "Средняя себестоимость": 22,
        "Средняя цена продажи": 22,
        "Товарооборот руб (с НДС)": 26,
        "Товарооборот шт": 18,
    }

    for col_idx, col_name in enumerate(col_names, start=1):
        letter = get_column_letter(col_idx)
        ws.column_dimensions[letter].width = col_widths.get(col_name, 18)

    # === ЗАКРЕПИТЬ ШАПКУ + АВТОФИЛЬТР ===
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = ws.dimensions