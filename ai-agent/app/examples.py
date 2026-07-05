"""Примеры пар вопрос → SQL для few-shot обучения LLM."""

EXAMPLES = [
    # === ТОПЫ ===
    {
        "question": "Топ-10 товаров по продажам",
        "sql": """SELECT product_name, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE product_name != ''
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10""",
    },
    {
        "question": "Топ-10 брендов по выручке",
        "sql": """SELECT coalesce(brand, 'Без бренда') AS brand, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
GROUP BY brand
ORDER BY revenue DESC
LIMIT 10""",
    },
    {
        "question": "Топ-5 сетей по обороту",
        "sql": """SELECT retail_chain, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
GROUP BY retail_chain
ORDER BY revenue DESC
LIMIT 5""",
    },
    {
        "question": "Топ-10 городов по продажам",
        "sql": """SELECT coalesce(city_name, 'Неизвестно') AS city, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE city_name IS NOT NULL AND city_name != ''
GROUP BY city
ORDER BY revenue DESC
LIMIT 10""",
    },
    {
        "question": "Топ-10 регионов по количеству проданных единиц",
        "sql": """SELECT coalesce(trim(region_name), 'Неизвестно') AS region, SUM(sales_quantity) AS qty
FROM sales_mart
WHERE region_name IS NOT NULL AND region_name != ''
GROUP BY region
ORDER BY qty DESC
LIMIT 10""",
    },
    {
        "question": "Топ-10 магазинов по обороту",
        "sql": """SELECT store_code, any(store_name) AS store_name, any(city_name) AS city, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE store_code IS NOT NULL
GROUP BY store_code
ORDER BY revenue DESC
LIMIT 10""",
    },

    # === ВРЕМЯ ===
    {
        "question": "Продажи по месяцам",
        "sql": """SELECT year, month, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
GROUP BY year, month
ORDER BY year, min(toDate(date))
LIMIT 100""",
    },
    {
        "question": "Динамика продаж по дням за июль 2025",
        "sql": """SELECT date, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE date >= '2025-07-01' AND date <= '2025-07-31'
GROUP BY date
ORDER BY date
LIMIT 100""",
    },
    {
        "question": "Продажи по неделям в 2025 году",
        "sql": """SELECT week_num, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE year = 2025 AND week_num IS NOT NULL
GROUP BY week_num
ORDER BY week_num
LIMIT 60""",
    },
    {
        "question": "Сравни продажи 2024 и 2025 годов",
        "sql": """SELECT year, round(SUM(sales_amount_rub), 2) AS revenue, SUM(sales_quantity) AS qty
FROM sales_mart
WHERE year IN (2024, 2025)
GROUP BY year
ORDER BY year""",
    },

    # === КАТЕГОРИИ ===
    {
        "question": "Продажи по категориям чипсов",
        "sql": """SELECT chip_type, round(SUM(sales_amount_rub), 2) AS revenue, SUM(sales_quantity) AS qty
FROM sales_mart
WHERE chip_type IS NOT NULL AND chip_type != '' AND chip_type != 'Не чипсы'
GROUP BY chip_type
ORDER BY revenue DESC
LIMIT 20""",
    },
    {
        "question": "Средняя цена товара по категориям",
        "sql": """SELECT product_category_3,
       round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_price
FROM sales_mart
WHERE product_category_3 != ''
GROUP BY product_category_3
ORDER BY avg_price DESC
LIMIT 30""",
    },
    {
        "question": "Продажи картофельных чипсов по брендам",
        "sql": """SELECT coalesce(brand, 'Без бренда') AS brand, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE chip_type = 'Картофельные чипсы'
GROUP BY brand
ORDER BY revenue DESC
LIMIT 20""",
    },

    # === МАРЖА ===
    {
        "question": "Топ-10 самых маржинальных брендов",
        "sql": """SELECT coalesce(brand, 'Без бренда') AS brand,
       round(SUM(coalesce(margin_rub, sales_amount_rub - sales_cost_price)), 2) AS margin,
       round(SUM(coalesce(margin_rub, sales_amount_rub - sales_cost_price)) / NULLIF(SUM(sales_amount_rub), 0) * 100, 2) AS margin_pct
FROM sales_mart
GROUP BY brand
ORDER BY margin DESC
LIMIT 10""",
    },
    {
        "question": "Маржинальность по сетям",
        "sql": """SELECT retail_chain,
       round(SUM(coalesce(margin_rub, sales_amount_rub - sales_cost_price)) / NULLIF(SUM(sales_amount_rub), 0) * 100, 2) AS margin_pct,
       round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
GROUP BY retail_chain
ORDER BY revenue DESC
LIMIT 20""",
    },

    # === КОЛИЧЕСТВЕННЫЕ ===
    {
        "question": "Сколько всего магазинов в каждой сети",
        "sql": """SELECT retail_chain, COUNT(DISTINCT store_code) AS stores_count
FROM sales_mart
WHERE store_code IS NOT NULL
GROUP BY retail_chain
ORDER BY stores_count DESC""",
    },
    {
        "question": "Сколько уникальных товаров продаётся в каждой сети",
        "sql": """SELECT retail_chain, COUNT(DISTINCT product_id) AS products_count
FROM sales_mart
WHERE product_id IS NOT NULL
GROUP BY retail_chain
ORDER BY products_count DESC""",
    },
    {
        "question": "Сколько магазинов в каждом регионе",
        "sql": """SELECT coalesce(trim(region_name), 'Неизвестно') AS region, COUNT(DISTINCT store_code) AS stores
FROM sales_mart
WHERE region_name IS NOT NULL AND region_name != ''
GROUP BY region
ORDER BY stores DESC
LIMIT 30""",
    },

    # === ФИЛЬТРЫ ===
    {
        "question": "Продажи Lay's в Москве",
        "sql": """SELECT date, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE brand ILIKE '%Lay%' AND city_name ILIKE '%Москва%'
GROUP BY date
ORDER BY date
LIMIT 365""",
    },
    {
        "question": "Топ-10 товаров в Пятерочке",
        "sql": """SELECT product_name, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE retail_chain = 'Пятерочка'
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10""",
    },
    {
        "question": "Продажи чипсов с солью",
        "sql": """SELECT product_name, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE flavor ILIKE '%Соль%' AND chip_type LIKE '%чипсы%'
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 20""",
    },
    {
        "question": "Товары весом 160 грамм",
        "sql": """SELECT product_name, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE toFloat64OrNull(weight_grams) = 160
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 20""",
    },

    # === СЛОЖНЫЕ ===
    {
        "question": "Топ-5 брендов в каждой сети",
        "sql": """SELECT retail_chain, brand, revenue
FROM (
    SELECT retail_chain, coalesce(brand, 'Без бренда') AS brand,
           round(SUM(sales_amount_rub), 2) AS revenue,
           row_number() OVER (PARTITION BY retail_chain ORDER BY SUM(sales_amount_rub) DESC) AS rn
    FROM sales_mart
    GROUP BY retail_chain, brand
)
WHERE rn <= 5
ORDER BY retail_chain, revenue DESC
LIMIT 200""",
    },
    {
        "question": "Средний чек по сетям",
        "sql": """SELECT retail_chain,
       round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_price
FROM sales_mart
GROUP BY retail_chain
ORDER BY avg_price DESC""",
    },
    {
        "question": "Динамика продаж по месяцам для картофельных чипсов",
        "sql": """SELECT year, month, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE chip_type = 'Картофельные чипсы'
GROUP BY year, month
ORDER BY year, min(toDate(date))
LIMIT 100""",
    },
    {
        "question": "Продажи по типам упаковки",
        "sql": """SELECT package_type, round(SUM(sales_amount_rub), 2) AS revenue, SUM(sales_quantity) AS qty
FROM sales_mart
WHERE package_type IS NOT NULL AND package_type != ''
GROUP BY package_type
ORDER BY revenue DESC""",
    },
    {
        "question": "Промо-продажи по сетям",
        "sql": """SELECT retail_chain, round(SUM(coalesce(promo_sales_rub, 0)), 2) AS promo_revenue
FROM sales_mart
GROUP BY retail_chain
HAVING promo_revenue > 0
ORDER BY promo_revenue DESC""",
    },
    {
        "question": "Доля промо в продажах по сетям",
        "sql": """SELECT retail_chain,
       round(SUM(coalesce(promo_sales_rub, 0)) / NULLIF(SUM(sales_amount_rub), 0) * 100, 2) AS promo_share_pct
FROM sales_mart
GROUP BY retail_chain
HAVING promo_share_pct > 0
ORDER BY promo_share_pct DESC""",
    },
    {
        "question": "Топ-10 самых продаваемых товаров по количеству штук",
        "sql": """SELECT product_name, SUM(sales_quantity) AS qty, round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE product_name != ''
GROUP BY product_name
ORDER BY qty DESC
LIMIT 10""",
    },
    {
        "question": "Общая статистика по базе",
        "sql": """SELECT COUNT(*) AS rows_total,
       COUNT(DISTINCT retail_chain) AS chains,
       COUNT(DISTINCT store_code) AS stores,
       COUNT(DISTINCT product_id) AS products,
       round(SUM(sales_amount_rub), 2) AS total_revenue,
       SUM(sales_quantity) AS total_qty,
       min(date) AS min_date,
       max(date) AS max_date
FROM sales_mart""",
    },
    {
        "question": "Цена за 1 грамм по брендам для картофельных чипсов",
        "sql": """SELECT
    brand,
    round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0) / NULLIF(toFloat64OrNull(weight_grams), 0), 3) AS price_per_gram_rub,
    round(SUM(sales_cost_price) / NULLIF(SUM(sales_quantity), 0) / NULLIF(toFloat64OrNull(weight_grams), 0), 3) AS cost_per_gram_rub,
    round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE chip_type = 'Картофельные чипсы'
  AND brand IS NOT NULL AND brand != ''
  AND toFloat64OrNull(weight_grams) > 0
GROUP BY brand, weight_grams
HAVING revenue > 100000
ORDER BY revenue DESC
LIMIT 30""",
    },

    # === НОРМАЛИЗАЦИЯ К 120Г ===
    {
        "question": "Нормализованная цена к 120 грамм по граммовкам для картофельных чипсов 2026",
        "sql": """SELECT
    toInt32OrNull(weight_grams) AS grams,
    round(
        (SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0))
        / NULLIF(toFloat64OrNull(weight_grams), 0)
        * 120,
        2
    ) AS price_normalized_120g,
    round(
        (SUM(sales_cost_price) / NULLIF(SUM(sales_quantity), 0))
        / NULLIF(toFloat64OrNull(weight_grams), 0)
        * 120,
        2
    ) AS cost_normalized_120g,
    round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2) AS actual_avg_price,
    SUM(sales_quantity) AS qty,
    round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE chip_type = 'Картофельные чипсы'
  AND year = 2026
  AND toInt32OrNull(weight_grams) IN (70, 120, 140, 180, 220, 225, 250)
GROUP BY grams, weight_grams
ORDER BY grams""",
    },

    # === АНАЛИЗ ГРАММОВОК С ЦЕНОЙ И СЕБЕСТОИМОСТЬЮ ===
    {
        "question": "Картофельные чипсы 70 120 140 180 220 225 250 грамм: цена, себестоимость, вкусы, бренды, цена за грамм",
        "sql": """SELECT
    toInt32OrNull(weight_grams) AS grams,
    brand,
    flavor,
    round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_sell_price,
    round(SUM(sales_cost_price) / NULLIF(SUM(sales_quantity), 0), 2) AS avg_cost_price,
    round(SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0) / NULLIF(toFloat64OrNull(weight_grams), 0), 3) AS price_per_gram,
    round(
        (SUM(sales_amount_rub) / NULLIF(SUM(sales_quantity), 0))
        / NULLIF(toFloat64OrNull(weight_grams), 0)
        * 120,
        2
    ) AS price_normalized_120g,
    round(SUM(sales_amount_rub), 2) AS revenue,
    SUM(sales_quantity) AS qty
FROM sales_mart
WHERE chip_type = 'Картофельные чипсы'
  AND toInt32OrNull(weight_grams) IN (70, 120, 140, 180, 220, 225, 250)
  AND brand IS NOT NULL AND brand != ''
  AND flavor IS NOT NULL AND flavor != ''
GROUP BY grams, brand, flavor, weight_grams
HAVING qty > 100
ORDER BY revenue DESC
LIMIT 500""",
    },

    # === СРАВНЕНИЕ БРЕНДОВ ПО ЦЕНЕ ЗА ГРАММ ===
    {
        "question": "Топ-20 самых дорогих брендов по цене за грамм",
        "sql": """SELECT
    brand,
    round(avg(sales_amount_rub / NULLIF(sales_quantity, 0) / NULLIF(toFloat64OrNull(weight_grams), 0)), 3) AS avg_price_per_gram,
    round(SUM(sales_amount_rub), 2) AS revenue
FROM sales_mart
WHERE brand IS NOT NULL AND brand != ''
  AND toFloat64OrNull(weight_grams) > 0
  AND sales_quantity > 0
GROUP BY brand
HAVING revenue > 100000
ORDER BY avg_price_per_gram DESC
LIMIT 20""",
    },
]


def format_examples_for_prompt(examples: list = None, limit: int = 8) -> str:
    """Форматирует примеры для системного промпта."""
    if examples is None:
        examples = EXAMPLES[:limit]

    lines = ["Примеры вопросов и SQL-запросов:", ""]
    for i, ex in enumerate(examples, 1):
        lines.append(f"Пример {i}:")
        lines.append(f"Вопрос: {ex['question']}")
        lines.append(f"SQL:\n{ex['sql']}")
        lines.append("")

    return "\n".join(lines)