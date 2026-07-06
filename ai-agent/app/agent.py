"""Логика text-to-SQL агента с self-correction, entity + metrics + product RAG."""
import re

from database import db
from llm import llm
from prompts import build_system_prompt, build_correction_prompt
from entities import enrich_question
from metrics import enrich_with_metrics
from product_rag import build_flavors_hint_for_query, build_context_for_llm


DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "RENAME", "DETACH", "ATTACH", "KILL", "OPTIMIZE",
    "GRANT", "REVOKE",
]

MAX_CORRECTION_ATTEMPTS = 2


# ============================================================
# ЧИСТКА И ВАЛИДАЦИЯ SQL
# ============================================================

def clean_sql(text: str) -> str:
    """Убирает markdown-обёртки и лишний текст, оставляет чистый SQL."""
    text = text.strip()
    text = re.sub(r"```sql", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```", "", text)
    text = text.strip()

    match = re.search(r"((?:WITH|SELECT)[\s\S]+)", text, flags=re.IGNORECASE)
    if match:
        text = match.group(1).strip()

    text = text.rstrip(";").strip()
    if text.endswith("."):
        text = text[:-1]

    return text


def is_safe(sql: str) -> bool:
    """Проверяет, что SQL безопасен: только SELECT/WITH, без DDL/DML."""
    sql_upper = sql.upper().lstrip()

    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return False

    for kw in DANGEROUS_KEYWORDS:
        if re.search(rf"\b{kw}\b", sql_upper):
            return False

    if sql.count(";") > 0:
        return False

    return True


def ensure_limit(sql: str, max_limit: int = 1000) -> str:
    """Добавляет LIMIT если его нет."""
    sql_clean = sql.strip().rstrip(";").strip()
    has_limit = re.search(r"\bLIMIT\s+\d+", sql_clean, flags=re.IGNORECASE)

    if not has_limit:
        sql_clean += f" LIMIT {max_limit}"

    return sql_clean


# ============================================================
# LLM-ВЫЗОВЫ
# ============================================================

def generate_sql(enriched_question: str) -> tuple[str, str]:
    """Первая генерация SQL."""
    system_prompt = build_system_prompt()
    sql_raw, model = llm.ask(system_prompt, enriched_question)
    return clean_sql(sql_raw), model


def correct_sql(enriched_question: str, prev_sql: str, error: str) -> tuple[str, str]:
    """Исправление SQL после ошибки выполнения."""
    system_prompt = build_system_prompt()
    correction_msg = build_correction_prompt(enriched_question, prev_sql, error)
    sql_raw, model = llm.ask(system_prompt, correction_msg)
    return clean_sql(sql_raw), model


# ============================================================
# ОБОГАЩЕНИЕ ЗАПРОСА
# ============================================================

def enrich_full(question: str) -> tuple[str, dict, dict]:
    """
    Обогащает вопрос всеми источниками контекста:
    1. entities (бренды, сети, вкусы, форматы)
    2. metrics (бизнес-формулы)
    3. product_mapping (реальные комбинации товаров)
    """
    enriched_q1, entities = enrich_question(question)
    enriched_q2, metrics_info = enrich_with_metrics(enriched_q1)

    # Обогащаем контекстом из product_mapping
    brands = entities.get("brands") or []
    try:
        flavors_hint = build_flavors_hint_for_query(
            question,
            brand=brands[0] if brands else None,
        )
        if flavors_hint:
            enriched_q2 = enriched_q2 + flavors_hint

        ctx = build_context_for_llm(
            brands=brands,
            flavors=entities.get("flavors"),
            max_items=10,
        )
        if ctx:
            enriched_q2 = enriched_q2 + ctx
    except Exception as e:
        print(f"[AGENT] product RAG failed: {e}")

    return enriched_q2, entities, metrics_info


# ============================================================
# ГЛАВНАЯ ФУНКЦИЯ
# ============================================================

def ask(question: str) -> dict:
    """
    Основная функция агента.
    1. Обогащает вопрос сущностями, метриками и product RAG.
    2. Генерирует SQL через LLM.
    3. Валидирует безопасность.
    4. Выполняет в ClickHouse.
    5. При ошибке — self-correction (до 2 попыток).
    """
    attempts = []

    # === Обогащаем вопрос ===
    print(f"[AGENT] Original question: {question}")

    enriched_question, entities, metrics_info = enrich_full(question)

    print(f"[AGENT] Entities: {entities}")
    print(f"[AGENT] Metrics: {[m['key'] for m in metrics_info.get('metrics', [])]}")
    if metrics_info.get("grams_list"):
        print(f"[AGENT] Grams: {metrics_info['grams_list']}")
    if metrics_info.get("target_grams"):
        print(f"[AGENT] Target grams: {metrics_info['target_grams']}")

    # === Попытка 1 ===
    sql, model = generate_sql(enriched_question)
    attempts.append({
        "step": "generate",
        "sql": sql,
        "model": model,
        "error": None,
    })

    if not is_safe(sql):
        raise ValueError(
            f"Небезопасный или некорректный SQL:\n\n```sql\n{sql}\n```"
        )

    sql = ensure_limit(sql)

    try:
        df = db.query(sql)
        return {
            "question": question,
            "enriched_question": enriched_question,
            "entities": entities,
            "metrics_info": metrics_info,
            "sql": sql,
            "data": df,
            "model": model,
            "attempts": attempts,
            "corrected": False,
        }
    except Exception as e:
        error_msg = str(e)
        print(f"[AGENT] Attempt 1 failed: {error_msg[:200]}")
        attempts[-1]["error"] = error_msg

    # === Попытки self-correction ===
    for i in range(MAX_CORRECTION_ATTEMPTS):
        print(f"[AGENT] Correction attempt {i + 1}/{MAX_CORRECTION_ATTEMPTS}")
        prev_sql = attempts[-1]["sql"]
        prev_error = attempts[-1]["error"]

        try:
            sql, model = correct_sql(enriched_question, prev_sql, prev_error)
        except Exception as e:
            print(f"[AGENT] LLM correction failed: {e}")
            break

        attempts.append({
            "step": f"correct_{i + 1}",
            "sql": sql,
            "model": model,
            "error": None,
        })

        if not is_safe(sql):
            attempts[-1]["error"] = "unsafe SQL"
            continue

        sql = ensure_limit(sql)

        try:
            df = db.query(sql)
            print(f"[AGENT] Correction {i + 1} succeeded!")
            return {
                "question": question,
                "enriched_question": enriched_question,
                "entities": entities,
                "metrics_info": metrics_info,
                "sql": sql,
                "data": df,
                "model": model,
                "attempts": attempts,
                "corrected": True,
            }
        except Exception as e:
            error_msg = str(e)
            attempts[-1]["error"] = error_msg
            print(f"[AGENT] Correction {i + 1} failed: {error_msg[:200]}")

    # Все попытки провалились
    last = attempts[-1]
    raise RuntimeError(
        f"Не удалось получить корректный SQL после {len(attempts)} попыток.\n\n"
        f"Последний SQL:\n```sql\n{last['sql']}\n```\n\n"
        f"Ошибка: {last['error']}"
    )