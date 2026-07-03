import re
from database import db
from llm import llm
from prompts import build_prompt


DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "RENAME", "DETACH", "ATTACH", "KILL", "OPTIMIZE"
]


def clean_sql(text: str) -> str:
    text = text.strip()
    text = re.sub(r"```sql", "", text, flags=re.IGNORECASE)
    text = re.sub(r"```", "", text)
    text = text.strip()

    # Вытаскиваем SELECT или WITH из любого текста
    match = re.search(r"((?:WITH|SELECT)[\s\S]+)", text, flags=re.IGNORECASE)
    if match:
        text = match.group(1).strip()

    # Убираем финальную точку с запятой
    text = text.rstrip(";").strip()

    # Убираем финальную точку
    if text.endswith("."):
        text = text[:-1]

    return text


def is_safe(sql: str) -> bool:
    sql_upper = sql.upper().lstrip()

    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        return False

    for kw in DANGEROUS_KEYWORDS:
        if re.search(rf"\b{kw}\b", sql_upper):
            return False

    # Запрещаем несколько запросов через ;
    if sql.count(";") > 0:
        return False

    return True


def ensure_limit(sql: str) -> str:
    """Добавляет LIMIT только если его нет."""
    sql_clean = sql.strip().rstrip(";").strip()

    # Проверяем есть ли LIMIT (учитывая, что LIMIT может быть с BY или FORMAT после)
    # Ищем LIMIT в конце запроса или перед FORMAT
    has_limit = re.search(
        r"\bLIMIT\s+\d+", 
        sql_clean, 
        flags=re.IGNORECASE
    )

    if not has_limit:
        sql_clean += " LIMIT 1000"

    return sql_clean


def ask(question: str):
    schema = db.get_schema()
    sample = db.get_sample(3)

    system_prompt = build_prompt(schema, sample)

    sql_raw, model = llm.ask(system_prompt, question)
    sql = clean_sql(sql_raw)

    if not is_safe(sql):
        raise ValueError(f"Небезопасный или некорректный SQL:\n\n```sql\n{sql}\n```")

    sql = ensure_limit(sql)

    try:
        df = db.query(sql)
    except Exception as e:
        raise RuntimeError(f"Ошибка выполнения SQL:\n\n```sql\n{sql}\n```\n\n**Причина:** {e}")

    return {
        "question": question,
        "sql": sql,
        "data": df,
        "model": model,
    }