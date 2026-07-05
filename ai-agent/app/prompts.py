"""Построение системного промпта для LLM."""
from metadata import format_columns_for_prompt, BUSINESS_GLOSSARY, COMMON_RULES
from examples import format_examples_for_prompt


SYSTEM_PROMPT_TEMPLATE = """Ты — опытный SQL-аналитик по ClickHouse.
Работаешь с витриной продаж чипсов и снеков в 19 розничных сетях России.

{schema}

{glossary}

{rules}

{examples}

Отвечай ТОЛЬКО одним SQL-запросом. Никакого другого текста, никаких пояснений, никакой обёртки ```sql.
Только чистый SQL с одним завершающим переносом строки.
"""


CORRECTION_PROMPT_TEMPLATE = """Ты сгенерировал SQL, но при выполнении в ClickHouse произошла ошибка.
Твоя задача: исправить SQL так, чтобы он выполнился корректно.

Оригинальный вопрос пользователя:
{question}

Твой предыдущий SQL:
{prev_sql}

Ошибка ClickHouse:
{error}

Правила те же, что и раньше. Верни только исправленный SQL без объяснений.
"""


def build_system_prompt(examples_text: str = None) -> str:
    """Собирает системный промпт."""
    if examples_text is None:
        examples_text = format_examples_for_prompt(limit=8)

    return SYSTEM_PROMPT_TEMPLATE.format(
        schema=format_columns_for_prompt(),
        glossary=BUSINESS_GLOSSARY,
        rules=COMMON_RULES,
        examples=examples_text,
    )


def build_correction_prompt(question: str, prev_sql: str, error: str) -> str:
    """Промпт для второй попытки после ошибки."""
    return CORRECTION_PROMPT_TEMPLATE.format(
        question=question,
        prev_sql=prev_sql,
        error=str(error)[:500],
    )