"""Построение системного промпта для LLM."""
from metadata import format_columns_for_prompt, BUSINESS_GLOSSARY, COMMON_RULES
from examples import format_examples_for_prompt


SYSTEM_PROMPT_TEMPLATE = """Ты — опытный SQL-аналитик по ClickHouse.
Работаешь с витриной продаж чипсов и снеков в 19 розничных сетях России.

{schema}

{glossary}

{rules}

{column_values}

{conversational}

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


def _get_column_values_hint() -> str:
    """Безопасно получает подсказку с реальными значениями колонок."""
    try:
        from column_values import build_column_values_hint
        return build_column_values_hint()
    except Exception as e:
        print(f"[PROMPTS] Failed to load column values: {e}")
        return ""


def _get_conversational_hint() -> str:
    """Безопасно получает подсказку по разговорным словам."""
    try:
        from column_values import build_conversational_hint
        return build_conversational_hint()
    except Exception as e:
        print(f"[PROMPTS] Failed to load conversational hints: {e}")
        return ""


def build_system_prompt(examples_text: str = None) -> str:
    """Собирает системный промпт со всеми подсказками."""
    if examples_text is None:
        examples_text = format_examples_for_prompt(limit=8)

    return SYSTEM_PROMPT_TEMPLATE.format(
        schema=format_columns_for_prompt(),
        glossary=BUSINESS_GLOSSARY,
        rules=COMMON_RULES,
        column_values=_get_column_values_hint(),
        conversational=_get_conversational_hint(),
        examples=examples_text,
    )


def build_correction_prompt(question: str, prev_sql: str, error: str) -> str:
    """Промпт для второй попытки после ошибки."""
    return CORRECTION_PROMPT_TEMPLATE.format(
        question=question,
        prev_sql=prev_sql,
        error=str(error)[:500],
    )