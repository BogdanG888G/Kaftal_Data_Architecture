import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "")
    OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

    CH_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
    CH_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CH_USER = os.getenv("CLICKHOUSE_USER", "admin")
    CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "123")
    CH_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "default")
    CH_TABLE = os.getenv("CLICKHOUSE_TABLE", "sales_mart")

    LLM_MODELS = [
        model.strip()
        for model in os.getenv(
            "LLM_MODELS",
            "openai/gpt-oss-120b:free,google/gemma-4-31b-it:free"
        ).split(",")
        if model.strip()
    ]


config = Config()