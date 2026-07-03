import time
import httpx
from config import config


class LLMClient:
    def __init__(self):
        if not config.OPENROUTER_API_KEY:
            raise RuntimeError("OPENROUTER_API_KEY is empty")

        self.api_key = config.OPENROUTER_API_KEY
        self.url = config.OPENROUTER_URL
        self.models = config.LLM_MODELS

    def ask(self, system_prompt: str, user_message: str):
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.api_key}",
            "HTTP-Referer": "http://213.165.222.200:8501",
            "X-Title": "Sales AI Agent",
        }

        payload_base = {
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "temperature": 0.1,
            "max_tokens": 1200,
        }

        errors = []

        for model in self.models:
            payload = {**payload_base, "model": model}
            print(f"[LLM] Trying model: {model}")

            try:
                with httpx.Client(timeout=90.0) as client:
                    response = client.post(self.url, headers=headers, json=payload)

                if response.status_code == 429:
                    # Пробуем понять сколько ждать
                    try:
                        err_data = response.json()
                        retry_after = err_data.get("error", {}).get("metadata", {}).get("retry_after_seconds", 0)
                    except Exception:
                        retry_after = 0

                    print(f"[LLM] {model} rate limit, retry after {retry_after}s")

                    # Если короткое ожидание — попробуем ещё раз ту же модель
                    if 0 < retry_after <= 5:
                        time.sleep(retry_after + 0.5)
                        try:
                            response = client.post(self.url, headers=headers, json=payload)
                        except Exception:
                            pass

                    if response.status_code == 429:
                        errors.append(f"{model}: rate limit")
                        continue

                if response.status_code >= 400:
                    errors.append(f"{model}: HTTP {response.status_code}")
                    print(f"[LLM] {model} HTTP {response.status_code}: {response.text[:200]}")
                    continue

                data = response.json()

                if "error" in data:
                    errors.append(f"{model}: {data['error']}")
                    continue

                content = data["choices"][0]["message"]["content"]
                print(f"[LLM] Success with {model}")
                return content.strip(), model

            except Exception as e:
                errors.append(f"{model}: {e}")
                continue

        raise RuntimeError("Все LLM-модели недоступны:\n" + "\n".join(errors[-5:]))


llm = LLMClient()