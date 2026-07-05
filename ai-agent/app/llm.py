"""OpenRouter клиент с fallback между моделями."""
import time
import random
import httpx

from config import config


class LLMClient:
    def __init__(self):
        if not config.OPENROUTER_API_KEY:
            raise RuntimeError("OPENROUTER_API_KEY is empty")

        self.api_key = config.OPENROUTER_API_KEY
        self.url = config.OPENROUTER_URL
        self.models = config.LLM_MODELS

    def ask(self, system_prompt: str, user_message: str, max_wait: float = 15.0):
        """
        Спросить у LLM с fallback между моделями.
        max_wait — сколько секунд максимум ждать одну модель при 429.
        """
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
            "max_tokens": 2000,
        }

        # Перемешиваем модели, чтобы не долбить одни и те же
        shuffled = list(self.models)
        random.shuffle(shuffled)

        errors = []

        for model in shuffled:
            payload = {**payload_base, "model": model}
            print(f"[LLM] Trying: {model}")

            for attempt in range(2):  # до 2 попыток на модель
                try:
                    with httpx.Client(timeout=90.0) as client:
                        response = client.post(self.url, headers=headers, json=payload)

                    # === 429: rate limit ===
                    if response.status_code == 429:
                        try:
                            err_data = response.json()
                            retry_after = err_data.get("error", {}).get(
                                "metadata", {}
                            ).get("retry_after_seconds", 0)
                        except Exception:
                            retry_after = 0

                        print(f"[LLM] {model} rate limit, retry_after={retry_after}s")

                        # Если ждать недолго — ждём и пробуем ещё раз
                        if 0 < retry_after <= max_wait and attempt == 0:
                            time.sleep(retry_after + 0.5)
                            continue  # повторный attempt для той же модели

                        errors.append(f"{model}: rate limit")
                        break  # переходим к следующей модели

                    # === Другие HTTP ошибки ===
                    if response.status_code >= 400:
                        errors.append(f"{model}: HTTP {response.status_code}")
                        print(f"[LLM] {model} HTTP {response.status_code}: {response.text[:200]}")
                        break

                    data = response.json()
                    if "error" in data:
                        errors.append(f"{model}: {data['error']}")
                        break

                    content = data["choices"][0]["message"]["content"]

                    if not content or not content.strip():
                        errors.append(f"{model}: empty content")
                        break

                    print(f"[LLM] ✅ Success with {model}")
                    # Небольшая задержка после успеха — чтобы не долбить сразу же
                    time.sleep(0.3)
                    return content.strip(), model

                except Exception as e:
                    errors.append(f"{model}: {e}")
                    print(f"[LLM] {model} exception: {e}")
                    break

        raise RuntimeError("Все LLM-модели недоступны:\n" + "\n".join(errors[-8:]))


llm = LLMClient()