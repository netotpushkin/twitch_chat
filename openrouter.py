"""OpenRouter-клиент: тонкая обёртка над их Chat Completions API.

Использует общий http_pool (keep-alive). Ключ и модель — из env.
"""

import json
import urllib.error

from config import OPENROUTER_API_KEY, OPENROUTER_MODEL, OPENROUTER_REFERER, OPENROUTER_TITLE
from http_pool import request


API_URL = "https://openrouter.ai/api/v1/chat/completions"


class OpenRouterError(RuntimeError):
    pass


class ContentFilteredError(OpenRouterError):
    """Провайдер (обычно Azure/OpenAI) сам зарезал ответ своим safety-фильтром.
    Самостоятельный сигнал — содержание сочтено небезопасным."""
    pass


def ask(prompt, system=None, model=None, temperature=0.7, max_tokens=512, timeout=30,
        reasoning=None, response_format=None):
    """Отправить один пользовательский запрос, вернуть текст ответа.

    prompt    — строка пользователя.
    system    — опциональный system-prompt.
    model     — переопределение модели; по умолчанию OPENROUTER_MODEL из env.
    reasoning — dict для OpenRouter, например {"enabled": False} чтобы отключить
                хидден-thinking на reasoning-моделях (иначе они сжирают max_tokens
                на «размышления» и возвращают пустой content).
    response_format — например {"type": "json_object"} чтобы заставить модель
                вернуть валидный JSON. Требуется, чтобы слово "json" встречалось
                в самих сообщениях (требование OpenAI/Azure).
    Бросает OpenRouterError при отсутствии ключа/ошибке API/пустом ответе.
    """
    if not OPENROUTER_API_KEY:
        raise OpenRouterError("OPENROUTER_API_KEY не задан в .env")

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    return chat(messages, model=model, temperature=temperature, max_tokens=max_tokens,
                timeout=timeout, reasoning=reasoning, response_format=response_format)


def chat(messages, model=None, temperature=0.7, max_tokens=512, timeout=30, reasoning=None,
         response_format=None):
    """То же, что ask(), но принимает готовый список messages для многошаговых диалогов."""
    if not OPENROUTER_API_KEY:
        raise OpenRouterError("OPENROUTER_API_KEY не задан в .env")

    payload = {
        "model": model or OPENROUTER_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if reasoning is not None:
        payload["reasoning"] = reasoning
    if response_format is not None:
        payload["response_format"] = response_format
    body = json.dumps(payload).encode("utf-8")

    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    # Эти заголовки опциональны, но OpenRouter показывает их в дашборде —
    # удобно различать вызовы от разных проектов.
    if OPENROUTER_REFERER:
        headers["HTTP-Referer"] = OPENROUTER_REFERER
    if OPENROUTER_TITLE:
        headers["X-Title"] = OPENROUTER_TITLE

    try:
        _status, _hdrs, data = request("POST", API_URL, headers=headers, body=body, timeout=timeout)
    except urllib.error.HTTPError as e:
        try:
            err_body = e.read().decode("utf-8", errors="replace")
        except Exception:
            err_body = ""
        raise OpenRouterError(f"HTTP {e.code}: {err_body[:500]}") from e

    try:
        payload = json.loads(data.decode("utf-8", errors="replace"))
    except ValueError as e:
        raise OpenRouterError(f"невалидный JSON в ответе: {e}") from e

    # Провайдер мог отрезать ответ собственным safety-фильтром — content=None,
    # finish_reason='content_filter'. Сообщаем об этом отдельным типом исключения,
    # чтобы вызывающий код мог решить, как трактовать (для модерации — это сильный
    # сигнал «удалить»).
    try:
        choice = payload["choices"][0]
        content = choice["message"]["content"]
    except (KeyError, IndexError, AttributeError) as e:
        raise OpenRouterError(f"неожиданная форма ответа: {payload!r}") from e
    if content is None:
        finish = choice.get("finish_reason") or choice.get("native_finish_reason") or ""
        if finish == "content_filter":
            raise ContentFilteredError("ответ зарезан safety-фильтром провайдера")
        raise OpenRouterError(f"пустой content, finish_reason={finish!r}")
    return content.strip()
