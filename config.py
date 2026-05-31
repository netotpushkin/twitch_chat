"""Конфигурация: env-переменные и константы, общие для всех модулей."""

import os
import secrets


def _load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

# Рантайм-состояние (токены, кэши) — рядом с кодом, в state/. Создаём при первом запуске.
STATE_DIR  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "state")
os.makedirs(STATE_DIR, exist_ok=True)

CLIENT_ID  = os.environ.get("TWITCH_CLIENT_ID", "PUT_YOUR_CLIENT_ID_HERE")
CHANNEL    = "#" + os.environ.get("TWITCH_CHANNEL", "put_channel_here").lstrip("#")
REDIRECT   = "http://localhost:3000"
SCOPES     = "chat:read chat:edit moderator:read:followers moderator:read:chatters moderator:manage:chat_messages channel:read:subscriptions channel:manage:broadcast"
TOKEN_FILE = os.path.join(STATE_DIR, "token.json")

OVERLAY_PORT  = int(os.environ.get("OVERLAY_PORT", "5005"))

# Donatty — приём донатов через SSE. Оба значения берутся из URL виджета
# https://widgets.donatty.com/donations/?ref=<DONATTY_REF>&token=<DONATTY_TOKEN>
# (token играет роль refresh-токена: меняется в личном кабинете при сбросе).
# Если хотя бы одна переменная пустая — интеграция отключается.
DONATTY_REF   = os.environ.get("DONATTY_REF", "")
DONATTY_TOKEN = os.environ.get("DONATTY_TOKEN", "")

# TTS-голос Silero v4_ru: aidar / baya / kseniya / xenia / eugene / random.
# Меняй в .env если хочешь другой голос; перезапуск бота подхватит.
TTS_SPEAKER = os.environ.get("TTS_SPEAKER", "baya")

# OpenRouter (https://openrouter.ai) — единый шлюз к разным LLM по OpenAI-совместимому API.
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_MODEL   = os.environ.get("OPENROUTER_MODEL", "openai/gpt-4o-mini")
# Опционально: показываются в дашборде OpenRouter, помогают различать источники вызовов.
OPENROUTER_REFERER = os.environ.get("OPENROUTER_REFERER", "")
OPENROUTER_TITLE   = os.environ.get("OPENROUTER_TITLE", "twitch_chat")

# Модерация чата через LLM + детерминированные предфильтры.
MODERATION_ENABLED = os.environ.get("MODERATION_ENABLED", "1") not in ("", "0", "false", "False")
# DRY_RUN=1 — только лог, без реального удаления. Полезно для обкатки промптов.
MODERATION_DRY_RUN = os.environ.get("MODERATION_DRY_RUN", "0") not in ("", "0", "false", "False")

# Авто-подбор заголовка и тегов стрима по чату через LLM.
TITLER_ENABLED      = os.environ.get("TITLER_ENABLED", "0") not in ("", "0", "false", "False")
TITLER_INTERVAL     = int(os.environ.get("TITLER_INTERVAL", "420"))
TITLER_MIN_MESSAGES = int(os.environ.get("TITLER_MIN_MESSAGES", "10"))
# Обязательные теги: всегда добавляются впереди списка перед PATCH /channels.
# Через запятую, например "Русский,18plus". Пустая строка — без обязательных.
TITLER_REQUIRED_TAGS = [
    t.strip() for t in os.environ.get("TITLER_REQUIRED_TAGS", "").split(",")
    if t.strip()
]


def _load_overlay_token():
    """OVERLAY_TOKEN из env → из state/overlay_token.txt → новый, сохранённый туда же.
    Постоянство нужно, чтобы URL-ы в OBS-сценах переживали перезапуск."""
    env = os.environ.get("OVERLAY_TOKEN")
    if env is not None:
        return env  # пустая строка — отключение защиты, как и обещано комментарием выше.
    path = os.path.join(STATE_DIR, "overlay_token.txt")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                tok = f.read().strip()
            if tok:
                return tok
        except OSError:
            pass
    tok = secrets.token_urlsafe(12)
    try:
        with open(path, "w", encoding="utf-8") as f:
            f.write(tok)
    except OSError:
        pass
    return tok


# Токен для /test/* и /yt/ended — защищает от случайных вызовов другими локальными процессами.
# OVERLAY_TOKEN= (пустая строка) в env отключает проверку.
OVERLAY_TOKEN = _load_overlay_token()
