"""Donatty SSE-клиент: подписка на донаты и публикация в donatty_bus.

Схема, реверс-инженерная из widgets.donatty.com/donations/Script.js:
    1. GET https://api.donatty.com/auth/tokens/{TOKEN} → {response: {accessToken: <JWT>}}
       где TOKEN — параметр ?token= из URL виджета (фактически refresh-токен).
    2. GET https://api.donatty.com/widgets/{REF}/sse?jwt=<JWT>&zoneOffset=<minutes>
       где REF — параметр ?ref= из URL виджета. Поток text/event-stream:
         data:{"action":"PING"}                                  — heartbeat
         data:{"action":"DATA","data":{                          — донат
             "subscriber":"…",  "message":"…", "amount":N,
             "currency":"RUB", "goal":{"title":"…"}, "mute":{…} }}

JWT валиден ~сутки (см. expireAt); при 401 / EOF — переподключаемся
с экспоненциальным backoff'ом и при необходимости обновляем JWT."""

import json
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

from events import donatty_bus


_API = "https://api.donatty.com"
_TIMEOUT_CONNECT = 10
# Donatty шлёт PING примерно каждые ~10 сек. Молчание > этого порога считаем зависанием.
_SILENCE_LIMIT   = 45
# Backoff: 1с → 2с → 4с … до 30с между попытками реконнекта.
_BACKOFF_START   = 1
_BACKOFF_MAX     = 30


def _fetch_jwt(token):
    url = f"{_API}/auth/tokens/{urllib.parse.quote(token, safe='')}"
    req = urllib.request.Request(url, headers={
        "Accept": "application/json",
        "Origin": "https://widgets.donatty.com",
        "Referer": "https://widgets.donatty.com/",
    })
    with urllib.request.urlopen(req, timeout=_TIMEOUT_CONNECT) as r:
        payload = json.loads(r.read().decode("utf-8"))
    jwt = (payload.get("response") or {}).get("accessToken")
    if not jwt:
        raise RuntimeError(f"Donatty: нет accessToken в ответе: {payload!r}")
    return jwt


def _zone_offset():
    # JS new Date().getTimezoneOffset() возвращает разницу UTC-local в минутах
    # (для МСК UTC+3 это -180). time.timezone имеет противоположный знак —
    # отсюда деление с минусом.
    secs = time.altzone if (time.daylight and time.localtime().tm_isdst) else time.timezone
    return secs // 60


def _open_sse(ref, jwt):
    qs = urllib.parse.urlencode({"jwt": jwt, "zoneOffset": _zone_offset()})
    url = f"{_API}/widgets/{urllib.parse.quote(ref, safe='')}/sse?{qs}"
    req = urllib.request.Request(url, headers={
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "Origin": "https://widgets.donatty.com",
        "Referer": "https://widgets.donatty.com/",
    })
    # timeout — на сам read(), без него висим вечно при пропаже сети.
    return urllib.request.urlopen(req, timeout=_SILENCE_LIMIT)


def _normalize(data):
    """Приводим донат к компактному виду, который ждёт оверлей.
    Поля Donatty: subscriber / message / amount / currency / goal{title}."""
    return {
        "type":     "donation",
        # id для корреляции с type:tts — оверлей сериализует модалку и аудио
        # одного и того же доната, используя этот id.
        "id":       uuid.uuid4().hex,
        "user":     (data.get("subscriber") or "Аноним").strip() or "Аноним",
        "message":  (data.get("message") or "").strip(),
        "amount":   data.get("amount", 0),
        "currency": data.get("currency") or "",
        "goal":     ((data.get("goal") or {}).get("title") or "").strip(),
    }


def _consume_stream(resp, log):
    """Читает SSE до разрыва. Возвращает (silence_break, reason)."""
    # readline блокируется до \n; чтобы ловить тишину — выставили socket-level timeout
    # при urlopen. Когда сервер молчит дольше — readline кидает TimeoutError/OSError.
    while True:
        try:
            line = resp.readline()
        except (TimeoutError, OSError) as e:
            return True, f"silence/io: {e}"
        if not line:
            return False, "eof"
        line = line.rstrip(b"\r\n")
        if not line:
            continue  # пустая строка — разделитель SSE-сообщений
        if line.startswith(b":"):
            continue  # SSE-комментарий
        if not line.startswith(b"data:"):
            continue  # event:/id:/retry: — Donatty их не шлёт, но мало ли
        payload = line[5:].lstrip()
        try:
            msg = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            log(f"(donatty) битый JSON: {e}; пропуск")
            continue
        action = msg.get("action")
        if action == "PING":
            continue
        if action == "DATA":
            data = msg.get("data") or {}
            event = _normalize(data)
            donatty_bus.publish(event)
            log(f"(donatty) донат: {event['user']} — {event['amount']} {event['currency']}")
            continue
        # Незнакомые action — логируем, но не падаем.
        log(f"(donatty) неизвестный action: {action!r}")


def run(ref, token, log=print):
    """Бесконечный цикл: получить JWT → читать SSE → реконнект при ошибке."""
    backoff = _BACKOFF_START
    jwt = None
    while True:
        try:
            if jwt is None:
                jwt = _fetch_jwt(token)
                log("(donatty) JWT получен, подключаюсь к SSE...")
            resp = _open_sse(ref, jwt)
        except urllib.error.HTTPError as e:
            # 401/403 — JWT протух или token отозван. Сбросим JWT, в следующей итерации
            # он перевыпустится. На 4xx кроме 401/403 — тоже сбросим, чтобы не зацикливаться.
            log(f"(donatty) HTTP {e.code} при подключении: {e.reason}")
            jwt = None
        except (urllib.error.URLError, TimeoutError, OSError) as e:
            log(f"(donatty) сеть: {e}")
        else:
            backoff = _BACKOFF_START  # успешно подключились
            try:
                _, reason = _consume_stream(resp, log)
                log(f"(donatty) поток закрыт: {reason}")
            finally:
                try: resp.close()
                except OSError: pass
            # После закрытия пробуем тем же JWT — если он истёк, _open_sse поймает 401.
        time.sleep(backoff)
        backoff = min(backoff * 2, _BACKOFF_MAX)
