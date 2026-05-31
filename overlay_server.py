"""Локальный HTTP-сервер: SSE-стримы для оверлеев, статика, тестовые эндпойнты."""

import hmac
import http.server
import json
import os
import queue
import re
import sys
import threading
import urllib.parse

from config import CHANNEL, OVERLAY_PORT, OVERLAY_TOKEN
from events import chat_bus, dice_bus, donatty_bus, emote_bus, events_bus, media_bus
import youtube
from youtube import (
    parse_youtube_id, yt_advance, yt_queue, yt_queue_lock, yt_vote,
)


STATIC_HTML = {"alerts", "chat", "dice", "donatty", "emote_rain", "tts", "webcam", "youtube"}

# Содержимое HTML кэшируется при старте — на каждый GET оверлея больше не лезем на диск.
_OVERLAY_CACHE: dict[str, bytes] = {}

# Любая форма открывающего <head ...> с произвольным регистром и атрибутами.
_HEAD_OPEN_RE = re.compile(rb"<head(\s[^>]*)?>", re.IGNORECASE)


def _preload_overlays():
    here = os.path.dirname(os.path.abspath(__file__))
    for name in STATIC_HTML:
        path = os.path.join(here, "overlays", name + ".html")
        try:
            with open(path, "rb") as f:
                _OVERLAY_CACHE[name] = f.read()
        except OSError as e:
            print(f"(!) не удалось загрузить overlays/{name}.html: {e}")


# ---------- Тестовые эндпойнты: таблица маршрутов ----------

def _qs(path):
    return urllib.parse.parse_qs(urllib.parse.urlparse(path).query)

def _qarg(q, name, default=""):
    return (q.get(name, [default])[0]) or default

def _qbool(q, name, default=False):
    raw = _qarg(q, name, "1" if default else "0")
    return raw.lower() in ("1", "true", "yes")

def _qint(q, name, default=0):
    try: return int(_qarg(q, name, str(default)) or str(default))
    except ValueError: return default


def _t_follow(q):
    user = _qarg(q, "user", "TestUser")
    events_bus.publish({"type": "follow", "user": user, "login": user.lower(), "at": ""})
    return 200, f"sent follow for {user}\n"

def _t_sub(q):
    user = _qarg(q, "user", "TestUser")
    tier = _qarg(q, "tier", "1000")
    gift = _qbool(q, "gift")
    events_bus.publish({"type": "sub", "user": user, "login": user.lower(),
                        "tier": tier, "gift": gift})
    return 200, f"sent sub for {user} tier={tier} gift={gift}\n"

def _t_resub(q):
    user = _qarg(q, "user", "TestUser")
    tier = _qarg(q, "tier", "1000")
    months = _qint(q, "months", 3)
    message = _qarg(q, "message", "Тестовое сообщение от ресабера")
    events_bus.publish({"type": "resub", "user": user, "login": user.lower(),
                        "tier": tier, "months": months, "streak": months,
                        "duration": 1, "message": message})
    return 200, f"sent resub for {user} {months}mo\n"

def _t_raid(q):
    user = _qarg(q, "user", "TestUser")
    viewers = _qint(q, "viewers", 42)
    events_bus.publish({"type": "raid", "user": user, "login": user.lower(),
                        "viewers": viewers})
    return 200, f"sent raid from {user} viewers={viewers}\n"

def _t_subgift(q):
    user = _qarg(q, "user", "TestUser")
    tier = _qarg(q, "tier", "1000")
    total = _qint(q, "total", 5)
    anon = _qbool(q, "anon")
    events_bus.publish({
        "type": "subgift",
        "user": "Аноним" if anon else user,
        "login": "" if anon else user.lower(),
        "tier": tier, "total": total, "cumulative": total, "anon": anon,
    })
    return 200, f"sent subgift {total} from {user} anon={anon}\n"

def _t_donation(q):
    import uuid
    user = _qarg(q, "user", "TestDonator")
    try:
        amount = int(_qarg(q, "amount", "100"))
    except ValueError:
        amount = 100
    message  = _qarg(q, "message", "Тестовый донат")
    currency = _qarg(q, "currency", "RUB")
    goal     = _qarg(q, "goal", "")
    donatty_bus.publish({
        "type": "donation", "id": uuid.uuid4().hex, "user": user, "message": message,
        "amount": amount, "currency": currency, "goal": goal,
    })
    return 200, f"sent donation {amount} {currency} from {user}\n"


def _t_emote_rain(q):
    char = _qarg(q, "char", "🔥")
    count = max(1, min(_qint(q, "count", 10), 200))
    emote_bus.publish({"emotes": [{"type": "emoji", "char": char} for _ in range(count)]})
    return 200, f"sent {count}x {char}\n"


def _t_yt_play(q):
    raw = _qarg(q, "v")
    vid = parse_youtube_id(raw)
    if not vid:
        return 400, "bad video id/url\n"
    start = _qint(q, "start", 0)
    yt_vote.start(vid)
    media_bus.publish({"evt": "play", "id": vid, "start": start})
    return 200, f"playing {vid}\n"

def _t_yt_stop(q):
    with yt_queue_lock:
        yt_queue.clear()
    yt_vote.stop()
    media_bus.publish({"evt": "stop"})
    return 200, "stopped\n"


TEST_ROUTES = {
    "/test/follow":       _t_follow,
    "/test/sub":          _t_sub,
    "/test/resub":        _t_resub,
    "/test/raid":         _t_raid,
    "/test/subgift":      _t_subgift,
    "/test/donation":     _t_donation,
    "/test/emote_rain":   _t_emote_rain,
    "/test/yt/play":      _t_yt_play,
    "/test/yt/stop":      _t_yt_stop,
}


class _OverlayHandler(http.server.BaseHTTPRequestHandler):
    def _check_token(self, q):
        """Защита write-эндпойнтов. Если OVERLAY_TOKEN пуст — допускаем только запросы
        без cross-origin Origin/Referer ИЛИ с Origin/Referer на наш собственный порт.
        Это блокирует CSRF из сторонних сайтов и из других localhost-приложений на других портах."""
        if OVERLAY_TOKEN:
            return hmac.compare_digest(_qarg(q, "token"), OVERLAY_TOKEN)
        origin = self.headers.get("Origin") or self.headers.get("Referer") or ""
        if not origin:
            return True  # запрос из самой OBS-страницы / curl без заголовка
        try:
            u = urllib.parse.urlparse(origin)
        except ValueError:
            return False
        host = (u.hostname or "")
        port = u.port or (443 if u.scheme == "https" else 80)
        return host in ("localhost", "127.0.0.1", "::1") and port == OVERLAY_PORT

    def _send_text(self, status, body, ctype="text/plain; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        if body:
            try: self.wfile.write(body)
            except OSError: pass

    def _serve_stream(self, bus, send_config=False):
        # БЕЗ Access-Control-Allow-Origin: оверлеи открываются с того же origin
        # (localhost:OVERLAY_PORT), OBS Browser Source CORS не проверяет.
        # ACAO:* позволял бы любому открытому сайту читать чат/донаты в реальном времени.
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        if send_config:
            cfg = json.dumps({"channel": CHANNEL.lstrip("#")}, ensure_ascii=False)
            try:
                self.wfile.write(f"event: config\ndata: {cfg}\n\n".encode("utf-8"))
                self.wfile.flush()
            except OSError:
                return
        q = bus.subscribe()
        try:
            while True:
                try:
                    data = q.get(timeout=15)
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            bus.unsubscribe(q)

    def do_GET(self):
        if self.path == "/stream":
            self._serve_stream(chat_bus, send_config=True);  return
        if self.path == "/events":
            self._serve_stream(events_bus, send_config=True); return
        if self.path == "/media":
            self._serve_stream(media_bus, send_config=False); return
        if self.path == "/dice":
            self._serve_stream(dice_bus, send_config=False); return
        if self.path == "/donatty":
            self._serve_stream(donatty_bus, send_config=False); return
        if self.path == "/emote_rain":
            self._serve_stream(emote_bus, send_config=False); return

        path_only = urllib.parse.urlparse(self.path).path
        name = path_only.lstrip("/")
        if not name and self.path == "/":
            name = "index"
        bare = name[:-5] if name.endswith(".html") else name
        if bare in STATIC_HTML:
            body = _OVERLAY_CACHE.get(bare)
            if body is None:
                self.send_response(404); self.end_headers(); return
            # Инжектим OVERLAY_TOKEN в страницу, чтобы её JS мог дёргать /yt/ended и /test/*.
            tok_json = json.dumps(OVERLAY_TOKEN or "")
            inject = f'<script>window.OVERLAY_TOKEN={tok_json};</script>'.encode("utf-8")
            m = _HEAD_OPEN_RE.search(body)
            if m:
                page = body[:m.end()] + inject + body[m.end():]
            else:
                page = inject + body
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(page)
            return
        if path_only == "/":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            links = "".join(f'<li><a href="/{n}.html">/{n}.html</a></li>' for n in sorted(STATIC_HTML))
            self.wfile.write(f"<!doctype html><meta charset=utf-8><title>overlays</title><ul>{links}</ul>".encode("utf-8"))
            return
        if path_only == "/yt/ended":
            q = _qs(self.path)
            if not self._check_token(q):
                self._send_text(403, "forbidden: add ?token=<OVERLAY_TOKEN>\n")
                return
            yt_advance()
            self._send_text(204, b"")
            return
        if path_only in TEST_ROUTES:
            q = _qs(self.path)
            if not self._check_token(q):
                self._send_text(403, "forbidden: add ?token=<OVERLAY_TOKEN>\n")
                return
            try:
                status, body = TEST_ROUTES[path_only](q)
            except Exception as e:
                status, body = 500, f"error: {e}\n"
            self._send_text(status, body)
            return
        self.send_response(404); self.end_headers()

    def log_message(self, *_):
        pass


class _QuietHTTPServer(http.server.ThreadingHTTPServer):
    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionAbortedError, ConnectionResetError, BrokenPipeError, TimeoutError)):
            return
        super().handle_error(request, client_address)


def start_overlay_server():
    _preload_overlays()
    if not OVERLAY_TOKEN:
        print("(!) OVERLAY_TOKEN пуст — write-эндпойнты доступны только same-origin "
              f"(http://localhost:{OVERLAY_PORT}). Установи OVERLAY_TOKEN для жёсткой защиты.")
    srv = _QuietHTTPServer(("localhost", OVERLAY_PORT), _OverlayHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
