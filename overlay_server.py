"""Локальный HTTP-сервер: SSE-стримы для оверлеев, WebSocket для медиа, статика, тесты."""

import base64
import hashlib
import hmac
import http.server
import json
import os
import queue
import re
import struct
import sys
import threading
import time
import urllib.parse

from config import CHANNEL, OVERLAY_PORT, OVERLAY_TOKEN
from events import chat_bus, dice_bus, donatty_bus, events_bus, goal_bus, image_bus, media_bus
import goal
import log
from youtube import (
    parse_youtube_id, yt_advance, yt_health_tick, yt_queue, yt_queue_lock,
    yt_report_pos, yt_vote,
)


# ---------- WebSocket (RFC 6455) — рукопашно поверх http.server, без зависимостей ----------
# Нужен для медиа-канала (youtube): одно двунаправленное соединение вместо SSE-вниз +
# обходного канала вверх. Так не упираемся в лимит соединений Chromium на хост (из-за
# которого постоянное SSE забивало пул и запросы оверлей→сервер не проходили).

_WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def _ws_accept(key):
    return base64.b64encode(hashlib.sha1((key + _WS_GUID).encode()).digest()).decode()


def _ws_encode(payload, opcode=0x1):
    """Кадр сервер→клиент (без маски). opcode: 0x1 text, 0x8 close, 0x9 ping, 0xA pong."""
    out = bytearray([0x80 | opcode])
    n = len(payload)
    if n < 126:
        out.append(n)
    elif n < 65536:
        out.append(126); out += struct.pack(">H", n)
    else:
        out.append(127); out += struct.pack(">Q", n)
    out += payload
    return bytes(out)


def _ws_read_frame(rfile):
    """Читает один кадр клиент→сервер. Возвращает (opcode, payload_bytes) или None при
    закрытии/обрыве. Кадры от клиента всегда маскированы — снимаем маску."""
    hdr = rfile.read(2)
    if len(hdr) < 2:
        return None
    masked = hdr[1] & 0x80
    n = hdr[1] & 0x7f
    if n == 126:
        ext = rfile.read(2)
        if len(ext) < 2:
            return None
        n = struct.unpack(">H", ext)[0]
    elif n == 127:
        ext = rfile.read(8)
        if len(ext) < 8:
            return None
        n = struct.unpack(">Q", ext)[0]
    mask = rfile.read(4) if masked else b""
    data = rfile.read(n) if n else b""
    if len(data) < n or (masked and len(mask) < 4):
        return None
    if masked:
        data = bytes(data[i] ^ mask[i % 4] for i in range(n))
    return hdr[0] & 0x0f, data


STATIC_HTML = {"alerts", "chat", "dice", "donatty", "goal", "images", "webcam", "youtube"}

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


def _t_goal_reset(q):
    goal.reset_for_test()
    return 200, "goal reset\n"


def _t_image(q):
    url = _qarg(q, "url", "")
    if not url:
        return 400, "need ?url=<direct image url>\n"
    image_bus.publish({"url": url, "user": _qarg(q, "user", "TestUser")})
    return 200, f"sent image {url}\n"


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
    "/test/goal_reset":   _t_goal_reset,
    "/test/image":        _t_image,
    "/test/yt/play":      _t_yt_play,
    "/test/yt/stop":      _t_yt_stop,
}


class _OverlayHandler(http.server.BaseHTTPRequestHandler):
    def _is_same_origin(self):
        """True, если запрос пришёл с нашей же страницы (Origin/Referer на наш порт)
        либо вовсе без них (запрос самой OBS-страницы / curl). Сторонний сайт не может
        подделать Referer на localhost:OVERLAY_PORT — это надёжная CSRF-защита."""
        origin = self.headers.get("Origin") or self.headers.get("Referer") or ""
        if not origin:
            return True
        try:
            u = urllib.parse.urlparse(origin)
        except ValueError:
            return False
        host = (u.hostname or "")
        port = u.port or (443 if u.scheme == "https" else 80)
        return host in ("localhost", "127.0.0.1", "::1") and port == OVERLAY_PORT

    def _check_token(self, q):
        """Защита write-эндпойнтов. Если OVERLAY_TOKEN задан — нужен верный token,
        иначе допускаем только same-origin (запрос со своей же страницы)."""
        if OVERLAY_TOKEN:
            return hmac.compare_digest(_qarg(q, "token"), OVERLAY_TOKEN)
        return self._is_same_origin()

    def _send_text(self, status, body, ctype="text/plain; charset=utf-8"):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.end_headers()
        if body:
            try: self.wfile.write(body)
            except OSError: pass

    def _serve_stream(self, bus, send_config=False, initial_event=None):
        # БЕЗ Access-Control-Allow-Origin: оверлеи открываются с того же origin
        # (localhost:OVERLAY_PORT), OBS Browser Source CORS не проверяет.
        # ACAO:* позволял бы любому открытому сайту читать чат/донаты в реальном времени.
        self.send_response(200)
        # charset=utf-8 нужен явно: CEF в OBS Browser Source без него иногда
        # пытается декодировать SSE как Latin-1 и портит кириллицу.
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        # Шины с состоянием (media) подписывают атомарно вместе со снапшотом текущего
        # состояния, чтобы переподключившийся оверлей сразу ресинхронизировался.
        if hasattr(bus, "subscribe_with_snapshot"):
            q, snapshot = bus.subscribe_with_snapshot()
        else:
            q, snapshot = bus.subscribe(), []
        try:
            if send_config:
                cfg = json.dumps({"channel": CHANNEL.lstrip("#")}, ensure_ascii=False)
                self.wfile.write(f"event: config\ndata: {cfg}\n\n".encode("utf-8"))
            for ev in ([initial_event] if initial_event is not None else []) + list(snapshot):
                payload = json.dumps(ev, ensure_ascii=False)
                self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
            self.wfile.flush()
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

    def _serve_websocket(self, bus):
        """Двунаправленный канал для медиа-оверлея. Вниз: команды из bus (play/stop/tick/…)
        + снапшот текущего состояния при подключении. Вверх: отчёты оверлея (hello/pos/ended).
        Один сокет на обе стороны — мимо лимита соединений Chromium на хост."""
        key = self.headers.get("Sec-WebSocket-Key")
        if not key:
            self.send_response(400); self.end_headers(); return
        # Рукопожатие пишем вручную: WebSocket требует статус-строку HTTP/1.1, а у сервера
        # protocol_version="HTTP/1.0" (менять его глобально нельзя — сломает SSE/статику,
        # которые шлются без Content-Length).
        self.wfile.write((
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {_ws_accept(key)}\r\n"
            "\r\n"
        ).encode("ascii"))
        self.wfile.flush()

        q, snapshot = bus.subscribe_with_snapshot()
        send_lock = threading.Lock()
        alive = threading.Event(); alive.set()

        def send_text(s):
            frame = _ws_encode(s.encode("utf-8"), 0x1)
            with send_lock:
                self.wfile.write(frame); self.wfile.flush()

        def writer():
            # Снапшот состояния → потом события из шины. Пустой get-таймаут шлёт ping,
            # чтобы держать соединение и быстро замечать обрыв. На закрытии reader кладёт
            # в очередь None — будит writer сразу, без ожидания таймаута. Перехват широкий:
            # после finish() сокета write даёт ValueError (закрытый файл), а не только OSError.
            try:
                for ev in snapshot:
                    send_text(json.dumps(ev, ensure_ascii=False))
                while alive.is_set():
                    try:
                        data = q.get(timeout=15)
                    except queue.Empty:
                        with send_lock:
                            self.wfile.write(_ws_encode(b"", 0x9)); self.wfile.flush()
                        continue
                    if data is None:        # сентинел закрытия
                        break
                    send_text(data)
            except Exception:
                pass
            finally:
                alive.clear()

        wt = threading.Thread(target=writer, daemon=True)
        wt.start()
        try:
            while alive.is_set():
                frame = _ws_read_frame(self.rfile)
                if frame is None:
                    break
                opcode, data = frame
                if opcode == 0x8:           # close
                    break
                if opcode == 0x9:           # ping → pong
                    with send_lock:
                        self.wfile.write(_ws_encode(data, 0xA)); self.wfile.flush()
                    continue
                if opcode != 0x1:           # игнорируем pong/бинарь/продолжение
                    continue
                try:
                    msg = json.loads(data.decode("utf-8"))
                except (ValueError, UnicodeDecodeError):
                    continue
                self._handle_ws_message(msg)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            alive.clear()
            try: q.put_nowait(None)   # разбудить writer, чтобы не висел до таймаута
            except queue.Full: pass
            bus.unsubscribe(q)
            self.close_connection = True  # не пытаться читать следующий запрос из ws-сокета

    def _handle_ws_message(self, msg):
        """Отчёт оверлея по WS. Аналог прежних /yt/hello, /yt/pos, /yt/ended."""
        evt = msg.get("evt")
        if evt == "pos":
            try: t, d = int(msg.get("t", -1)), int(msg.get("d", -1))
            except (TypeError, ValueError): return
            yt_report_pos(msg.get("id") or None, t, d)
        elif evt == "ended":
            yt_advance(msg.get("id") or None)
        elif evt == "hello":
            log.log(f"(youtube) overlay подключился (ws), версия страницы v={msg.get('v', '?')}")

    def do_GET(self):
        if self.path == "/stream":
            self._serve_stream(chat_bus, send_config=True);  return
        if self.path == "/events":
            self._serve_stream(events_bus, send_config=True); return
        if self.path == "/dice":
            self._serve_stream(dice_bus, send_config=False); return
        if self.path == "/donatty":
            self._serve_stream(donatty_bus, send_config=False); return
        if self.path == "/images":
            self._serve_stream(image_bus, send_config=False); return
        if self.path == "/goal":
            self._serve_stream(goal_bus, send_config=False, initial_event=goal.snapshot()); return

        path_only = urllib.parse.urlparse(self.path).path
        if path_only == "/ws/media":
            q = _qs(self.path)
            if not self._check_token(q):
                self.send_response(403); self.end_headers(); return
            self._serve_websocket(media_bus); return

        name = path_only.lstrip("/")
        if not name and self.path == "/":
            name = "index"
        bare = name[:-5] if name.endswith(".html") else name
        if bare in STATIC_HTML:
            body = _OVERLAY_CACHE.get(bare)
            if body is None:
                self.send_response(404); self.end_headers(); return
            # Инжектим OVERLAY_TOKEN в страницу, чтобы её JS мог дёргать /test/*.
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


def _media_heartbeat():
    """Пульс для оверлея: пока клип играет, раз в секунду шлём tick по WS (/ws/media).
    По тику оверлей читает позицию плеера и шлёт её обратно, по чему сервер ловит реальный
    конец клипа."""
    while True:
        time.sleep(1.0)
        try:
            # Пульс шлём только когда клип играет И оверлей реально подключён —
            # незачем дёргать шину, если никто не слушает.
            if yt_vote.is_playing() and media_bus.has_clients():
                media_bus.publish({"evt": "tick"})
            yt_health_tick()  # громко предупредит, если позиции от оверлея перестали идти
        except Exception:
            pass


def start_overlay_server():
    _preload_overlays()
    if not OVERLAY_TOKEN:
        print("(!) OVERLAY_TOKEN пуст — write-эндпойнты доступны только same-origin "
              f"(http://localhost:{OVERLAY_PORT}). Установи OVERLAY_TOKEN для жёсткой защиты.")
    # Слушаем явно на 127.0.0.1 (а не "localhost", который на Windows может резолвиться
    # в ::1): и localhost, и 127.0.0.1 от клиента сюда попадают (Happy Eyeballs).
    srv = _QuietHTTPServer(("127.0.0.1", OVERLAY_PORT), _OverlayHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    threading.Thread(target=_media_heartbeat, daemon=True).start()
