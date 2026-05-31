"""HTTPS-клиент с переиспользованием соединений (keep-alive).

Thread-local пул: на каждый поток — своя HTTPSConnection на каждый хост.
Это убирает TLS-handshake на каждом Helix/EventSub/YouTube запросе."""

import http.client
import io
import socket
import threading
import urllib.error
import urllib.parse


_tls = threading.local()


def _conn(host, port=443, timeout=10):
    conns = getattr(_tls, "conns", None)
    if conns is None:
        conns = {}
        _tls.conns = conns
    key = (host, port)
    conn = conns.get(key)
    if conn is None:
        conn = http.client.HTTPSConnection(host, port, timeout=timeout)
        conns[key] = conn
    return conn


def request(method, url, headers=None, body=None, timeout=10):
    """Выполняет HTTP-запрос с переиспользованием соединения.
    Возвращает (status, headers_dict, body_bytes). При HTTP >=400 — кидает HTTPError
    с уже прочитанным телом, как делает urllib.request.urlopen."""
    u = urllib.parse.urlparse(url)
    host = u.hostname
    port = u.port or 443
    path = u.path + (("?" + u.query) if u.query else "")
    hdrs = dict(headers or {})
    hdrs.setdefault("Connection", "keep-alive")
    for attempt in (1, 2):
        try:
            conn = _conn(host, port, timeout)
            conn.request(method, path, body=body, headers=hdrs)
            resp = conn.getresponse()
            data = resp.read()
            if resp.status >= 400:
                # fp=BytesIO(data), чтобы у вызывающего работал e.read()
                # — как у urlopen-овской HTTPError.
                raise urllib.error.HTTPError(
                    url, resp.status, resp.reason, resp.headers, io.BytesIO(data)
                )
            return resp.status, dict(resp.getheaders()), data
        except urllib.error.HTTPError:
            raise
        except (http.client.HTTPException, ConnectionError, OSError, socket.timeout):
            # соединение могло протухнуть — сносим и пробуем ещё раз
            conns = getattr(_tls, "conns", {}) or {}
            old = conns.pop((host, port), None)
            if old is not None:
                try: old.close()
                except Exception: pass
            if attempt == 2:
                raise
