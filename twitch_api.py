"""Twitch API: OAuth, Helix, бейджи, рендер эмоутов."""

import hmac
import http.server
import json
import os
import re
import secrets
import threading
import time
import urllib.parse
import webbrowser

import http_pool
import log
from config import CLIENT_ID, REDIRECT, SCOPES, TOKEN_FILE


# ---------- OAuth Implicit Flow с локальным сервером ----------

CAPTURE_HTML = b"""<!doctype html><meta charset="utf-8"><body>
<p id="s">Authorizing...</p>
<script>
  const p = new URLSearchParams(location.hash.slice(1));
  if (p.get("access_token")) {
    fetch("/save?" + p.toString())
      .then(() => document.getElementById("s").innerText = "Done. You can close this tab.");
  } else {
    document.getElementById("s").innerText = "No token in URL.";
  }
</script></body>"""


class _OAuthHandler(http.server.BaseHTTPRequestHandler):
    captured = {}
    expected_state = ""

    def do_GET(self):
        if self.path.startswith("/save"):
            q = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            data = {k: v[0] for k, v in q.items()}
            got_state = data.get("state", "")
            if not _OAuthHandler.expected_state or not hmac.compare_digest(
                got_state, _OAuthHandler.expected_state
            ):
                # Чужой запрос (или с устаревшим state) — игнорируем, не сохраняем токен.
                self.send_response(403); self.end_headers(); self.wfile.write(b"bad state")
                return
            _OAuthHandler.captured = data
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(CAPTURE_HTML)

    def log_message(self, *_):
        pass


def _fetch_self(token):
    """Возвращает (login, user_id) для владельца токена."""
    _, _, body = http_pool.request(
        "GET",
        "https://api.twitch.tv/helix/users",
        headers={"Authorization": f"Bearer {token}", "Client-Id": CLIENT_ID},
    )
    d = json.loads(body)["data"][0]
    return d["login"], d["id"]


def _validate_token(token):
    """Возвращает множество скоупов токена или None если токен невалиден."""
    try:
        _, _, body = http_pool.request(
            "GET",
            "https://id.twitch.tv/oauth2/validate",
            headers={"Authorization": f"OAuth {token}"},
            timeout=10,
        )
        return set(json.loads(body).get("scopes", []))
    except Exception:
        return None


def helix_get(url, token):
    _, _, body = http_pool.request(
        "GET", url,
        headers={"Authorization": f"Bearer {token}", "Client-Id": CLIENT_ID},
        timeout=10,
    )
    return json.loads(body)


def get_chatters(token, broadcaster_id, moderator_id):
    """GET /helix/chat/chatters — все, кто сейчас в чате канала.

    Требует скоуп moderator:read:chatters и чтобы moderator_id был модератором канала.
    Возвращает dict {user_id: (user_login, user_name)}. Пагинируется до конца."""
    out: dict[str, tuple[str, str]] = {}
    cursor = ""
    while True:
        url = (
            "https://api.twitch.tv/helix/chat/chatters"
            f"?broadcaster_id={broadcaster_id}"
            f"&moderator_id={moderator_id}"
            f"&first=1000"
        )
        if cursor:
            url += f"&after={urllib.parse.quote(cursor)}"
        data = helix_get(url, token)
        for ch in data.get("data", []):
            uid = ch.get("user_id")
            if uid:
                out[uid] = (ch.get("user_login", ""), ch.get("user_name", ""))
        cursor = (data.get("pagination") or {}).get("cursor", "")
        if not cursor:
            break
    return out


def helix_get_channel(token, broadcaster_id):
    """GET /helix/channels — текущая информация о канале (title, game, tags).

    Возвращает dict из data[0]: title, game_id, game_name, tags, broadcaster_language."""
    data = helix_get(
        f"https://api.twitch.tv/helix/channels?broadcaster_id={broadcaster_id}",
        token,
    )
    rows = data.get("data") or []
    if not rows:
        raise RuntimeError("helix /channels вернул пустой data")
    return rows[0]


def helix_patch_channel(token, broadcaster_id, title=None, tags=None):
    """PATCH /helix/channels — обновляет заголовок и/или теги стрима.

    Требует скоуп channel:manage:broadcast. Передаются только переданные поля —
    остальные (категория, язык) не трогаем. Теги: список из <=10 строк
    по 1-25 символов латиницы/цифр/_."""
    body_obj = {}
    if title is not None:
        body_obj["title"] = title
    if tags is not None:
        body_obj["tags"] = tags
    if not body_obj:
        return
    url = f"https://api.twitch.tv/helix/channels?broadcaster_id={broadcaster_id}"
    http_pool.request(
        "PATCH", url,
        headers={
            "Authorization": f"Bearer {token}",
            "Client-Id": CLIENT_ID,
            "Content-Type": "application/json",
        },
        body=json.dumps(body_obj).encode("utf-8"),
        timeout=10,
    )


def helix_send_announcement(token, broadcaster_id, moderator_id, message, color="primary"):
    """POST /helix/chat/announcements — публикует объявление в чате (выделенный блок).

    Требует scope moderator:manage:announcements и чтобы moderator_id был модером канала.
    color: primary|blue|green|orange|purple. Длина message — до 500 символов.
    Rate-limit ≈1 анонс / 2 сек на канал, иначе 429 (поднимется HTTPError)."""
    url = (
        "https://api.twitch.tv/helix/chat/announcements"
        f"?broadcaster_id={broadcaster_id}&moderator_id={moderator_id}"
    )
    http_pool.request(
        "POST", url,
        headers={
            "Authorization": f"Bearer {token}",
            "Client-Id": CLIENT_ID,
            "Content-Type": "application/json",
        },
        body=json.dumps({"message": message, "color": color}).encode("utf-8"),
        timeout=10,
    )


def helix_delete_message(token, broadcaster_id, moderator_id, message_id):
    """DELETE /helix/moderation/chat — удалить одно сообщение по его id из IRC-тега.

    Требует скоуп moderator:manage:chat_messages и чтобы moderator_id был модератором
    канала (для стримера это его собственный user_id)."""
    url = (
        "https://api.twitch.tv/helix/moderation/chat"
        f"?broadcaster_id={broadcaster_id}"
        f"&moderator_id={moderator_id}"
        f"&message_id={message_id}"
    )
    http_pool.request(
        "DELETE", url,
        headers={"Authorization": f"Bearer {token}", "Client-Id": CLIENT_ID},
        timeout=10,
    )


# Кэш follow-статуса. user_id -> (is_follower, expires_at монотонные секунды).
_follower_cache: dict[str, tuple[bool, float]] = {}
_follower_lock = threading.Lock()
_FOLLOWER_TTL = 600  # 10 минут — компромисс между свежестью и числом запросов.


def is_follower(token, broadcaster_id, user_id):
    """Проверка через Helix /channels/followers с in-memory кэшем (TTL 10 мин).

    Возвращает True/False; при ошибках сети — False (fail-safe: считаем не-фолловером,
    раз уж модерация в этой ветке строже)."""
    if not user_id or not broadcaster_id:
        return False
    now = time.monotonic()
    with _follower_lock:
        cached = _follower_cache.get(user_id)
        if cached and cached[1] > now:
            return cached[0]
    try:
        data = helix_get(
            "https://api.twitch.tv/helix/channels/followers"
            f"?broadcaster_id={broadcaster_id}&user_id={user_id}",
            token,
        )
        result = bool(data.get("data"))
    except Exception:
        return False
    with _follower_lock:
        _follower_cache[user_id] = (result, now + _FOLLOWER_TTL)
    return result


def mark_follower(user_id):
    """Принудительно отметить юзера как фолловера (например, на event 'channel.follow')."""
    if not user_id:
        return
    with _follower_lock:
        _follower_cache[user_id] = (True, time.monotonic() + _FOLLOWER_TTL)


def authorize():
    # Сбрасываем class-level captured на случай повторного вызова в том же процессе
    # (например, после инвалидации токена).
    _OAuthHandler.captured = {}
    state = secrets.token_urlsafe(24)
    _OAuthHandler.expected_state = state
    srv = http.server.HTTPServer(("localhost", 3000), _OAuthHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    params = urllib.parse.urlencode({
        "response_type": "token",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT,
        "scope": SCOPES,
        "state": state,
    })
    url = f"https://id.twitch.tv/oauth2/authorize?{params}"
    log.log(f"Открываю браузер для авторизации: {url}")
    webbrowser.open(url)

    # Ждём колбэк OAuth не вечно: если пользователь закрыл вкладку или Twitch вернул
    # ошибку без access_token — выходим и даём вызвавшему обработать исключение.
    OAUTH_TIMEOUT = 5 * 60
    deadline = time.monotonic() + OAUTH_TIMEOUT
    while "access_token" not in _OAuthHandler.captured:
        if time.monotonic() >= deadline:
            srv.shutdown()
            _OAuthHandler.expected_state = ""
            raise TimeoutError(f"OAuth не завершён за {OAUTH_TIMEOUT} с")
        time.sleep(0.2)
    srv.shutdown()
    _OAuthHandler.expected_state = ""

    token = _OAuthHandler.captured["access_token"]
    nick, user_id = _fetch_self(token)
    with open(TOKEN_FILE, "w", encoding="utf-8") as f:
        json.dump({"token": token, "nick": nick, "user_id": user_id, "scopes": SCOPES}, f)
    log.log(f"Авторизован как {nick} (id={user_id}), токен сохранён в {TOKEN_FILE}")
    return token, nick, user_id


def load_credentials():
    """Возвращает (token, nick, user_id). Если в файле нет нужных скоупов — переавторизация."""
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            d = json.load(f)
        required = set(SCOPES.split())
        scopes_now = _validate_token(d["token"])
        if scopes_now is None:
            log.log("(!) Токен невалиден, перезапрашиваю авторизацию.")
        elif not required.issubset(scopes_now):
            missing = required - scopes_now
            log.log(f"(!) Токен без нужных скоупов: {' '.join(missing)} — нужна переавторизация.")
        else:
            user_id = d.get("user_id") or _fetch_self(d["token"])[1]
            return d["token"], d["nick"], user_id
        try:
            os.remove(TOKEN_FILE)
        except OSError:
            pass
    return authorize()


# ---------- Бейджи + эмоуты ----------

# (set_id, version) -> URL PNG-картинки 1x
BADGE_MAP: dict[tuple[str, str], str] = {}


def load_badges(token, channel_login):
    """Загружает глобальные бейджи + бейджи канала. Возвращает broadcaster_id канала (или None)."""
    BADGE_MAP.clear()
    try:
        users = helix_get(f"https://api.twitch.tv/helix/users?login={channel_login}", token)
        broadcaster_id = users["data"][0]["id"]
    except Exception as e:
        log.log(f"(не удалось получить broadcaster_id канала {channel_login}: {e})")
        broadcaster_id = None

    urls = ["https://api.twitch.tv/helix/chat/badges/global"]
    if broadcaster_id:
        urls.append(f"https://api.twitch.tv/helix/chat/badges?broadcaster_id={broadcaster_id}")

    for url in urls:
        try:
            data = helix_get(url, token)
            for set_obj in data.get("data", []):
                set_id = set_obj["set_id"]
                for v in set_obj["versions"]:
                    BADGE_MAP[(set_id, v["id"])] = v.get("image_url_1x") or v.get("image_url")
        except Exception as e:
            log.log(f"(не удалось загрузить {url}: {e})")
    return broadcaster_id


EMOTE_URL = "https://static-cdn.jtvnw.net/emoticons/v2/{id}/default/dark/1.0"
# Большая версия с CDN — для оверлея-дождя, чтобы не пикселило при увеличении.
EMOTE_URL_BIG = "https://static-cdn.jtvnw.net/emoticons/v2/{id}/default/dark/3.0"


def escape_html(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
             .replace('"', "&quot;").replace("'", "&#39;"))


def render_with_emotes(text, emotes_tag):
    """Возвращает HTML с подставленными <img> на месте эмоутов. Остальной текст экранируется.

    Twitch отдаёт позиции эмоутов в UTF-16 code units (суррогатная пара = 2),
    поэтому работаем через utf-16-le, иначе на сообщениях с emoji сдвигаемся."""
    spans = []  # (start, end, emote_id) в UTF-16 code units
    for entry in (emotes_tag or "").split("/"):
        if ":" not in entry:
            continue
        emote_id, positions = entry.split(":", 1)
        for pos in positions.split(","):
            if "-" in pos:
                a, b = pos.split("-", 1)
                try:
                    spans.append((int(a), int(b), emote_id))
                except ValueError:
                    pass
    if not spans:
        return escape_html(text)
    spans.sort()
    buf = text.encode("utf-16-le", errors="replace")  # 2 байта на code unit
    def slice_u16(u16_start, u16_end):
        # u16_end включительно → срез [start*2 : (end+1)*2]
        return buf[u16_start * 2: (u16_end + 1) * 2].decode("utf-16-le", errors="replace")
    out = []
    cursor = 0  # в UTF-16 code units
    total_u16 = len(buf) // 2
    for start, end, emote_id in spans:
        if start < cursor or start >= total_u16:
            continue  # перекрытие или выход за пределы — пропускаем
        end = min(end, total_u16 - 1)
        out.append(escape_html(slice_u16(cursor, start - 1)) if start > cursor else "")
        alt = escape_html(slice_u16(start, end))
        out.append(f'<img class="emote" src="{EMOTE_URL.format(id=emote_id)}" alt="{alt}" title="{alt}">')
        cursor = end + 1
    if cursor < total_u16:
        out.append(escape_html(slice_u16(cursor, total_u16 - 1)))
    return "".join(out)


_EMOJI_RE = re.compile(
    "["
    "\U0001F1E6-\U0001F1FF"
    "\U0001F300-\U0001F5FF"
    "\U0001F600-\U0001F64F"
    "\U0001F680-\U0001F6FF"
    "\U0001F700-\U0001F77F"
    "\U0001F780-\U0001F7FF"
    "\U0001F800-\U0001F8FF"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FAFF"
    "\U00002600-\U000026FF"
    "\U00002700-\U000027BF"
    "]",
    re.UNICODE,
)


def extract_emotes(text, emotes_tag):
    """Возвращает список «частиц» из сообщения для оверлея-дождя:
    {type:'img', url, alt} для твич-эмоутов и {type:'emoji', char} для юникод-эмодзи.
    Порядок не важен — оверлей всё равно рандомит позиции."""
    out = []
    buf = text.encode("utf-16-le", errors="replace") if emotes_tag else b""
    for entry in (emotes_tag or "").split("/"):
        if ":" not in entry:
            continue
        emote_id, positions = entry.split(":", 1)
        url = EMOTE_URL_BIG.format(id=emote_id)
        alt = emote_id
        ranges = [p for p in positions.split(",") if "-" in p]
        if ranges:
            try:
                a, b = (int(x) for x in ranges[0].split("-", 1))
                alt = buf[a * 2:(b + 1) * 2].decode("utf-16-le", errors="replace") or emote_id
            except ValueError:
                pass
        for _ in ranges:
            out.append({"type": "img", "url": url, "alt": alt})
    for m in _EMOJI_RE.finditer(text):
        out.append({"type": "emoji", "char": m.group(0)})
    return out


def role_from_badges(tags):
    """Возвращает 'broadcaster' / 'mod' / 'vip' / '' по бейджам."""
    badges = tags.get("badges", "")
    if "broadcaster/" in badges:
        return "broadcaster"
    if "lead_moderator/" in badges or "moderator/" in badges:
        return "mod"
    if "vip/" in badges:
        return "vip"
    return ""


# Кэш: одна и та же строка badges= встречается у юзера на каждом сообщении.
_badges_cache: dict[str, list] = {}
_BADGES_CACHE_MAX = 1024


def resolve_badges(tags):
    """Из тега badges=set/ver,... в список {url, alt} для рендера в оверлее."""
    raw = tags.get("badges", "")
    cached = _badges_cache.get(raw)
    if cached is not None:
        return cached
    out = []
    for b in raw.split(","):
        if "/" not in b:
            continue
        set_id, ver = b.split("/", 1)
        url = BADGE_MAP.get((set_id, ver)) or BADGE_MAP.get((set_id, "1"))
        if url:
            out.append({"url": url, "alt": set_id})
    if len(_badges_cache) >= _BADGES_CACHE_MAX:
        _badges_cache.clear()
    _badges_cache[raw] = out
    return out
