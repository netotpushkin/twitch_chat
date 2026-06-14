"""
Самый простой Twitch чат-бот.

Первый запуск:
    1. Зарегистрируй приложение на https://dev.twitch.tv/console/apps
       OAuth Redirect URL: http://localhost:3000
    2. Пропиши CLIENT_ID и CHANNEL в .env (TWITCH_CLIENT_ID / TWITCH_CHANNEL).
    3. python bot.py  — откроется браузер для авторизации.

Дальше токен лежит в token.json — браузер больше не открывается.
"""

import collections
import re
import socket
import ssl
import sys
import threading
import time
import ipaddress
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor

from commands import BOT_COMMANDS
from config import CHANNEL, CLIENT_ID, DONATTY_REF, DONATTY_TOKEN, OVERLAY_PORT, OVERLAY_TOKEN
import dice
import donatty
import economy
from events import chat_bus, dice_bus, image_bus
import goal
import king
import log
import moderation
import tts
from prompt import Prompt
import titler
import twitch_api
from twitch_api import (
    escape_html, load_badges, load_credentials,
    render_with_emotes, resolve_badges, role_from_badges,
)
import youtube
from youtube import (
    YT_VOTE_WINDOW, yt_advance, yt_close_voting, yt_publish_vote, yt_set_volume,
    yt_volume, yt_vote,
)
from eventsub import run_eventsub
from overlay_server import start_overlay_server


# ---------- Картинки от VIP/модов на оверлее ----------

MAX_IMAGE_BYTES = 3 * 1024 * 1024  # 3 МБ — потолок веса картинки/видео для вывода на экран
_image_pool = ThreadPoolExecutor(max_workers=4)
# Лимит одновременно принятых задач (выполняются + ждут в очереди). При спаме
# ссылками лишние молча отбрасываем, чтобы очередь пула не росла неограниченно.
_MAX_IMAGE_INFLIGHT = 10
_image_slots = threading.BoundedSemaphore(_MAX_IMAGE_INFLIGHT)


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    """Не следуем редиректам: цель могла бы увести на внутренний адрес (SSRF),
    а размер/тип финального ресурса не совпал бы с проверенным."""
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect blocked", headers, fp)


_image_opener = urllib.request.build_opener(_NoRedirectHandler)


def _is_public_url(url):
    """True только если хост резолвится в публичный IP. Внутренние/приватные/
    loopback/link-local адреса режем — пускаем только внешние ссылки (анти-SSRF)."""
    try:
        host = urllib.parse.urlparse(url).hostname
        if not host:
            return False
        infos = socket.getaddrinfo(host, None)
    except Exception:
        return False
    if not infos:
        return False
    for info in infos:
        try:
            addr = ipaddress.ip_address(info[4][0])
        except ValueError:
            return False
        if not addr.is_global or addr.is_multicast:
            return False
    return True


def _validate_and_publish_image(url, user):
    """HEAD-запрос: показываем только внешние (публичные) image/* или video/*
    не тяжелее MAX_IMAGE_BYTES. Внутренние адреса, редиректы и хосты без
    Content-Length отсекаем (fail-closed). В воркер-пуле — не блокирует IRC-loop."""
    if not _is_public_url(url):
        return
    try:
        req = urllib.request.Request(
            url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
        with _image_opener.open(req, timeout=5) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            clen = r.headers.get("Content-Length")
            if not (ctype.startswith("image/") or ctype.startswith("video/")):
                return
            if clen is None or int(clen) > MAX_IMAGE_BYTES:
                return
    except Exception:
        return
    image_bus.publish({"url": url, "user": user})


def _run_image_task(url, user):
    try:
        _validate_and_publish_image(url, user)
    finally:
        _image_slots.release()


def _submit_image(url, user):
    """Кладём задачу в пул, если есть свободный слот; иначе пропускаем картинку."""
    if not _image_slots.acquire(blocking=False):
        return  # очередь переполнена
    try:
        _image_pool.submit(_run_image_task, url, user)
    except RuntimeError:
        _image_slots.release()  # пул уже закрыт — слот не занимаем


# ---------- Команды: антиспам по (login, cmd) ----------

CMD_COOLDOWN = 5.0  # секунд: антиспам на одну команду от одного юзера
_cmd_cooldowns: dict[tuple[str, str], float] = {}
_CD_CLEAN_EVERY = 256
_cd_writes = 0


def _cmd_cooldown_passed(login, cmd, now_ts):
    """True — команду можно выполнять; False — юзер ещё в кулдауне.
    Опционально подчищает старые записи, чтобы словарь не рос бесконечно."""
    global _cd_writes
    key = (login, cmd)
    if now_ts - _cmd_cooldowns.get(key, 0.0) < CMD_COOLDOWN:
        return False
    _cmd_cooldowns[key] = now_ts
    _cd_writes += 1
    if _cd_writes >= _CD_CLEAN_EVERY:
        _cd_writes = 0
        cutoff = now_ts - CMD_COOLDOWN
        for k in [k for k, ts in _cmd_cooldowns.items() if ts < cutoff]:
            _cmd_cooldowns.pop(k, None)
    return True


# Модерские команды: один общий таймер на всех (ключ — только cmd), а не per-user.
# Пока таймер горит, та же команда от любого мода молча игнорируется. Тикает только
# от мода — чтобы не-моды не могли вхолостую заблокировать команду. Словарь крошечный
# (по числу модерских команд), чистка не нужна.
MOD_COMMANDS = {"!скип", "!озвучка", "!громкость", "!заголовок"}
_mod_cmd_cooldowns: dict[str, float] = {}


def _mod_cmd_cooldown_passed(cmd, now_ts):
    """True — модерскую команду можно выполнять; False — общий таймер ещё горит."""
    if now_ts - _mod_cmd_cooldowns.get(cmd, 0.0) < CMD_COOLDOWN:
        return False
    _mod_cmd_cooldowns[cmd] = now_ts
    return True


# ---------- IRC ----------

def _parse_tags(tag_str):
    tags = {}
    for part in tag_str.split(";"):
        if "=" in part:
            k, v = part.split("=", 1)
            tags[k] = v
    return tags


# Палитра дефолтных цветов ника, которую Twitch назначает юзерам без выбранного цвета.
_DEFAULT_COLORS = [
    "#FF0000", "#0000FF", "#00FF00", "#B22222", "#FF7F50",
    "#9ACD32", "#FF4500", "#2E8B57", "#DAA520", "#D2691E",
    "#5F9EA0", "#1E90FF", "#FF69B4", "#8A2BE2", "#00FF7F",
]
_HEX_COLOR_RE = re.compile(r"#[0-9A-Fa-f]{6}")


def _default_color(login):
    # Twitch выбирает из палитры по сумме кодов первого и последнего символа ника.
    if not login:
        return _DEFAULT_COLORS[0]
    n = ord(login[0]) + ord(login[-1])
    return _DEFAULT_COLORS[n % len(_DEFAULT_COLORS)]


SILENCE_LIMIT = 5 * 60   # сек без единого байта → коннект мёртв (Twitch PING-ит раз в ~5 мин)
RECV_TIMEOUT  = 1.0      # для отзывчивости на Ctrl+C
PING_EVERY    = 60       # сами шлём PING раз в минуту — против NAT-тайм-аута

# Лимиты отправки в чат (Twitch): 20 за 30с обычному юзеру, 100 за 30с моду/VIP/стримеру.
RATE_LIMIT_WINDOW = 30
RATE_LIMIT_USER   = 20
RATE_LIMIT_MOD    = 100


class RateLimiter:
    def __init__(self):
        self.times = collections.deque()
        self.limit = RATE_LIMIT_USER
        self.lock = threading.Lock()

    def set_privileged(self, privileged):
        with self.lock:
            self.limit = RATE_LIMIT_MOD if privileged else RATE_LIMIT_USER

    def acquire(self):
        """Блокирует поток, пока в окне есть свободный слот. Возвращает 0 или сколько ждали."""
        waited = 0.0
        while True:
            with self.lock:
                now = time.monotonic()
                while self.times and now - self.times[0] >= RATE_LIMIT_WINDOW:
                    self.times.popleft()
                if len(self.times) < self.limit:
                    self.times.append(now)
                    return waited
                sleep_for = RATE_LIMIT_WINDOW - (now - self.times[0]) + 0.05
            time.sleep(sleep_for)
            waited += sleep_for


def _enable_aggressive_keepalive(sock):
    """TCP keepalive: первый зонд через 60с простоя, дальше каждые 20с, 3 потери = разрыв."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if sys.platform == "win32":
        sock.ioctl(socket.SIO_KEEPALIVE_VALS, (1, 60_000, 20_000))
    else:
        for name, val in [("TCP_KEEPIDLE", 60), ("TCP_KEEPINTVL", 20), ("TCP_KEEPCNT", 3)]:
            opt = getattr(socket, name, None)
            if opt is not None:
                sock.setsockopt(socket.IPPROTO_TCP, opt, val)


def _connect(token, nick):
    raw = socket.create_connection(("irc.chat.twitch.tv", 6697), timeout=10)
    _enable_aggressive_keepalive(raw)
    ctx = ssl.create_default_context()
    s = ctx.wrap_socket(raw, server_hostname="irc.chat.twitch.tv")
    s.settimeout(RECV_TIMEOUT)
    s.sendall(
        f"PASS oauth:{token}\r\nNICK {nick}\r\n"
        f"CAP REQ :twitch.tv/tags twitch.tv/commands\r\n"
        f"JOIN {CHANNEL}\r\n".encode()
    )
    return s


_LINE_RE      = re.compile(r"^(?:@(?P<tags>\S+) )?:(?P<user>\w+)!\S+ PRIVMSG \S+ :(?P<text>.*)$")
_USERSTATE_RE = re.compile(r"^(?:@(?P<tags>\S+) )?:tmi\.twitch\.tv USERSTATE \S+")
_CLEARMSG_RE  = re.compile(r"^@(?P<tags>\S+) :tmi\.twitch\.tv CLEARMSG \S+(?: :.*)?$")
_CLEARCHAT_RE = re.compile(r"^(?:@(?P<tags>\S+) )?:tmi\.twitch\.tv CLEARCHAT \S+(?: :(?P<login>\S+))?$")


def run_chat(token, nick, broadcaster_id=None, user_id=None):
    titler.start(token, broadcaster_id)
    sock_holder = {"s": None}
    limiter = RateLimiter()
    prompt = Prompt()
    log.attach(prompt)
    SEEN_LOGINS_MAX = 10_000
    seen_logins: "collections.OrderedDict[str, bool]" = collections.OrderedDict()
    # safe_send зовут из IRC-цикла, executor'а YouTube, таймера голосования и Prompt;
    # PING/PONG пишет IRC-цикл. sendall поверх TLS-сокета не потокобезопасен — сериализуем.
    send_lock = threading.Lock()

    def _raw_send(data):
        s = sock_holder["s"]
        if s is None:
            return None, "no socket"
        with send_lock:
            try:
                s.sendall(data)
                return s, None
            except OSError as e:
                return s, str(e)

    def safe_send(msg):
        if sock_holder["s"] is None:
            prompt.print("(нет коннекта, сообщение не отправлено)")
            return
        # Защита от IRC command injection: \r\n в msg от LLM/доната/титлера/кинга
        # иначе сформируют вторую команду внутри PRIVMSG.
        msg = msg.replace("\r", " ").replace("\n", " ")
        waited = limiter.acquire()
        if waited > 0:
            prompt.print(f"(rate-limit: ждал {waited:.1f}с)")
        _, err = _raw_send(f"PRIVMSG {CHANNEL} :{msg}\r\n".encode("utf-8"))
        if err:
            prompt.print(f"(не отправилось: {err})")

    def announce(text, color="primary"):
        """Helix-объявление в чат (выделенный блок). На любой сбой/отсутствие id — обычный PRIVMSG."""
        if not broadcaster_id or not user_id:
            safe_send(text)
            return
        try:
            twitch_api.helix_send_announcement(token, broadcaster_id, user_id, text, color=color)
        except Exception as e:
            prompt.print(f"(announce упал: {type(e).__name__}: {e}) — пишу обычным сообщением")
            safe_send(text)

    youtube.set_chat_send(safe_send)
    # Стример сам себе модератор; для отдельного бота сюда нужно передать его user_id.
    # send=safe_send — чтобы при удалении бот мог написать в чат "@user, причина".
    moderation.setup(token, broadcaster_id, user_id, prompt, send=safe_send)
    king.start(announce=lambda text: announce(text, color="purple"), log=prompt.print)
    goal.start(announce=lambda text: announce(text, color="green"), log=prompt.print)
    prompt.start(safe_send)

    backoff = 1
    try:
        while True:
            try:
                s = _connect(token, nick)
            except OSError as e:
                prompt.print(f"Не удалось подключиться ({e}). Повтор через {backoff}с.")
                sock_holder["s"] = None
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue
            sock_holder["s"] = s
            prompt.print(f"Подключён к {CHANNEL} как {nick}. Пиши в терминал — уйдёт в чат. (Ctrl+C — выход)")
            backoff = 1

            buf = ""
            last_recv = time.monotonic()
            last_ping_sent = time.monotonic()
            disconnect_reason = None

            while True:
                if time.monotonic() - last_ping_sent > PING_EVERY:
                    _, err = _raw_send(b"PING :keepalive\r\n")
                    if err:
                        disconnect_reason = f"send failed: {err}"
                        break
                    last_ping_sent = time.monotonic()

                if time.monotonic() - last_recv > SILENCE_LIMIT:
                    disconnect_reason = f"тишина {SILENCE_LIMIT}с"
                    break

                try:
                    chunk = s.recv(4096).decode("utf-8", errors="ignore")
                except socket.timeout:
                    continue
                except OSError as e:
                    disconnect_reason = f"recv failed: {e}"
                    break

                if not chunk:
                    disconnect_reason = "соединение закрыто сервером"
                    break

                last_recv = time.monotonic()
                buf += chunk
                while "\r\n" in buf:
                    line, buf = buf.split("\r\n", 1)
                    if line.startswith("PING"):
                        _, err = _raw_send(f"PONG {line[5:]}\r\n".encode())
                        if err:
                            disconnect_reason = f"send failed: {err}"
                            break
                        continue
                    if line.startswith("PONG"):
                        continue
                    parts = line.split()
                    if parts and (parts[0] == "RECONNECT" or (len(parts) > 1 and parts[1] == "RECONNECT")):
                        disconnect_reason = "сервер прислал RECONNECT"
                        break
                    if "Login authentication failed" in line:
                        prompt.print("Токен невалиден. Удали token.json и запусти снова.")
                        try: s.close()
                        except OSError: pass
                        return
                    m = _LINE_RE.match(line)
                    if m:
                        tags = _parse_tags(m.group("tags")) if m.group("tags") else {}
                        login = m.group("user")
                        user = tags.get("display-name") or login
                        text = m.group("text")
                        color = tags.get("color", "")
                        stripped = text.strip()
                        # Команда — только из канонического списка commands.BOT_COMMANDS.
                        # «!что-нибудь» нераспознанное — обычное сообщение: идёт в оверлей и
                        # под модерацию, а не выбрасывается молча.
                        first_token = stripped.split(None, 1)[0].lower() if stripped else ""
                        is_command = first_token in BOT_COMMANDS
                        llogin = login.lower()
                        first_in_session = llogin not in seen_logins
                        if first_in_session:
                            seen_logins[llogin] = True
                            if len(seen_logins) > SEEN_LOGINS_MAX:
                                seen_logins.popitem(last=False)
                        else:
                            seen_logins.move_to_end(llogin)
                        if not is_command:
                            role = role_from_badges(tags)
                            # Ссылки на картинки/webm от VIP/мода/стримера уходят на оверлей
                            # картинок. Само сообщение с такой ссылкой в чат-оверлей НЕ выводим
                            # (и в дождь эмоутов тоже) — картинка показывается отдельно.
                            img_urls = (moderation.find_image_urls(text)
                                        if role in ("broadcaster", "mod", "vip") else [])
                            if not img_urls:
                                chat_bus.publish({
                                    "type": "msg",
                                    "id": tags.get("id", ""),
                                    "login": llogin,
                                    "user": user,
                                    "color": color if _HEX_COLOR_RE.fullmatch(color or "") else _default_color(llogin),
                                    "badges": resolve_badges(tags),
                                    "html": render_with_emotes(text, tags.get("emotes", "")),
                                    "first": first_in_session,
                                    "role": role,
                                    "king": king.is_king(llogin),
                                })
                            # Картинки/webm на оверлей. Вес проверяем HEAD-запросом в воркере
                            # (не блокируем IRC-loop); очередь пула ограничена _MAX_IMAGE_INFLIGHT.
                            for img_url in img_urls:
                                _submit_image(img_url, user)
                            # TTS: режим "king" — озвучиваем только короля доната;
                            # "all" — каждое сообщение в чате (переключается !озвучка).
                            # Озвучиваем только после прохождения модерации — поэтому
                            # завёрнуто в колбэк on_pass (зовётся из воркера модерации,
                            # если сообщение не удалено). text/llogin биндим дефолтными
                            # аргументами: колбэк сработает позже, когда переменные цикла
                            # уже укажут на другое сообщение.
                            def _voice_if_passed(text=text, llogin=llogin):
                                # Ссылки вслух не читаются: tts._clean сам вырезает URL,
                                # а сообщение-только-ссылка после очистки пустое и не озвучивается.
                                if tts.get_chat_mode() == "all":
                                    tts.enqueue(text, source="chat-all")
                                elif king.is_king(llogin):
                                    tts.enqueue(text, source="king-message")
                            # Модерация — асинхронно, не блокирует IRC-loop. Если LLM решит
                            # «удалять», оверлей увидит CLEARMSG как обычное действие модератора.
                            moderation.moderate(tags, llogin, text, on_pass=_voice_if_passed)
                            # +1 монета с кулдауном; функция сама фильтрует частые сообщения.
                            economy.award_chat(tags.get("user-id", ""), llogin, user)
                        if is_command:
                            cparts = stripped.split()
                            cmd = cparts[0].lower()
                            args = cparts[1:]
                            role = role_from_badges(tags)
                            is_mod = role in ("broadcaster", "mod")
                            now_ts = time.monotonic()
                            # Антиспам. Модерские команды — один общий таймер на всех
                            # (ключ = cmd, тикает только от мода): пока он горит, та же
                            # команда от любого мода игнорируется. Остальные команды —
                            # персональный таймер (login, cmd).
                            if cmd in MOD_COMMANDS:
                                if is_mod and not _mod_cmd_cooldown_passed(cmd, now_ts):
                                    continue
                            elif not _cmd_cooldown_passed(llogin, cmd, now_ts):
                                continue
                            if cmd == "!ютуб":
                                if not args:
                                    safe_send(
                                        f"@{user} кинь после команды ссылку на YouTube-клип, "
                                        f"например: !ютуб https://youtu.be/dQw4w9WgXcQ — "
                                        f"принимаются ролики 1–8 минут с 10000+ просмотров, "
                                        f"без Shorts и трансляций."
                                    )
                                else:
                                    # Сетевой поход в YouTube — в фоновый пул, чтобы IRC-цикл
                                    # не вставал на таймаут запроса (до 10с).
                                    youtube.submit_youtube_command(args[0], user, safe_send, prompt)
                            elif cmd == "!-":
                                r = yt_vote.cast(llogin, "skip", yt_close_voting)
                                if r["status"] == "opened":
                                    safe_send(
                                        f"@{user} запустил голосование за скип — "
                                        f"{YT_VOTE_WINDOW} секунд на !- / !+"
                                    )
                                if r["status"] in ("opened", "ok"):
                                    yt_publish_vote(r)
                            elif cmd == "!+":
                                r = yt_vote.cast(llogin, "keep", yt_close_voting)
                                if r["status"] == "ok":
                                    yt_publish_vote(r)
                            elif cmd == "!скип":
                                if is_mod and yt_vote.is_playing():
                                    prompt.print("(youtube) скип стримером")
                                    yt_advance()
                            elif cmd == "!кубик":
                                # Вопрос — всё после команды, как написал юзер
                                # (без re-tokenize: пробелы внутри сохраняем).
                                question = stripped[len(cparts[0]):].strip()
                                dice.submit(user, question, safe_send, dice_bus, prompt=prompt)
                            elif cmd == "!монеты":
                                # Без аргументов — свой баланс. С @ником (только мод/стример) — чужой.
                                if args and is_mod:
                                    target = args[0].lstrip("@").lower()
                                    bal = economy.balance_by_login(target)
                                    if bal is None:
                                        safe_send(f"@{user} у @{target} пока 0 монет")
                                    else:
                                        safe_send(f"@{user} у @{target}: {bal} монет")
                                else:
                                    bal = economy.balance(tags.get("user-id", ""))
                                    safe_send(f"@{user} у тебя {bal} монет")
                            elif cmd == "!топ":
                                rows = economy.top(5)
                                if not rows:
                                    safe_send(f"@{user} пока пусто, копите")
                                else:
                                    parts_msg = [f"{i + 1}. {name} — {bal}"
                                                 for i, (name, bal) in enumerate(rows)]
                                    safe_send("Топ: " + " | ".join(parts_msg))
                            elif cmd == "!дать":
                                if len(args) < 2:
                                    safe_send(f"@{user} формат: !дать @ник <количество>")
                                else:
                                    try:
                                        amount = int(args[1])
                                    except ValueError:
                                        safe_send(f"@{user} количество должно быть числом")
                                        continue
                                    ok, msg = economy.transfer(
                                        tags.get("user-id", ""), args[0], amount
                                    )
                                    if msg:
                                        safe_send(f"@{user} {msg}")
                            elif cmd == "!озвучка":
                                if not is_mod:
                                    continue
                                new_mode = tts.toggle_chat_mode()
                                if new_mode == "all":
                                    safe_send(f"@{user} озвучка чата: ВСЕ сообщения")
                                else:
                                    safe_send(f"@{user} озвучка чата: только король доната")
                            elif cmd == "!громкость":
                                if not is_mod:
                                    continue
                                if not args:
                                    safe_send(
                                        f"@{user} текущая громкость YouTube: "
                                        f"{yt_volume()}/100 — задай число: !громкость 50"
                                    )
                                    continue
                                try:
                                    n = int(args[0])
                                except ValueError:
                                    safe_send(f"@{user} нужно число 0..100")
                                    continue
                                applied = yt_set_volume(n)
                                safe_send(f"@{user} громкость YouTube: {applied}/100")
                            elif cmd == "!заголовок":
                                if not is_mod:
                                    continue
                                if not broadcaster_id:
                                    safe_send(f"@{user} broadcaster_id неизвестен")
                                    continue
                                titler.submit_manual(token, broadcaster_id, user, safe_send)
                        continue

                    um = _USERSTATE_RE.match(line)
                    if um and um.group("tags"):
                        us_tags = _parse_tags(um.group("tags"))
                        b = us_tags.get("badges", "")
                        privileged = any(r in b for r in (
                            "broadcaster/", "moderator/", "lead_moderator/", "vip/"
                        ))
                        limiter.set_privileged(privileged)
                        continue

                    if " NOTICE " in line and "msg_ratelimit" in line:
                        prompt.print("(!) Twitch: упёрлись в rate-limit, отправка приостановлена ~30 мин")
                        continue

                    cm = _CLEARMSG_RE.match(line)
                    if cm:
                        cm_tags = _parse_tags(cm.group("tags"))
                        msg_id = cm_tags.get("target-msg-id", "")
                        if msg_id:
                            chat_bus.publish({"type": "clearmsg", "id": msg_id})
                        continue

                    cc = _CLEARCHAT_RE.match(line)
                    if cc:
                        login_targeted = (cc.group("login") or "").lower()
                        chat_bus.publish({
                            "type": "clearchat",
                            "login": login_targeted,
                        })
                        continue
                if disconnect_reason:
                    break

            prompt.print(f"Реконнект ({disconnect_reason})...")
            sock_holder["s"] = None
            try: s.close()
            except OSError: pass
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)
    except KeyboardInterrupt:
        print("\nОстановлен.")


if __name__ == "__main__":
    if CLIENT_ID == "PUT_YOUR_CLIENT_ID_HERE":
        raise SystemExit("Сначала пропиши CLIENT_ID (в коде или в env TWITCH_CLIENT_ID).")
    if CHANNEL == "#put_channel_here":
        raise SystemExit("Сначала пропиши CHANNEL (в коде или в env TWITCH_CHANNEL).")
    token, nick, user_id = load_credentials()
    broadcaster_id = load_badges(token, CHANNEL.lstrip("#"))
    start_overlay_server()
    economy.load()
    tts.start(log=print)

    base = f"http://localhost:{OVERLAY_PORT}"
    tok = f"&token={OVERLAY_TOKEN}" if OVERLAY_TOKEN else ""
    print(f"Индекс оверлеев: {base}/")
    print(f"Чат-оверлей:     {base}/chat.html    (источник: /stream)")
    print(f"Оверлей алертов: {base}/alerts.html  (источник: /events)")
    print(f"Веб-камера:      {base}/webcam.html")
    if OVERLAY_TOKEN:
        print(f"OVERLAY_TOKEN={OVERLAY_TOKEN}  (сохранён в overlay_token.txt — переживёт перезапуск)")
    print(f"Тест фолловера: {base}/test/follow?user=TestUser{tok}")
    print(f"Тест подписки:  {base}/test/sub?user=TestUser&tier=1000{tok}")
    print(f"Тест ресаба:    {base}/test/resub?user=TestUser&months=6{tok}")
    print(f"Тест гифт-пака: {base}/test/subgift?user=TestUser&total=5{tok}")
    print(f"Тест рейда:     {base}/test/raid?user=TestUser&viewers=42{tok}")
    print(f"Donatty-оверлей: {base}/donatty.html  (источник: /donatty)")
    print(f"Тест доната:    {base}/test/donation?user=TestUser&amount=500&message=привет{tok}")
    print(f"Сбор-оверлей:    {base}/goal.html     (источник: /goal)")
    print(f"Сброс сбора:    {base}/test/goal_reset?_=1{tok}")
    print(f"Кубик-оверлей:   {base}/dice.html     (источник: /dice)")
    print(f"YouTube-оверлей: {base}/youtube.html  (источник: /media)")
    print(f"Тест play:      {base}/test/yt/play?v=dQw4w9WgXcQ{tok}")
    print(f"Тест stop:      {base}/test/yt/stop?_=1{tok}")
    print(f"Оверлей картинок:{base}/images.html  (источник: /images)")
    print(f"Тест картинки:  {base}/test/image?url=https://i.imgur.com/REAL.png{tok}")

    # EventSub в отдельном потоке.
    threading.Thread(target=run_eventsub, args=(token, broadcaster_id, user_id), daemon=True).start()
    # Donatty в отдельном потоке — только если задана пара REF+TOKEN.
    if DONATTY_REF and DONATTY_TOKEN:
        threading.Thread(target=donatty.run, args=(DONATTY_REF, DONATTY_TOKEN, print),
                         daemon=True, name="donatty").start()
    else:
        print("(donatty) DONATTY_REF/DONATTY_TOKEN не заданы — интеграция выключена")
    # Watchtime-тикер: периодически тянет /chat/chatters и раздаёт монеты.
    threading.Thread(target=economy.run_watchtime_ticker,
                     args=(token, broadcaster_id, user_id), daemon=True,
                     name="watchtime").start()

    run_chat(token, nick, broadcaster_id=broadcaster_id, user_id=user_id)
