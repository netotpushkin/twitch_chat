"""YouTube: парсинг ID, метаданные с кэшем, голосование за скип, очередь клипов."""

import collections
import concurrent.futures
import json
import re
import threading
import time
import urllib.parse

import http_pool
import log
from events import media_bus


_YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")

YT_VOTE_WINDOW = 30      # секунд: окно голосования с момента первого !-
YT_MIN_VIEWS   = 10_000  # минимум просмотров
YT_MIN_SEC     = 60      # минимум 1 минута
YT_MAX_SEC     = 480     # максимум 8 минут


def _fmt_duration(sec):
    return f"{sec // 60}:{sec % 60:02d}"


def _balanced_json_after(text, marker):
    """Из text вырезает первый сбалансированный JSON-объект после marker. None если не нашли."""
    i = text.find(marker)
    if i < 0:
        return None
    i = text.find("{", i)
    if i < 0:
        return None
    depth = 0
    in_str = False
    esc = False
    for j in range(i, len(text)):
        c = text[j]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    return text[i:j + 1]
    return None


_YT_META_TTL  = 3600.0   # 1 час: статус/длина клипа меняются редко
_YT_META_CAP  = 256
_YT_META_LOCK = threading.Lock()
_yt_meta_cache: dict[str, tuple[float, dict]] = {}

_YT_SHORT_RE = re.compile(
    r'rel="canonical"\s+href="https?://(?:www\.)?youtube\.com/shorts/'
)


def fetch_youtube_meta(video_id):
    """Возвращает dict {title, views, length, live} или None при сбое.
    Кэшируется на _YT_META_TTL секунд по video_id — повторные !ютуб на тот же
    клип не лезут в YouTube заново.

    ВНИМАНИЕ: парсинг JSON из HTML — хрупкий, ломается при ребрендингах YouTube.
    Альтернатива на будущее — официальный Data API (нужен API-ключ + квота)."""
    now = time.monotonic()
    with _YT_META_LOCK:
        cached = _yt_meta_cache.get(video_id)
        if cached and now - cached[0] < _YT_META_TTL:
            return cached[1]

    url = f"https://www.youtube.com/watch?v={video_id}&hl=en"
    try:
        _, _, body = http_pool.request("GET", url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }, timeout=10)
        html = body.decode("utf-8", errors="ignore")
    except Exception:
        return None
    raw = _balanced_json_after(html, "ytInitialPlayerResponse")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except Exception:
        return None
    vd = data.get("videoDetails") or {}
    ps = data.get("playabilityStatus") or {}
    is_short = bool(_YT_SHORT_RE.search(html))
    try:
        result = {
            "status": ps.get("status", ""),
            "reason": ps.get("reason", "") or "",
            "title":  vd.get("title", ""),
            "views":  int(vd.get("viewCount", "0") or 0),
            "length": int(vd.get("lengthSeconds", "0") or 0),
            "live":   bool(vd.get("isLiveContent") or vd.get("isLive")),
            "short":  is_short,
            # playableInEmbed=false → владелец запретил воспроизведение в embed;
            # на youtube.com открывается, а в плеере оверлея будет чёрный экран.
            # Ключа может не быть — тогда считаем встраиваемым (не режем зря).
            "embeddable": bool(ps.get("playableInEmbed", True)),
        }
    except (ValueError, TypeError):
        return None

    with _YT_META_LOCK:
        if len(_yt_meta_cache) >= _YT_META_CAP:
            for k in sorted(_yt_meta_cache, key=lambda k: _yt_meta_cache[k][0])[: _YT_META_CAP // 4]:
                _yt_meta_cache.pop(k, None)
        _yt_meta_cache[video_id] = (now, result)
    return result


_YT_STATUS_MSG = {
    "ERROR":                     "это видео удалено или не существует",
    "LOGIN_REQUIRED":            "это видео приватное или требует подтверждения возраста",
    "AGE_VERIFICATION_REQUIRED": "видео с возрастным ограничением — не пропущу",
    "UNPLAYABLE":                "видео недоступно для просмотра",
    "CONTENT_CHECK_REQUIRED":    "видео с предупреждением о контенте — не пропущу",
}


def check_youtube_clip(video_id):
    """Возвращает (ok, reason). reason — фраза для чата либо dict с метой при успехе."""
    meta = fetch_youtube_meta(video_id)
    if not meta:
        return False, "не получилось проверить ролик, YouTube не ответил"
    status = meta["status"]
    if status and status != "OK":
        return False, _YT_STATUS_MSG.get(status, "видео недоступно для просмотра")
    if not meta.get("embeddable", True):
        return False, "владелец запретил воспроизведение этого видео вне YouTube"
    if meta["live"]:
        return False, "это live-трансляция, нужен записанный клип"
    if meta["short"]:
        return False, "Shorts не пускаю, нужно обычное видео"
    if meta["length"] <= 0:
        return False, "странный ролик с нулевой длительностью"
    if meta["length"] < YT_MIN_SEC:
        return False, (
            f"коротковато ({_fmt_duration(meta['length'])}), "
            f"минимум {_fmt_duration(YT_MIN_SEC)}"
        )
    if meta["length"] > YT_MAX_SEC:
        return False, (
            f"длинновато ({_fmt_duration(meta['length'])}), "
            f"максимум {_fmt_duration(YT_MAX_SEC)}"
        )
    if meta["views"] < YT_MIN_VIEWS:
        return False, (
            f"маловато просмотров ({meta['views']:,}), "
            f"нужно от {YT_MIN_VIEWS:,}"
        ).replace(",", " ")
    return True, meta


def parse_youtube_id(s):
    """Из URL или голого ID достаёт 11-символьный videoId или None."""
    s = (s or "").strip()
    if not s:
        return None
    if _YT_ID_RE.match(s):
        return s
    try:
        u = urllib.parse.urlparse(s if "://" in s else "https://" + s)
    except ValueError:
        return None
    host = (u.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host.startswith("m."):
        host = host[2:]
    if host == "youtu.be":
        cand = u.path.lstrip("/").split("/", 1)[0]
        return cand if _YT_ID_RE.match(cand) else None
    if host.endswith("youtube.com") or host.endswith("youtube-nocookie.com"):
        q = urllib.parse.parse_qs(u.query)
        if "v" in q and _YT_ID_RE.match(q["v"][0]):
            return q["v"][0]
        parts = [p for p in u.path.split("/") if p]
        if len(parts) >= 2 and parts[0] in ("embed", "shorts", "v", "live"):
            cand = parts[1]
            if _YT_ID_RE.match(cand):
                return cand
    return None


class YoutubeVote:
    """Голосование по текущему YouTube-клипу.

    Окно голосования открывается с первого !-, длится YT_VOTE_WINDOW секунд.
    По закрытию: если за скип больше — скипаем, иначе клип остаётся, повторное
    голосование на том же клипе невозможно."""
    def __init__(self):
        self.lock = threading.Lock()
        self.video_id = None
        self.votes = {}
        self.voting_open = False
        self.voting_done = False
        self.ends_at = 0.0
        self.timer = None

    def _cancel_timer(self):
        if self.timer is not None:
            try: self.timer.cancel()
            except Exception: pass
            self.timer = None

    def is_playing(self):
        with self.lock:
            return self.video_id is not None

    def current_id(self):
        with self.lock:
            return self.video_id

    def start(self, video_id):
        with self.lock:
            self.video_id = video_id
            self.votes = {}
            self.voting_open = False
            self.voting_done = False
            self.ends_at = 0.0
            self._cancel_timer()

    def stop(self):
        with self.lock:
            self.video_id = None
            self.votes = {}
            self.voting_open = False
            self.voting_done = False
            self.ends_at = 0.0
            self._cancel_timer()

    def _counts(self):
        sk = sum(1 for v in self.votes.values() if v == "skip")
        kp = sum(1 for v in self.votes.values() if v == "keep")
        return sk, kp

    def remaining(self):
        with self.lock:
            if not self.voting_open:
                return 0
            return max(0, int(round(self.ends_at - time.monotonic())))

    def cast(self, login, choice, on_close):
        with self.lock:
            if not self.video_id:
                return {"status": "no_video", "skip": 0, "keep": 0, "remaining": 0}
            if self.voting_done:
                return {"status": "closed", "skip": 0, "keep": 0, "remaining": 0}
            opened_now = False
            if not self.voting_open:
                if choice != "skip":
                    return {"status": "no_window", "skip": 0, "keep": 0, "remaining": 0}
                self.voting_open = True
                self.ends_at = time.monotonic() + YT_VOTE_WINDOW
                self.timer = threading.Timer(YT_VOTE_WINDOW, on_close)
                self.timer.daemon = True
                self.timer.start()
                opened_now = True
            prev = self.votes.get(login)
            if prev == choice:
                sk, kp = self._counts()
                rem = max(0, int(round(self.ends_at - time.monotonic())))
                return {"status": "same", "skip": sk, "keep": kp, "remaining": rem}
            self.votes[login] = choice
            sk, kp = self._counts()
            rem = max(0, int(round(self.ends_at - time.monotonic())))
            return {
                "status": "opened" if opened_now else "ok",
                "skip": sk, "keep": kp, "remaining": rem,
            }

    def close(self):
        with self.lock:
            if not self.voting_open or self.voting_done:
                return None
            self.voting_done = True
            self.voting_open = False
            self._cancel_timer()
            sk, kp = self._counts()
            return self.video_id, sk, kp, sk > kp


yt_vote = YoutubeVote()


# ---------- Громкость YouTube-оверлея ----------

YT_VOLUME_DEFAULT = 50

_yt_volume = YT_VOLUME_DEFAULT
_yt_volume_lock = threading.Lock()


def yt_volume():
    with _yt_volume_lock:
        return _yt_volume


def yt_set_volume(value):
    """Устанавливает громкость 0..100 и шлёт событие в оверлей."""
    global _yt_volume
    value = max(0, min(100, int(value)))
    with _yt_volume_lock:
        _yt_volume = value
    media_bus.publish({"evt": "volume", "value": value})
    return value

# Пул для обработки команд с сетевыми запросами (!ютуб).
_cmd_executor = concurrent.futures.ThreadPoolExecutor(
    max_workers=4, thread_name_prefix="cmd"
)


# ---------- Очередь клипов ----------

YT_QUEUE_MAX = 10
yt_queue = collections.deque()
yt_queue_lock = threading.Lock()

# Отправка обычного сообщения в чат (PRIVMSG) — оповещение о старте клипа.
# Задаётся из bot.py.
_chat_send = None


def set_chat_send(fn):
    global _chat_send
    _chat_send = fn


def yt_queue_push(item):
    """Кладёт клип в очередь. Возвращает:
       N (>=1) — позицию,
       -1     — переполнено,
       -2     — этот клип уже в очереди."""
    with yt_queue_lock:
        if any(q["id"] == item["id"] for q in yt_queue):
            return -2
        if len(yt_queue) >= YT_QUEUE_MAX:
            return -1
        yt_queue.append(item)
        return len(yt_queue)


def yt_queue_pop():
    with yt_queue_lock:
        if not yt_queue:
            return None
        return yt_queue.popleft()


# ОСНОВНОЙ механизм перехода — детекция реального конца по позиции плеера (yt_report_pos):
# оверлей по WS-тику шлёт t/d на /ws/media, сервер ловит t≈d. Этот таймер — лишь дальняя
# страховка на случай, если позиции перестанут приходить (оверлей отвалился / WS порвался).
# Запас с лихвой покрывает стартовую буферизацию, чтобы НЕ срезать ещё играющий клип:
# при рабочей детекции реальный конец всегда раньше.
YT_WATCHDOG_GRACE = 30

_watchdog_lock = threading.Lock()
_clip_watchdog = None


def _cancel_watchdog_locked():
    """Снимает текущий таймер. Вызывать под _watchdog_lock."""
    global _clip_watchdog
    if _clip_watchdog is not None:
        try: _clip_watchdog.cancel()
        except Exception: pass
        _clip_watchdog = None


def _arm_watchdog(clip_id, length):
    """Страховочный таймер на длительность + запас (см. YT_WATCHDOG_GRACE). В норме клип
    переключает детекция реального конца (yt_report_pos), а этот таймер срабатывает, лишь
    если позиции перестали приходить. По срабатыванию делает yt_advance(clip_id) —
    идемпотентный: если клип уже сменился (реальным концом/голосованием/скипом), страховка
    ничего не сделает (guard по current_id)."""
    global _clip_watchdog
    with _watchdog_lock:
        _cancel_watchdog_locked()
        if length and length > 0:
            t = threading.Timer(length + YT_WATCHDOG_GRACE, lambda: yt_advance(clip_id))
            t.daemon = True
            _clip_watchdog = t
            t.start()


def _disarm_watchdog():
    with _watchdog_lock:
        _cancel_watchdog_locked()


def _announce_clip(item):
    if _chat_send:
        try: _chat_send(f"▶ «{item['title']}» — !- скип, !+ оставить")
        except Exception: pass


def yt_start_clip(item, announce=True):
    """Запускает клип в плеере, сбрасывает голосование, при announce=True пишет в чат.
    Лог пишется здесь, чтобы был виден ЛЮБОЙ старт — и ручной, и авто (из очереди /
    по watchdog), который иначе проходил бы молча."""
    yt_vote.start(item["id"])
    _arm_watchdog(item["id"], item.get("length"))
    _mark_pos_seen()  # отсчёт тишины health-check ведём от старта клипа
    log.log(
        f"(youtube) play {item['id']} — {item.get('title','')!r} "
        f"({_fmt_duration(item.get('length') or 0)}, "
        f"{item.get('views', 0):,} views)".replace(",", " ")
    )
    media_bus.publish({
        "evt": "play",
        "id": item["id"],
        "start": 0,
        "title": item.get("title", ""),
        "requester": item.get("requester", ""),
        "volume": yt_volume(),  # чтобы переподключившийся оверлей знал текущую громкость
    })
    if announce:
        _announce_clip(item)


# Сериализует переход к следующему клипу. Без него watchdog-таймер и закрытие
# голосования могли сработать одновременно и снять из очереди сразу два клипа.
_advance_lock = threading.Lock()


def yt_advance(expect_id=None):
    """Берёт следующий клип из очереди. Если очередь пуста — останавливает плеер.

    expect_id — id клипа, который сейчас должен играть. Если он задан и не совпадает
    с текущим (другой поток уже переключил клип), переход не выполняется. Так watchdog
    и закрытие голосования об одном и том же клипе дают максимум один переход."""
    nxt = None
    with _advance_lock:
        if expect_id is not None and yt_vote.current_id() != expect_id:
            return
        nxt = yt_queue_pop()
        if nxt:
            # Смена состояния и событие на оверлей — под локом (быстро, in-memory).
            yt_start_clip(nxt, announce=False)
        else:
            yt_vote.stop()
            _disarm_watchdog()
            media_bus.publish({"evt": "stop"})
            log.log("(youtube) очередь пуста — плеер остановлен")
    # Анонс в чат — синхронный Helix-запрос; выносим его за лок, чтобы не держать
    # _advance_lock на время сетевого вызова (иначе он бы блокировал параллельные
    # переходы и IRC-поток на команде !скип).
    if nxt:
        _announce_clip(nxt)


# Детекция реального конца по позиции, что шлёт оверлей. Состояние на текущий клип.
_pos_lock = threading.Lock()
_pos = {"vid": None, "t": -1, "stall": 0}

# Health-check обратного канала. Пока клип играет, оверлей раз в секунду шлёт позицию.
# Если позиций долго нет — канал оверлей→сервер сломан (как было из-за лимита соединений
# Chromium): клип тогда переключит лишь страховочный таймер. Пишем об этом громко в лог,
# чтобы не выяснять вслепую. _last_pos_at взводится при старте клипа (см. yt_start_clip).
YT_POS_SILENCE_WARN = 12  # секунд тишины при играющем клипе → предупреждение
_last_pos_at = 0.0
_health_warned = False


def _mark_pos_seen():
    global _last_pos_at, _health_warned
    _last_pos_at = time.monotonic()
    _health_warned = False


def yt_health_tick():
    """Зовётся раз в секунду (из heartbeat). Один раз на клип пишет предупреждение, если
    клип играет, а позиции от оверлея не приходят дольше YT_POS_SILENCE_WARN."""
    global _health_warned
    if not yt_vote.is_playing():
        return
    if _health_warned or not _last_pos_at:
        return
    silent = time.monotonic() - _last_pos_at
    if silent > YT_POS_SILENCE_WARN:
        _health_warned = True
        log.log(
            f"(youtube) ВНИМАНИЕ: {int(silent)} c нет позиций от оверлея — обратный канал "
            f"оверлей→сервер не работает. Клип переключит только страховочный таймер "
            f"(+{YT_WATCHDOG_GRACE}c). Проверь, что оверлей открыт и подключён по WS (/ws/media)."
        )


def yt_report_pos(vid, t, d):
    """Оверлей доложил позицию плеера (t/d секунды) по пульсу. Главный механизм перехода:
    видим реальный конец — доиграли (t≈d) или позиция замерла у конца — и переключаем."""
    if not vid or d <= 0 or yt_vote.current_id() != vid:
        return
    _mark_pos_seen()
    with _pos_lock:
        if _pos["vid"] != vid:
            _pos.update(vid=vid, t=-1, stall=0)
        stalled = (t > 0 and t == _pos["t"] and t >= d - 3)
        _pos["stall"] = _pos["stall"] + 1 if stalled else 0
        _pos["t"] = t
        ended = (t >= d - 1) or _pos["stall"] >= 2
    if ended:
        yt_advance(vid)  # переход виден по следующему 'play …' / 'очередь пуста …'


def yt_close_voting():
    """Колбэк таймера: подводит итог окна голосования."""
    result = yt_vote.close()
    if not result:
        return
    vid, _, _, should_skip = result
    media_bus.publish({"evt": "vote", "open": False})
    if should_skip:
        yt_advance(vid)


def yt_publish_vote(r):
    """Шлёт обновление состояния голосования на оверлей-страницу."""
    media_bus.publish({
        "evt": "vote", "open": True,
        "skip": r["skip"], "keep": r["keep"],
        "seconds_left": r["remaining"],
    })


def handle_youtube_command(raw_arg, user, safe_send, prompt):
    """Обработчик !ютуб: парс ID → проверка через YouTube → постановка в очередь / запуск.
    Запускается из _cmd_executor, чтобы не блокировать IRC-цикл сетевым запросом."""
    vid = parse_youtube_id(raw_arg)
    if not vid:
        safe_send(f"@{user}, не похоже на ссылку YouTube — проверь её")
        return
    ok, info = check_youtube_clip(vid)
    if not ok:
        safe_send(f"@{user}, {info}")
        prompt.print(f"(youtube) отказ {vid}: {info}")
        return
    item = {
        "id": vid,
        "title": info["title"],
        "length": info["length"],
        "views": info["views"],
        "requester": user,
    }
    if yt_vote.current_id() == vid:
        safe_send(f"@{user}, этот клип как раз сейчас играет")
    elif yt_vote.is_playing():
        pos = yt_queue_push(item)
        if pos == -2:
            safe_send(f"@{user}, этот клип уже в очереди")
        elif pos == -1:
            safe_send(
                f"@{user}, очередь забита "
                f"({YT_QUEUE_MAX} клипов) — дождись, пока что-то доиграет"
            )
        else:
            safe_send(
                f"@{user}, поставил в очередь: "
                f"«{info['title']}», позиция {pos}"
            )
            prompt.print(f"(youtube) queue #{pos}: {vid}")
    else:
        yt_start_clip(item)  # лог старта пишется внутри yt_start_clip


def submit_youtube_command(raw_arg, user, safe_send, prompt):
    """Отправляет обработку в фоновый пул, IRC-цикл не блокируется."""
    fut = _cmd_executor.submit(handle_youtube_command, raw_arg, user, safe_send, prompt)

    def _log_err(f):
        exc = f.exception()
        if exc is not None:
            import traceback
            tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            prompt.print(f"(youtube) исключение в обработке !ютуб:\n{tb}")

    fut.add_done_callback(_log_err)
