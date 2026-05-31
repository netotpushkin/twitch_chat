"""Модерация чата: детерминированные фильтры + LLM с одним порогом для всех.

Точка входа — moderate(tags, login, text), её зовёт IRC-loop в bot.py. Сама проверка
выполняется в фоновом пуле потоков, чтобы не тормозить приём сообщений.

Пайплайн:
  1. Привилегии (broadcaster/mod/vip) → пропуск.
  2. Известная бот-команда (!ютуб и т.п.) → пропуск.
  3. ASCII-арт / символьный спам → DELETE без LLM.
  4. URL не на YouTube → DELETE без LLM.
  5. Тривиально-короткое сообщение → пропуск.
  6. LLM-вердикт: удаляем только жёсткие нарушения, всё лёгкое пропускаем.

Любые исключения внутри пайплайна логируются и трактуются как «не удалять» (fail-open):
ложное удаление бесит юзеров сильнее, чем пропущенный спам.
"""

import concurrent.futures
import re
import threading

import log
from config import (
    MODERATION_ENABLED, MODERATION_DRY_RUN,
    OPENROUTER_API_KEY,
)
from openrouter import ask as llm_ask, OpenRouterError, ContentFilteredError
from twitch_api import helix_delete_message, role_from_badges


# Имена команд, которые бот реально обрабатывает в bot.py — пропускаются без модерации.
# Любой другой текст с ведущим "!" модерируется как обычное сообщение.
KNOWN_COMMANDS = {"!ютуб", "!-", "!+", "!скип"}


# ---------- Состояние, прокидываемое из bot.py при старте ----------

_state = {
    "token": None,
    "broadcaster_id": None,
    "moderator_id": None,   # для стримера = его user_id
    "prompt": None,         # Prompt из bot.py — чтобы логировать поверх строки ввода
    "send": None,           # safe_send из bot.py — публикация в чат
}
_state_lock = threading.Lock()
# Воркер-пул: IRC-loop сабмитит задачу и сразу возвращается. 4 потока хватает с запасом —
# при типичных rps в чате LLM-вызов в 1-3с не успевает забить очередь.
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=4, thread_name_prefix="mod")


def setup(token, broadcaster_id, moderator_id, prompt, send=None):
    """Зовётся один раз из bot.py после успешной авторизации.
    send — функция отправки сообщения в чат (safe_send из bot.py); если None,
    бот будет только удалять, без объяснения в чат."""
    with _state_lock:
        _state["token"] = token
        _state["broadcaster_id"] = broadcaster_id
        _state["moderator_id"] = moderator_id
        _state["prompt"] = prompt
        _state["send"] = send


def _log(msg):
    p = _state.get("prompt")
    if p is not None:
        p.print(msg)
    else:
        log.log(msg)


# ---------- Фильтр ASCII-арта / символьного спама ----------

# Юникод-диапазоны, которыми обычно рисуют арт в чатах.
_ART_RANGES = (
    (0x2500, 0x257F),  # Box Drawing
    (0x2580, 0x259F),  # Block Elements
    (0x25A0, 0x25FF),  # Geometric Shapes
    (0x2800, 0x28FF),  # Braille (любимое у пастер-ботов)
)
# Один и тот же не-пробельный не-словесный символ повторяется ≥8 раз подряд:
# ░░░░░░░░, ▄▄▄▄▄▄▄▄, !!!!!!!! и т.п.
_LONG_RUN_RE = re.compile(r"([^\w\s])\1{7,}", re.UNICODE)


def _is_art(text):
    """True — это похоже на арт/спам-набивку. Срабатывает только на длинных сообщениях,
    чтобы не цеплять короткие `:)` или `<3<3<3`."""
    if len(text) < 20:
        return False
    if _LONG_RUN_RE.search(text):
        return True
    art = 0
    for c in text:
        o = ord(c)
        for lo, hi in _ART_RANGES:
            if lo <= o <= hi:
                art += 1
                break
    return art / len(text) > 0.5


# ---------- URL-фильтр ----------

# Хосты, которые разрешены.
_YT_HOSTS = {
    "youtube.com", "www.youtube.com", "m.youtube.com",
    "music.youtube.com", "youtu.be", "www.youtu.be",
    "youtube-nocookie.com", "www.youtube-nocookie.com",
}

# Невидимые юникод-символы, которыми пытаются ломать регексы (ZWSP, ZWNJ, ZWJ, WJ, BOM).
_ZERO_WIDTH_RE = re.compile("[​‌‍⁠﻿]")
# Простые приёмы обфускации точки: " . ", "[.]", "(.)" → "."
_DOT_OBFUSCATE_RE = re.compile(r"\s*[\[\(]\s*\.\s*[\]\)]\s*|\s+\.\s+|\s+\.\s*(?=[a-zA-Zа-яА-Я])")

# Захватываем URL двумя ветками:
#   а) явная схема http:// / https:// / www.
#   б) host-вида X.Y[.Z…].TLD из ограниченного белого списка TLD, с обязательным
#      слэш-путём — иначе "node.js", "vue.js", "app.py" и т.п. ложно срабатывают.
_TLD_RE = (
    r"com|net|org|io|co|ru|ua|by|kz|tv|gg|tk|ml|ga|cf|xyz|info|biz|me|cc|de|"
    r"uk|us|app|dev|site|online|store|shop|club|live|stream|link|page|pro|fm"
)
_URL_RE = re.compile(
    r"(?i)(?:https?://|www\.)\S+"
    r"|(?<![\w@])[\w-]+(?:\.[\w-]+)*\.(?:" + _TLD_RE + r")\b(?:/\S*)?",
)


def _normalize(text):
    text = _ZERO_WIDTH_RE.sub("", text)
    text = _DOT_OBFUSCATE_RE.sub(".", text)
    return text


def _extract_host(raw):
    h = raw.lower()
    for prefix in ("https://", "http://"):
        if h.startswith(prefix):
            h = h[len(prefix):]
            break
    h = h.split("/", 1)[0].split("?", 1)[0].split("#", 1)[0]
    return h.rstrip(".,;:!?)\"'")


def _find_bad_url(text):
    """Возвращает host первой не-YT ссылки или None если ссылок нет / все YouTube."""
    norm = _normalize(text)
    for m in _URL_RE.finditer(norm):
        host = _extract_host(m.group(0))
        if not host or "." not in host:
            continue
        if host in _YT_HOSTS:
            continue
        return host
    return None


# ---------- LLM-вердикт ----------

_SYSTEM_PROMPT = """Ты модератор Twitch-чата. На вход — одно сообщение.
Удаляй ТОЛЬКО жёсткие нарушения, всё остальное пропускай. Сомневаешься — пропускай.

УДАЛЯЕМ (отвечай "DELETE:<краткая причина>", без цитат):
(1) обсценная лексика + унижающий ярлык в адрес конкретного человека:
    "хуй сосёшь тварь", "сдохни сука ебучая", "пиздабол ебанутый".
(2) реальные угрозы физическим насилием с конкретикой (место/время/способ):
    "адрес твой пробью и приду", "выйдешь — изуродую", "отрежу тебе руки".
    Маркёры шуток ("лол", "кек", "xd", "😂", "ха-ха", "ору", "kekw") = НЕ угроза.
    Гипербола без маркёра шуткой не считается.
(3) разжигание ненависти и обобщения о группах по нации/расе/религии/полу/ориентации,
    включая слуры: "русские все алкаши", "евреи правят миром", "хохлы выродки".
(4) подробные призывы к суициду со способом: "под поезд кидайся", "купи верёвку и в лес".

OK — всё остальное:
- лёгкие оскорбления без мата: "ну ты придурок", "дурачок", "клоун ты";
- мат на ситуацию/игру/стримера без ярлыка на личность: "блять как же бесит",
  "охуенно сложно", "ну и хуйня";
- сексуальные намёки и комплименты: "симпатичная", "лайк за внешность";
- шуточные угрозы и сленг: "минус репа", "вылетишь из чата", "go die";
- мнения об идеях/событиях/группах без слуров и без призыва к ненависти:
  "вакцинация фигня", "крипта пузырь", "коммунизм не работает", "ислам мне не близок";
- сарказм, грубоватые шутки, Twitch-сленг.

Ответ одной строкой: "OK" или "DELETE:<причина>"."""


def _llm_verdict(text):
    """Возвращает причину нарушения (str) или None если всё ок / LLM недоступна."""
    if not OPENROUTER_API_KEY:
        return None
    try:
        reply = llm_ask(
            text, system=_SYSTEM_PROMPT,
            max_tokens=60, temperature=0.0, timeout=10,
        )
    except ContentFilteredError:
        # Провайдер сам зарезал — это сильный сигнал «опасное содержание».
        # Удаляем с нейтральной формулировкой (внутренних деталей не светим в чат).
        return "нарушение правил чата"
    except OpenRouterError as e:
        _log(f"(moderation) LLM недоступен: {e}")
        return None
    reply = (reply or "").strip()
    if not reply:
        return None
    if not reply.upper().startswith("DELETE"):
        return None
    # Формат "DELETE:причина" — берём всё после двоеточия как человекочитаемое объяснение.
    reason = reply.split(":", 1)[1].strip() if ":" in reply else ""
    if not reason:
        reason = "нарушение правил чата"
    # Подрезаем длину, чтобы не отправлять в чат стену текста, если модель разговорилась.
    if len(reason) > 100:
        reason = reason[:97].rstrip() + "..."
    return reason


# ---------- Действие ----------

def _announce(display_name, reason):
    """Отправить в чат человекочитаемое объяснение удаления.
    display_name — то, что пользователь видит в чате (с регистром).
    reason — короткая фраза без префикса, например 'оскорбление' или 'ссылка не на YouTube'."""
    send = _state.get("send")
    if send is None:
        return
    try:
        send(f"@{display_name}, {reason}")
    except Exception as e:
        _log(f"(moderation) не удалось анонсировать удаление: {e}")


def _delete(tags, msg_id, login, text, reason, announce_text=None):
    """reason — короткая внутренняя метка для логов (art / link:host / llm:<что-то>).
    announce_text — если задан, отправляется в чат как `@user, <announce_text>`."""
    snippet = text if len(text) <= 80 else text[:77] + "..."
    display_name = tags.get("display-name") or login
    if MODERATION_DRY_RUN:
        _log(f"(moderation DRY) удалил бы {login} [{reason}]: {snippet}")
        if announce_text:
            _log(f"(moderation DRY) написал бы в чат: @{display_name}, {announce_text}")
        return
    token = _state.get("token")
    bid = _state.get("broadcaster_id")
    mid = _state.get("moderator_id")
    if not (token and bid and mid):
        _log(f"(moderation) state не инициализирован — пропускаю удаление {login}")
        return
    try:
        helix_delete_message(token, bid, mid, msg_id)
        _log(f"(moderation) удалил {login} [{reason}]: {snippet}")
    except Exception as e:
        _log(f"(moderation) DELETE failed для {login}: {e}")
        return
    if announce_text:
        _announce(display_name, announce_text)


def _process(tags, login, text):
    msg_id = tags.get("id", "")
    if not msg_id:
        return  # без message-id Helix удалить не сможет

    # 1. Привилегии — никогда не модерим.
    role = role_from_badges(tags)
    if role in ("broadcaster", "mod", "vip"):
        return

    # 2. Известная бот-команда — пропускаем (там свой кулдаун и логика).
    stripped = text.strip()
    first_token = stripped.split(None, 1)[0].lower() if stripped else ""
    if first_token in KNOWN_COMMANDS:
        return

    # 3. ASCII-арт / символьный спам.
    if _is_art(text):
        _delete(tags, msg_id, login, text, "art",
                announce_text="без ASCII-арта и набивки символами, пожалуйста")
        return

    # 4. URL-фильтр: только YouTube разрешён.
    bad = _find_bad_url(text)
    if bad:
        _delete(tags, msg_id, login, text, f"link:{bad}",
                announce_text=f"ссылки разрешены только на YouTube ({bad} — нельзя)")
        return

    # 5. Слишком короткое — не имеет смысла гонять в LLM.
    if len(stripped) < 5:
        return

    # 6. LLM-вердикт. Один уровень правил для всех — удаляем только жёсткие нарушения.
    reason = _llm_verdict(text)
    if reason:
        # reason — то, что LLM написал после "DELETE:". Это и есть человекочитаемая причина.
        _delete(tags, msg_id, login, text, f"llm:{reason}", announce_text=reason)


def _safe_process(tags, login, text):
    try:
        _process(tags, login, text)
    except Exception as e:
        _log(f"(moderation) внутренняя ошибка: {e}")


def moderate(tags, login, text):
    """Точка входа из IRC-loop. Не блокирует — сабмитит работу в воркер-пул."""
    if not MODERATION_ENABLED:
        return
    if _state.get("token") is None:
        return  # ещё не вызвали setup()
    try:
        _executor.submit(_safe_process, tags, login, text)
    except RuntimeError:
        # executor могли закрыть при выходе — игнорируем.
        pass
