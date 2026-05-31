"""Экономика монет: in-memory стор + персист в state/coins.json.

Идентификатор зрителя — Twitch user_id (стабилен при смене ника). Логин/дисплей
храним для отображения и для команд вида '!дать @ник'.

Дефолтные ставки в RATES; если есть state/economy.json — мержим поверх с
hot-reload по mtime (проверяется на каждом тике watchtime).
"""

import json
import os
import threading
import time

import log
from config import STATE_DIR


# ---------- Конфиг ставок ----------

RATES = {
    # watchtime: каждые WATCHTIME_TICK_SEC раздаём WATCHTIME_BASE всем в /chatters,
    # а если юзер писал в чат за последние WATCHTIME_ACTIVE_SEC — ещё WATCHTIME_ACTIVE.
    "watchtime_tick_sec":   300,
    "watchtime_base":       10,
    "watchtime_active":     10,
    "watchtime_active_sec": 300,

    # +1 за каждое не-командное сообщение, не чаще раза в чат_cooldown_sec.
    "chat_message":         1,
    "chat_cooldown_sec":    60,

    # Минимальный возраст аккаунта в днях — пока не используется, задел на анти-фрод.
    "min_account_age_days": 0,

    # Алерты.
    "follow":               200,
    "sub":                  1000,
    "resub":                500,
    "subgift_per_recipient": 500,  # умножается на total в payload
    "raid":                 500,
}

_RATES_FILE = os.path.join(STATE_DIR, "economy.json")
_rates_mtime = 0.0


def _maybe_reload_rates():
    """Перечитать ставки из state/economy.json, если файл изменился. Тихо игнорим ошибки."""
    global _rates_mtime
    try:
        st = os.stat(_RATES_FILE)
    except OSError:
        return
    if st.st_mtime == _rates_mtime:
        return
    try:
        with open(_RATES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.log(f"(economy) не удалось прочитать economy.json: {e}")
        return
    if not isinstance(data, dict):
        return
    for k, v in data.items():
        if k in RATES and isinstance(v, (int, float)):
            RATES[k] = int(v) if isinstance(RATES[k], int) else float(v)
    _rates_mtime = st.st_mtime
    log.log(f"(economy) ставки обновлены из {_RATES_FILE}")


# ---------- Стор ----------

_COINS_FILE = os.path.join(STATE_DIR, "coins.json")

# user_id -> {"login", "display", "balance", "earned", "last_msg_ts", "last_seen_ts"}
_users: dict[str, dict] = {}
# user_id'ы, уже получившие follow-бонус (защита от анфолл/рефолл фарма)
_follow_bonus_received: set[str] = set()
_lock = threading.Lock()
_dirty = False


def _entry(uid):
    """Внутренняя: получить/создать запись. Лочить снаружи."""
    e = _users.get(uid)
    if e is None:
        e = {"login": "", "display": "", "balance": 0, "earned": 0,
             "last_msg_ts": 0.0, "last_seen_ts": 0.0}
        _users[uid] = e
    return e


def load():
    """Прочитать state/coins.json в память. Зовётся один раз на старте."""
    global _dirty
    _maybe_reload_rates()
    if not os.path.exists(_COINS_FILE):
        _dirty = False
        return
    try:
        with open(_COINS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        log.log(f"(economy) не удалось прочитать coins.json: {e} — старт с нуля")
        return
    with _lock:
        for uid, rec in (data.get("users") or {}).items():
            if not isinstance(rec, dict):
                continue
            _users[uid] = {
                "login":        str(rec.get("login", "")).lower(),
                "display":      str(rec.get("display", "")),
                "balance":      int(rec.get("balance", 0)),
                "earned":       int(rec.get("earned", 0)),
                "last_msg_ts":  float(rec.get("last_msg_ts", 0.0)),
                "last_seen_ts": float(rec.get("last_seen_ts", 0.0)),
            }
        for uid in data.get("follow_bonus", []) or []:
            _follow_bonus_received.add(str(uid))
    _dirty = False
    log.log(f"(economy) загружено {len(_users)} аккаунтов")


def _snapshot():
    """Снимок для записи на диск. Лочить снаружи."""
    return {
        "users": _users,
        "follow_bonus": sorted(_follow_bonus_received),
    }


def save():
    """Атомарная запись на диск. Пропускает, если ничего не менялось."""
    global _dirty
    with _lock:
        if not _dirty:
            return
        snap = json.dumps(_snapshot(), ensure_ascii=False)
        _dirty = False
    tmp = _COINS_FILE + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(snap)
        os.replace(tmp, _COINS_FILE)
    except OSError as e:
        log.log(f"(economy) ошибка записи coins.json: {e}")


def _flusher():
    while True:
        time.sleep(30)
        try:
            save()
        except Exception as e:
            log.log(f"(economy) flusher: {e}")


def start_flusher():
    threading.Thread(target=_flusher, daemon=True, name="coins-flusher").start()


# ---------- Публичный API ----------

def _award_locked(uid, amount, login, display):
    """Внутренняя: начисление под уже взятым _lock. Возвращает новый баланс."""
    e = _entry(uid)
    if login is not None:
        e["login"] = login.lower()
    if display:
        e["display"] = display
    e["balance"] += amount
    e["earned"] += amount
    e["last_seen_ts"] = time.time()
    return e["balance"]


def award(user_id, amount, reason="", login=None, display=None):
    """Начислить amount монет user_id'у. login/display обновляются если переданы.
    Возвращает новый баланс или None если данных не хватило."""
    global _dirty
    if not user_id or amount <= 0:
        return None
    with _lock:
        bal = _award_locked(user_id, amount, login, display)
        _dirty = True
    if reason:
        log.log(f"(economy) +{amount} → {login or user_id} ({reason}), баланс={bal}")
    return bal


def award_chat(user_id, login, display):
    """Бонус за сообщение в чате с кулдауном. Возвращает True если начислили."""
    global _dirty
    if not user_id:
        return False
    amount = RATES["chat_message"]
    cooldown = RATES["chat_cooldown_sec"]
    if amount <= 0:
        return False
    now = time.time()
    with _lock:
        e = _entry(user_id)
        if login:
            e["login"] = login.lower()
        if display:
            e["display"] = display
        if now - e["last_msg_ts"] < cooldown:
            # Кулдаун — только обновляем тайминги, баланс не трогаем и стор не дёргаем
            # (иначе на каждом сообщении в чате 30с-флашер переписывал бы coins.json).
            e["last_msg_ts"] = now
            e["last_seen_ts"] = now
            return False
        e["balance"] += amount
        e["earned"] += amount
        e["last_msg_ts"] = now
        e["last_seen_ts"] = now
        _dirty = True
    return True


def award_follow(user_id, login=None, display=None):
    """Follow-бонус с дедупом: один раз на user_id за всё время.
    Дедуп и начисление атомарны — иначе флашер может сохранить «бонус выдан»
    без самого баланса, и при крахе юзер потеряет монеты навсегда."""
    global _dirty
    if not user_id:
        return None
    amount = RATES["follow"]
    if amount <= 0:
        return None
    with _lock:
        if user_id in _follow_bonus_received:
            return None
        bal = _award_locked(user_id, amount, login, display)
        _follow_bonus_received.add(user_id)
        _dirty = True
    log.log(f"(economy) +{amount} → {login or user_id} (follow), баланс={bal}")
    return bal


def award_watchtime(uids_to_info):
    """uids_to_info: {user_id: (login, display)} — из /chat/chatters.
    Каждому +watchtime_base, активным (писал недавно) ещё +watchtime_active."""
    global _dirty
    base = RATES["watchtime_base"]
    active_bonus = RATES["watchtime_active"]
    active_window = RATES["watchtime_active_sec"]
    if base <= 0 and active_bonus <= 0:
        return 0, 0
    now = time.time()
    given = 0
    active = 0
    with _lock:
        for uid, (login, display) in uids_to_info.items():
            if not uid:
                continue
            e = _entry(uid)
            if login:
                e["login"] = login.lower()
            if display:
                e["display"] = display
            amount = base
            if now - e["last_msg_ts"] < active_window:
                amount += active_bonus
                active += 1
            if amount > 0:
                e["balance"] += amount
                e["earned"] += amount
                e["last_seen_ts"] = now
                given += 1
        if given:
            _dirty = True
    return given, active


def balance(user_id):
    with _lock:
        e = _users.get(user_id)
        return e["balance"] if e else 0


def balance_by_login(login):
    """Поиск по нику (лог-н ниже регистром)."""
    login = (login or "").lstrip("@").lower()
    if not login:
        return None
    with _lock:
        for e in _users.values():
            if e["login"] == login:
                return e["balance"]
    return None


def transfer(from_uid, to_login, amount):
    """Перевод по нику получателя. Возвращает (ok: bool, msg: str)."""
    global _dirty
    if amount <= 0:
        return False, "сумма должна быть больше 0"
    to_login = (to_login or "").lstrip("@").lower()
    if not to_login:
        return False, "укажи получателя: !дать @ник N"
    with _lock:
        src = _users.get(from_uid)
        if not src or src["balance"] < amount:
            return False, f"недостаточно монет (у тебя {src['balance'] if src else 0})"
        if src["login"] == to_login:
            return False, "нельзя переводить самому себе"
        dst = None
        for e in _users.values():
            if e["login"] == to_login:
                dst = e
                break
        if dst is None:
            return False, f"@{to_login} ещё ни разу не появлялся, ему пока некуда переводить"
        src["balance"] -= amount
        dst["balance"] += amount
        dst["earned"] += amount  # для зрителя это «доход», для лидерборда логично учесть
        _dirty = True
        dst_disp = dst["display"] or dst["login"]
    return True, f"перевёл {amount} → @{dst_disp}"


def run_watchtime_ticker(token, broadcaster_id, moderator_id):
    """Фоновой цикл: раз в watchtime_tick_sec тянет /chat/chatters и начисляет всем.

    Зовётся в отдельном потоке из bot.py. При ошибках сети молча ждёт следующий тик."""
    import twitch_api
    if not broadcaster_id or not moderator_id:
        log.log("(economy) watchtime отключён: нет broadcaster_id/moderator_id")
        return
    log.log("(economy) watchtime-тикер запущен")
    while True:
        _maybe_reload_rates()
        try:
            chatters = twitch_api.get_chatters(token, broadcaster_id, moderator_id)
        except Exception as e:
            log.log(f"(economy) /chatters недоступен: {e}")
            chatters = None
        if chatters:
            given, active = award_watchtime(chatters)
            log.log(f"(economy) watchtime: {given} зрителей получили монеты ({active} активных)")
        tick = RATES["watchtime_tick_sec"]
        time.sleep(max(30, tick))


def top(n=5):
    """Топ-N по балансу. Возвращает список (display_or_login, balance)."""
    with _lock:
        rows = [(e["display"] or e["login"], e["balance"])
                for e in _users.values() if e["balance"] > 0]
    rows.sort(key=lambda x: -x[1])
    return rows[:n]
