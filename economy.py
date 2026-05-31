"""Экономика монет: SQLite-стор в state/coins.db.

Идентификатор зрителя — Twitch user_id (стабилен при смене ника). Логин/дисплей
храним для отображения и для команд вида '!дать @ник'.

Дефолтные ставки в RATES; если есть state/economy.json — мержим поверх с
hot-reload по mtime (проверяется на каждом тике watchtime).

Один process-wide соединение в WAL-режиме под глобальным локом: пишущих потоков
немного (IRC-loop, watchtime-тикер, EventSub-листенер), нагрузка маленькая,
блокировка пренебрежима. WAL даёт durability без двойных записей.
"""

import json
import os
import sqlite3
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

_DB_FILE = os.path.join(STATE_DIR, "coins.db")
_conn: sqlite3.Connection | None = None
# RLock — на случай если кто-то в будущем вызовет одно API-метод внутри другого.
# С обычным Lock это был бы дедлок.
_lock = threading.RLock()


def _init_schema():
    # journal_mode=WAL — параллельные чтения не блокируются записью; кэш-страницы
    # пишутся в .db-wal и подмерживаются периодически. synchronous=NORMAL — fsync
    # на каждый commit пропускается, при kill -9 теряем максимум последний commit,
    # БД остаётся целостной.
    _conn.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        CREATE TABLE IF NOT EXISTS users (
            uid          TEXT PRIMARY KEY,
            login        TEXT NOT NULL DEFAULT '',
            display      TEXT NOT NULL DEFAULT '',
            balance      INTEGER NOT NULL DEFAULT 0,
            earned       INTEGER NOT NULL DEFAULT 0,
            last_msg_ts  REAL    NOT NULL DEFAULT 0,
            last_seen_ts REAL    NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_users_login ON users(login);
        CREATE TABLE IF NOT EXISTS follow_bonus (
            uid TEXT PRIMARY KEY
        );
    """)


def load():
    """Открыть state/coins.db, создать схему. Зовётся один раз на старте."""
    global _conn
    _maybe_reload_rates()
    # check_same_thread=False — соединение шерим между потоками; сериализация
    # лежит на _lock. isolation_level=None (autocommit) — управляем транзакциями
    # явно через BEGIN IMMEDIATE / COMMIT внутри _tx().
    _conn = sqlite3.connect(_DB_FILE, check_same_thread=False, isolation_level=None)
    _init_schema()
    n = _conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    log.log(f"(economy) БД {_DB_FILE} открыта, {n} аккаунтов")


# ---------- helpers ----------

class _tx:
    """`with _tx():` — атомарная транзакция под _lock. Без вложенности.
    BEGIN IMMEDIATE сразу берёт write-lock БД — другие writers подождут;
    реальная сериализация и так на _lock, это страховка."""
    def __enter__(self):
        _lock.acquire()
        _conn.execute("BEGIN IMMEDIATE")
        return _conn
    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                _conn.execute("COMMIT")
            else:
                _conn.execute("ROLLBACK")
        finally:
            _lock.release()


def _upsert_meta(uid, login, display):
    """Создаёт строку для uid если её нет, обновляет login/display только если
    они переданы непустыми. Должно вызываться внутри _tx()."""
    _conn.execute("INSERT OR IGNORE INTO users(uid) VALUES (?)", (uid,))
    if login:
        _conn.execute("UPDATE users SET login=? WHERE uid=?", (login.lower(), uid))
    if display:
        _conn.execute("UPDATE users SET display=? WHERE uid=?", (display, uid))


def _balance_of(uid):
    """Под _tx() — текущий баланс."""
    row = _conn.execute("SELECT balance FROM users WHERE uid=?", (uid,)).fetchone()
    return row[0] if row else 0


# ---------- Публичный API ----------

def award(user_id, amount, reason="", login=None, display=None):
    """Начислить amount монет user_id'у. login/display обновляются если переданы.
    Возвращает новый баланс или None если данных не хватило."""
    if not user_id or amount <= 0:
        return None
    now = time.time()
    with _tx():
        _upsert_meta(user_id, login, display)
        _conn.execute(
            "UPDATE users SET balance=balance+?, earned=earned+?, last_seen_ts=? WHERE uid=?",
            (amount, amount, now, user_id))
        bal = _balance_of(user_id)
    if reason:
        log.log(f"(economy) +{amount} → {login or user_id} ({reason}), баланс={bal}")
    return bal


def award_chat(user_id, login, display):
    """Бонус за сообщение в чате с кулдауном. Возвращает True если начислили."""
    if not user_id:
        return False
    amount = RATES["chat_message"]
    cooldown = RATES["chat_cooldown_sec"]
    if amount <= 0:
        return False
    now = time.time()
    with _tx():
        _upsert_meta(user_id, login, display)
        row = _conn.execute(
            "SELECT last_msg_ts FROM users WHERE uid=?", (user_id,)
        ).fetchone()
        last = row[0] if row else 0.0
        if now - last < cooldown:
            # Кулдаун — обновляем только тайминги, баланс не трогаем.
            _conn.execute(
                "UPDATE users SET last_msg_ts=?, last_seen_ts=? WHERE uid=?",
                (now, now, user_id))
            return False
        _conn.execute(
            "UPDATE users SET balance=balance+?, earned=earned+?, last_msg_ts=?, last_seen_ts=? WHERE uid=?",
            (amount, amount, now, now, user_id))
    return True


def award_follow(user_id, login=None, display=None):
    """Follow-бонус с дедупом: один раз на user_id за всё время.
    INSERT OR IGNORE + UPDATE в одной транзакции — либо обе записи на диске,
    либо ни одной; повторный fold не даст бонус, потерянный bonus невозможен."""
    if not user_id:
        return None
    amount = RATES["follow"]
    if amount <= 0:
        return None
    now = time.time()
    with _tx():
        cur = _conn.execute("INSERT OR IGNORE INTO follow_bonus(uid) VALUES (?)", (user_id,))
        if cur.rowcount == 0:
            return None
        _upsert_meta(user_id, login, display)
        _conn.execute(
            "UPDATE users SET balance=balance+?, earned=earned+?, last_seen_ts=? WHERE uid=?",
            (amount, amount, now, user_id))
        bal = _balance_of(user_id)
    log.log(f"(economy) +{amount} → {login or user_id} (follow), баланс={bal}")
    return bal


_WATCHTIME_BATCH = 500


def award_watchtime(uids_to_info):
    """uids_to_info: {user_id: (login, display)} — из /chat/chatters.
    Каждому +watchtime_base, активным (писал недавно) ещё +watchtime_active.

    Бьём на батчи по _WATCHTIME_BATCH чтобы не держать write-lock дольше ~50мс:
    иначе на крупном канале balance/award_chat от IRC-loop будут ждать всю
    обработку. Между батчами _lock освобождается, читатели проскакивают."""
    base = RATES["watchtime_base"]
    active_bonus = RATES["watchtime_active"]
    active_window = RATES["watchtime_active_sec"]
    if base <= 0 and active_bonus <= 0:
        return 0, 0
    now = time.time()
    given = 0
    active = 0
    items = [(uid, info) for uid, info in uids_to_info.items() if uid]
    for i in range(0, len(items), _WATCHTIME_BATCH):
        with _tx():
            for uid, (login, display) in items[i:i + _WATCHTIME_BATCH]:
                _upsert_meta(uid, login, display)
                row = _conn.execute(
                    "SELECT last_msg_ts FROM users WHERE uid=?", (uid,)
                ).fetchone()
                last = row[0] if row else 0.0
                amount = base
                if now - last < active_window:
                    amount += active_bonus
                    active += 1
                if amount > 0:
                    _conn.execute(
                        "UPDATE users SET balance=balance+?, earned=earned+?, last_seen_ts=? WHERE uid=?",
                        (amount, amount, now, uid))
                    given += 1
    return given, active


def balance(user_id):
    with _lock:
        row = _conn.execute(
            "SELECT balance FROM users WHERE uid=?", (user_id,)
        ).fetchone()
    return row[0] if row else 0


def balance_by_login(login):
    """Поиск по нику (лог-ин ниже регистром). O(log n) через idx_users_login."""
    login = (login or "").lstrip("@").lower()
    if not login:
        return None
    with _lock:
        row = _conn.execute(
            "SELECT balance FROM users WHERE login=? LIMIT 1", (login,)
        ).fetchone()
    return row[0] if row else None


def transfer(from_uid, to_login, amount):
    """Перевод по нику получателя. Возвращает (ok: bool, msg: str)."""
    if amount <= 0:
        return False, "сумма должна быть больше 0"
    to_login = (to_login or "").lstrip("@").lower()
    if not to_login:
        return False, "укажи получателя: !дать @ник N"
    with _tx():
        src = _conn.execute(
            "SELECT login, balance FROM users WHERE uid=?", (from_uid,)
        ).fetchone()
        src_bal = src[1] if src else 0
        if not src or src_bal < amount:
            return False, f"недостаточно монет (у тебя {src_bal})"
        if src[0] == to_login:
            return False, "нельзя переводить самому себе"
        dst = _conn.execute(
            "SELECT uid, display, login FROM users WHERE login=? LIMIT 1", (to_login,)
        ).fetchone()
        if not dst:
            return False, f"@{to_login} ещё ни разу не появлялся, ему пока некуда переводить"
        _conn.execute(
            "UPDATE users SET balance=balance-? WHERE uid=?", (amount, from_uid))
        _conn.execute(
            "UPDATE users SET balance=balance+?, earned=earned+? WHERE uid=?",
            (amount, amount, dst[0]))
        dst_disp = dst[1] or dst[2]
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
        rows = _conn.execute(
            "SELECT COALESCE(NULLIF(display, ''), login) AS name, balance "
            "FROM users WHERE balance > 0 ORDER BY balance DESC LIMIT ?",
            (n,)
        ).fetchall()
    return [(r[0], r[1]) for r in rows]
