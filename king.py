"""Трекер «короля доната» за сессию.

Король — автор последнего ненулевого не-анонимного доната. Каждый новый донат
переписывает корону. Имя из поля `subscriber` Donatty приводится к lowercase и
сравнивается с Twitch-логином автора сообщения в чате — если донатер написал
ник, которого нет на твиче, корона висит «впустую» до следующего доната.

При смене короля:
    • в чат идёт уведомление через send-callback
    • повторный донат от того же короля корону не переподтверждает (только сумма
      обновляется в state)

Состояние живёт в памяти, на рестарте бота обнуляется."""

import json
import threading

from events import donatty_bus
import tts


_ANON_NAMES = {"", "аноним", "anonymous", "anon"}

# Склонения валют для TTS: (1, 2-4, 5+) + грамматический род (для num2words).
# Если кода нет в словаре — читаем код буквами.
_CURRENCY_WORDS = {
    "RUB": ("рубль",  "рубля",  "рублей",  "masculine"),
    "RUR": ("рубль",  "рубля",  "рублей",  "masculine"),
    "USD": ("доллар", "доллара", "долларов", "masculine"),
    "EUR": ("евро",   "евро",   "евро",    "neuter"),
    "UAH": ("гривна", "гривны", "гривен",  "feminine"),
    "KZT": ("тенге",  "тенге",  "тенге",   "masculine"),
    "BYN": ("белорусский рубль", "белорусских рубля", "белорусских рублей", "masculine"),
}


def _plural_ru(n, one, few, many):
    """1 рубль, 2 рубля, 5 рублей, 21 рубль, 25 рублей и т.д."""
    n = int(abs(n))
    if 11 <= n % 100 <= 14:
        return many
    r = n % 10
    if r == 1: return one
    if 2 <= r <= 4: return few
    return many


def _spell_number(n, gender="masculine"):
    """500 → «пятьсот»; Silero не умеет цифры, превращаем в слова."""
    try:
        from num2words import num2words
        return num2words(int(n), lang="ru", gender=gender)
    except Exception:
        return str(int(n))


def _amount_phrase(amount, currency):
    """«пятьсот рублей» из (500, 'RUB'). Дроби обрезаем — TTS звучит лучше."""
    n = int(round(amount))
    forms = _CURRENCY_WORDS.get((currency or "").upper())
    if forms:
        one, few, many, gender = forms
        word = _plural_ru(n, one, few, many)
    else:
        word = currency or ""
        gender = "masculine"
    return f"{_spell_number(n, gender)} {word}".strip()

_lock = threading.Lock()
_state = {
    "login":   None,   # lowercase twitch login кандидата
    "display": None,   # как написано в донате — для красивого упоминания
    "amount":  0.0,
    "currency": "",
}
_announce = None      # callable(str) — пишет в чат; ставится в start()
_log = print


def current_king_login():
    with _lock:
        return _state["login"]


def is_king(login):
    if not login:
        return False
    with _lock:
        return _state["login"] == login.lower()


def _on_donation(event):
    """Озвучиваем любой донат с текстом + при необходимости меняем короля."""
    donation_id = event.get("id") or ""
    subscriber = (event.get("user") or "").strip()
    try:
        amount = float(event.get("amount") or 0)
    except (TypeError, ValueError):
        amount = 0
    message = (event.get("message") or "").strip()
    currency = event.get("currency") or ""

    # Озвучиваем КАЖДЫЙ донат. Формат: «Имя, спасибо за 500 рублей. Пишет: текст».
    # Анонимам — без «спасибо», только их текст. Совсем пустой донат — пропускаем.
    is_anon = subscriber.lower() in _ANON_NAMES
    if is_anon:
        spoken = message
    elif amount > 0:
        spoken = f"{subscriber}, спасибо за {_amount_phrase(amount, currency)}."
        if message:
            spoken += f" Пишет: {message}"
    else:
        spoken = f"{subscriber} пишет: {message}" if message else ""
    if spoken:
        tts.enqueue(spoken, source="donation", donation_id=donation_id)

    # Дальше — логика короля. Корону получает АВТОР ПОСЛЕДНЕГО ДОНАТА:
    # любой ненулевой не-анонимный донат сразу переписывает короля.
    if subscriber.lower() in _ANON_NAMES or amount <= 0:
        return

    new_login = subscriber.lower()

    with _lock:
        prev_login = _state["login"]
        if prev_login == new_login:
            # Тот же донатер задонатил повторно — корона уже у него, тихо обновляем сумму.
            _state["amount"]   = amount
            _state["currency"] = currency
            return
        _state["login"]    = new_login
        _state["display"]  = subscriber
        _state["amount"]   = amount
        _state["currency"] = currency

    _log(f"(king) новый король: {subscriber} ({amount} {currency})")

    if _announce:
        try:
            _announce(
                f"👑 @{subscriber} перехватил корону "
                f"({amount:g} {currency}) — его сообщения в чате теперь озвучены"
            )
        except Exception as e:
            _log(f"(king) ошибка announce: {e}")


def _listener():
    q = donatty_bus.subscribe()
    while True:
        raw = q.get()
        try:
            event = json.loads(raw)
        except (TypeError, ValueError):
            continue
        if event.get("type") != "donation":
            continue
        try:
            _on_donation(event)
        except Exception as e:
            _log(f"(king) обработка упала: {e}")


def start(announce=None, log=print):
    """Запустить листенер шины донатов. announce(text) — отправка в чат."""
    global _announce, _log
    _announce = announce
    _log = log
    threading.Thread(target=_listener, daemon=True, name="king").start()
