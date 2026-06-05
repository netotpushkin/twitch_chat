"""Сбор средств: инкрементальные цели с LLM-заголовком.

Логика:
  - Цель стартует с 10 ₽. Каждый донат (любая валюта, сумма складывается как
    число) увеличивает progress. Как только progress >= target — текущий сбор
    закрывается, target += 1, progress сбрасывается в 0, LLM генерирует новый
    заголовок, а в чат уходит анонс.
  - Переплата НЕ переносится: если на цели 10 ₽ задонатили 25 — закрывается
    одна цель, новая стартует с 0 / 11. Так задумано.
  - Состояние живёт в state/goal.json (atomic-запись), переживает рестарт.

В шину goal_bus публикуем два типа событий:
  {"type": "goal_update",    "target": N, "progress": M, "title": "..."}
  {"type": "goal_completed", "target": N, "title": "..."}
"""

import json
import os
import threading

from config import OPENROUTER_API_KEY, STATE_DIR
from events import donatty_bus, goal_bus
import openrouter


STATE_PATH = os.path.join(STATE_DIR, "goal.json")

INITIAL_TARGET = 10
LLM_TIMEOUT    = 15
LLM_MAX_TOKENS = 200
TITLE_MAX_LEN  = 100
ADVANCE_DELAY  = 10.0   # сек паузы между закрытием цели и стартом следующей

SYSTEM_PROMPT = (
    "Придумай случайную цель сбора на Twitch-стриме. Тон: едкий сарказм, "
    "усталая ирония над повседневностью, потреблением, инфоцыганством, "
    "соцсетями, стримерской средой, рабочей рутиной, отношениями с собой "
    "и людьми. Цель формулируется на полном серьёзе — как реальный лот или "
    "сервис — но сам объект высмеивает что-то очевидное и плохое в жизни.\n"
    "Не банально, не зло, не оскорбительно к конкретным группам. Сухой "
    "разочарованный взгляд, не клоунада. Тематика — рандом, каждый раз "
    "новая сфера.\n"
    "Формулировка: одна строка, именная или с глаголом-инфинитивом. Без "
    "вопросов, восклицаний, обращений к зрителю, без слов «сбор», «копим», "
    "«поддержим». В тексте цели НЕ упоминай конкретные суммы, цены, рубли, ₽ — "
    "ни числами, ни словами.\n"
    f"До {TITLE_MAX_LEN} символов, по-русски, без кавычек, без точки в конце. "
    "Верни только цель."
)

_lock = threading.RLock()
_state = {
    "target":    INITIAL_TARGET,
    "progress":  0.0,
    "title":     f"Сбор на {INITIAL_TARGET} ₽",
    "completed": 0,
}
_announce = None  # callable(text) — постинг в чат, ставится в start()
_log = print

# Активный threading.Timer, отсчитывающий ADVANCE_DELAY между закрытием
# текущей цели и стартом следующей. Пока установлен — донаты игнорируются
# и состояние «зависшее на 100%».
_advance_timer: "threading.Timer | None" = None


def _atomic_write(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)


def _load():
    if not os.path.exists(STATE_PATH):
        return False
    try:
        with open(STATE_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, ValueError) as e:
        _log(f"(goal) state/goal.json битый, игнорирую: {e}")
        return False
    if not isinstance(data, dict):
        return False
    target = data.get("target")
    progress = data.get("progress")
    title = data.get("title")
    if not isinstance(target, int) or target < 1:
        return False
    if not isinstance(progress, (int, float)) or progress < 0:
        return False
    if not isinstance(title, str) or not title:
        title = _fallback_title(target)
    completed = data.get("completed")
    if not isinstance(completed, int) or completed < 0:
        completed = 0
    _state["target"]    = target
    _state["progress"]  = float(progress)
    _state["title"]     = title
    _state["completed"] = completed
    return True


def _save():
    try:
        _atomic_write(STATE_PATH, _state)
    except OSError as e:
        _log(f"(goal) запись state/goal.json не удалась: {e}")


def _fallback_title(target):
    return f"Сбор на {target} ₽"


def _generate_title(target):
    """Спросить у LLM короткий заголовок. На любую ошибку — fallback."""
    if not OPENROUTER_API_KEY:
        return _fallback_title(target)
    user_msg = f"Сумма сбора: {target} ₽. Сгенерируй цель."
    try:
        raw = openrouter.ask(
            user_msg,
            system=SYSTEM_PROMPT,
            temperature=0.9,
            max_tokens=LLM_MAX_TOKENS,
            timeout=LLM_TIMEOUT,
        )
    except openrouter.OpenRouterError as e:
        _log(f"(goal) LLM ошибка, fallback: {e}")
        return _fallback_title(target)
    title = (raw or "").strip().rstrip(".").strip()
    # Снимаем обёрточные кавычки ТОЛЬКО если LLM обернула всю строку целиком,
    # не задевая кавычки внутри (например, «Пятёрочка», "Хрусteam" и т.п.).
    for op, cl in (('"', '"'), ("'", "'"), ("«", "»")):
        if len(title) >= 2 and title.startswith(op) and title.endswith(cl):
            title = title[1:-1].strip()
            break
    if not title:
        return _fallback_title(target)
    if len(title) > TITLE_MAX_LEN:
        # Грубо обрезаем по слову, чтобы не получить «полусловие».
        cut = title[:TITLE_MAX_LEN].rsplit(" ", 1)[0]
        title = cut if len(cut) >= TITLE_MAX_LEN // 2 else title[:TITLE_MAX_LEN]
    return title


def snapshot():
    """Текущее состояние для первой посылки SSE-клиенту."""
    with _lock:
        return {
            "type":     "goal_update",
            "target":   _state["target"],
            "progress": _state["progress"],
            "title":    _state["title"],
        }


def _publish_update():
    goal_bus.publish(snapshot())


def _announce_new_goal(title, target):
    if not _announce:
        return
    try:
        _announce(f"Новый сбор: «{title}» — цель {target} ₽")
    except Exception as e:
        _log(f"(goal) announce упал: {e}")


def _cancel_advance_timer():
    """Отменить отложенный переход к новой цели (если он есть)."""
    global _advance_timer
    if _advance_timer is not None:
        try:
            _advance_timer.cancel()
        except Exception:
            pass
        _advance_timer = None


def _advance():
    """Переход к следующей цели после паузы ADVANCE_DELAY.
    LLM-вызов держим вне _lock, чтобы не блокировать обработку донатов."""
    global _advance_timer
    with _lock:
        new_target = _state["target"] + 1
    new_title = _generate_title(new_target)
    with _lock:
        _state["target"]    = new_target
        _state["progress"]  = 0.0
        _state["title"]     = new_title
        _save()
        _advance_timer = None
    _publish_update()
    _announce_new_goal(new_title, new_target)
    _log(f"(goal) стартует {new_target} ₽: «{new_title}»")


def _schedule_advance(delay=ADVANCE_DELAY):
    """Запланировать переход к новой цели через `delay` секунд."""
    global _advance_timer
    _cancel_advance_timer()
    _advance_timer = threading.Timer(delay, _advance)
    _advance_timer.daemon = True
    _advance_timer.start()


def reset_for_test():
    """Сброс в начальное состояние (для /test/goal_reset)."""
    _cancel_advance_timer()
    with _lock:
        _state["target"]    = INITIAL_TARGET
        _state["progress"]  = 0.0
        _state["title"]     = _generate_title(INITIAL_TARGET)
        _state["completed"] = 0
        _save()
    _publish_update()
    _announce_new_goal(_state["title"], _state["target"])


def _on_donation(event):
    try:
        amount = float(event.get("amount") or 0)
    except (TypeError, ValueError):
        amount = 0.0
    if amount <= 0:
        return

    schedule_advance = False
    completed_payload = None
    with _lock:
        if _advance_timer is not None:
            # Идёт пауза между сборами — донаты в этот момент пропускаем.
            # Переплата всё равно сбрасывается при старте новой цели.
            return
        _state["progress"] += amount
        if _state["progress"] < _state["target"]:
            _save()
            update = dict(snapshot())
        else:
            # Цель достигнута. Фиксируем визуально на 100% (бар «дорисуется»
            # до конца), счётчик увеличиваем, паузу запускаем.
            old_target = _state["target"]
            old_title  = _state["title"]
            _state["completed"] += 1
            _state["progress"]  = _state["target"]   # для красоты бара = 100%
            _save()
            update = dict(snapshot())
            completed_payload = {
                "type":   "goal_completed",
                "target": old_target,
                "title":  old_title,
            }
            schedule_advance = True

    goal_bus.publish(update)
    if completed_payload:
        goal_bus.publish(completed_payload)
        _log(f"(goal) цель {completed_payload['target']} ₽ закрыта; "
             f"следующая через {ADVANCE_DELAY:g}с")
    if schedule_advance:
        _schedule_advance()


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
            _log(f"(goal) обработка упала: {type(e).__name__}: {e}")


def start(announce=None, log=print):
    """Поднять листенер донатов. На первый запуск (нет state/goal.json) —
    сгенерировать стартовый заголовок и объявить новую цель в чат."""
    global _announce, _log
    _announce = announce
    _log = log

    loaded = _load()
    if not loaded:
        with _lock:
            _state["target"]    = INITIAL_TARGET
            _state["progress"]  = 0.0
            _state["title"]     = _generate_title(INITIAL_TARGET)
            _state["completed"] = 0
            _save()
            title  = _state["title"]
            target = _state["target"]
        _announce_new_goal(title, target)
        _log(f"(goal) первый сбор: «{title}» — цель {target} ₽")
    else:
        _log(f"(goal) загружен сбор: «{_state['title']}» — "
             f"{_state['progress']:g} / {_state['target']} ₽")
        # Если рестартанули посреди «паузы» (progress >= target из сохранёнки) —
        # сразу запускаем отложенный переход, не блокируя стартап.
        with _lock:
            need_advance = _state["progress"] >= _state["target"]
        if need_advance:
            _log("(goal) state застал на 100% — планирую переход к следующей цели")
            _schedule_advance(delay=ADVANCE_DELAY)

    # На случай если overlay уже подключился до загрузки state — пушим
    # актуальный snapshot, чтобы он перерисовался с правильными цифрами.
    _publish_update()
    threading.Thread(target=_listener, daemon=True, name="goal").start()
