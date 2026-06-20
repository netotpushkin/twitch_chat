"""EventSub WebSocket: подписка на фолловеров/сабов/рейдов и публикация в events_bus."""

import json
import time
import urllib.error

import http_pool
import log
from config import CLIENT_ID
from events import events_bus


# Опциональная зависимость — если не установлено, EventSub просто отключается.
try:
    import websocket  # pip install websocket-client
except ImportError:
    websocket = None


EVENTSUB_URL = "wss://eventsub.wss.twitch.tv/ws"


def _eventsub_subscribe(token, sub_type, version, condition, session_id):
    """POST /helix/eventsub/subscriptions — подписаться на событие в сессии."""
    body = json.dumps({
        "type": sub_type,
        "version": version,
        "condition": condition,
        "transport": {"method": "websocket", "session_id": session_id},
    }).encode("utf-8")
    _, _, resp_body = http_pool.request(
        "POST",
        "https://api.twitch.tv/helix/eventsub/subscriptions",
        headers={
            "Authorization": f"Bearer {token}",
            "Client-Id": CLIENT_ID,
            "Content-Type": "application/json",
        },
        body=body,
        timeout=10,
    )
    return json.loads(resp_body)


def run_eventsub(token, broadcaster_id, our_user_id):
    """Подключение к EventSub WebSocket и публикация событий в events_bus."""
    if websocket is None:
        log.log("(!) websocket-client не установлен — алерты выключены. "
              "Установи: pip install websocket-client")
        return
    if not broadcaster_id or not our_user_id:
        log.log("(!) EventSub: нет broadcaster_id или user_id — алерты выключены.")
        return

    state = {"url": EVENTSUB_URL, "session_id": None, "subscribed": False, "give_up": False}

    def subscribe_all(sid):
        targets = [
            ("channel.follow", "2", {
                "broadcaster_user_id": broadcaster_id,
                "moderator_user_id":   our_user_id,
            }),
            ("channel.subscribe", "1", {
                "broadcaster_user_id": broadcaster_id,
            }),
            ("channel.subscription.message", "1", {
                "broadcaster_user_id": broadcaster_id,
            }),
            ("channel.subscription.gift", "1", {
                "broadcaster_user_id": broadcaster_id,
            }),
            ("channel.raid", "1", {
                "to_broadcaster_user_id": broadcaster_id,
            }),
        ]
        for sub_type, version, condition in targets:
            try:
                _eventsub_subscribe(token, sub_type, version, condition, sid)
                log.log(f"(EventSub) подписан на {sub_type} для {broadcaster_id}")
            except urllib.error.HTTPError as e:
                try: body = e.read().decode("utf-8", "ignore")
                except Exception: body = ""
                log.log(f"(EventSub) {sub_type} не удалась: HTTP {e.code} {body}")
                if e.code == 403 and sub_type == "channel.follow":
                    log.log("    Нужны права мода/стримера на этом канале + скоуп moderator:read:followers.")
                    log.log("    Алерты отключаются до перезапуска бота.")
                    state["give_up"] = True
                    return
            except Exception as e:
                log.log(f"(EventSub) {sub_type} не удалась: {e}")

    def on_message(ws, raw):
        try:
            m = json.loads(raw)
        except Exception:
            return
        mt = m.get("metadata", {}).get("message_type", "")
        if mt == "session_welcome":
            sid = m["payload"]["session"]["id"]
            state["session_id"] = sid
            if not state["subscribed"]:
                subscribe_all(sid)
                state["subscribed"] = True
        elif mt == "session_keepalive":
            pass
        elif mt == "notification":
            st = m["metadata"]["subscription_type"]
            ev = m["payload"]["event"]
            if st == "channel.follow":
                # Освежаем кэш фолловеров, чтобы модерация сразу понизила тир до light.
                from twitch_api import mark_follower
                uid = ev.get("user_id", "")
                login = (ev.get("user_login") or "").lower()
                display = ev.get("user_name", "")
                mark_follower(uid)
                events_bus.publish({
                    "type":  "follow",
                    "user":  display,
                    "login": login,
                    "at":    ev.get("followed_at", ""),
                })
            elif st == "channel.subscribe":
                login = (ev.get("user_login") or "").lower()
                display = ev.get("user_name", "")
                events_bus.publish({
                    "type":  "sub",
                    "user":  display,
                    "login": login,
                    "tier":  ev.get("tier", ""),
                    "gift":  bool(ev.get("is_gift")),
                })
            elif st == "channel.subscription.message":
                msg = (ev.get("message") or {}).get("text", "")
                login = (ev.get("user_login") or "").lower()
                display = ev.get("user_name", "")
                events_bus.publish({
                    "type":     "resub",
                    "user":     display,
                    "login":    login,
                    "tier":     ev.get("tier", ""),
                    "months":   ev.get("cumulative_months") or 0,
                    "streak":   ev.get("streak_months") or 0,
                    "duration": ev.get("duration_months") or 0,
                    "message":  msg,
                })
            elif st == "channel.raid":
                login = (ev.get("from_broadcaster_user_login") or "").lower()
                display = ev.get("from_broadcaster_user_name", "")
                events_bus.publish({
                    "type":    "raid",
                    "user":    display,
                    "login":   login,
                    "viewers": ev.get("viewers") or 0,
                })
            elif st == "channel.subscription.gift":
                anon = bool(ev.get("is_anonymous"))
                login = (ev.get("user_login") or "").lower()
                display = ev.get("user_name", "")
                total = ev.get("total") or 0
                events_bus.publish({
                    "type":  "subgift",
                    "user":  "Аноним" if anon else display,
                    "login": "" if anon else login,
                    "tier":  ev.get("tier", ""),
                    "total": total,
                    "cumulative": ev.get("cumulative_total") or 0,
                    "anon":  anon,
                })
        elif mt == "session_reconnect":
            # Мягкая миграция через reconnect_url не реализована (см. комментарий
            # у внешнего цикла). Просто закрываем сокет — фоллбек: новый коннект
            # на базовый URL → новый session_welcome → subscribe_all.
            ws.close()
        elif mt == "revocation":
            log.log(f"(EventSub) подписка отозвана: {m['payload']}")
            state["subscribed"] = False

    def on_error(ws, err):
        if state["give_up"]:
            return
        log.log(f"(EventSub) ошибка: {err}")

    def on_close(ws, code, msg):
        pass

    while not state["give_up"]:
        try:
            ws = websocket.WebSocketApp(
                state["url"],
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=20, ping_timeout=10)
        except Exception as e:
            log.log(f"(EventSub) исключение: {e}")
        if state["give_up"]:
            return
        # После любого разрыва возвращаемся на базовый URL и переподписываемся.
        # Twitch предлагает мягкую миграцию через session_reconnect, но трактовать
        # её корректно (с тайм-аутом reconnect_url) — лишняя сложность. Здесь
        # просто фоллбек: новый коннект → новый session_welcome → subscribe_all.
        state["subscribed"] = False
        state["url"] = EVENTSUB_URL
        time.sleep(3)
