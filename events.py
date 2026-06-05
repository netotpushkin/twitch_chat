"""SSE-шины: бродкастеры событий для оверлеев."""

import json
import queue
import threading


class Broadcaster:
    """Pub/sub: подписчики получают копии всех опубликованных событий через Queue."""
    def __init__(self):
        self.clients = []
        self.lock = threading.Lock()

    def subscribe(self):
        q = queue.Queue(maxsize=200)
        with self.lock:
            self.clients.append(q)
        return q

    def unsubscribe(self, q):
        with self.lock:
            if q in self.clients:
                self.clients.remove(q)

    def publish(self, obj):
        data = json.dumps(obj, ensure_ascii=False)
        with self.lock:
            targets = list(self.clients)
        for q in targets:
            try:
                q.put_nowait(data)
            except queue.Full:
                pass


chat_bus    = Broadcaster()  # /stream  — обычный чат
events_bus  = Broadcaster()  # /events  — алерты (фолловеры/сабы/рейды)
media_bus   = Broadcaster()  # /media   — YouTube-плеер
dice_bus    = Broadcaster()  # /dice    — анимация броска д20
donatty_bus = Broadcaster()  # /donatty — донаты через Donatty + TTS-события (type=tts)
emote_bus   = Broadcaster()  # /emote_rain — поток эмоутов/эмодзи для оверлея-дождя
goal_bus    = Broadcaster()  # /goal    — текущий сбор: цель, прогресс, заголовок
