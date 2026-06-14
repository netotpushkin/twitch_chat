"""SSE-шины: бродкастеры событий для оверлеев."""

import json
import queue
import threading
import time


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


class MediaBroadcaster(Broadcaster):
    """Broadcaster, который запоминает текущее состояние плеера и отдаёт его новому
    подписчику. Нужно, чтобы переподключившийся оверлей (реконнект SSE, смена сцены
    в OBS) сразу ресинхронизировался: показал играющий клип с нужной позиции/громкости
    или остался скрытым, если ничего не играет. Без этого единственное событие play/stop
    могло прийти в момент, когда оверлей был отключён, и потеряться навсегда."""
    def __init__(self):
        super().__init__()
        self._state_lock = threading.Lock()
        self._play = None      # последнее событие play или None если остановлено
        self._play_at = 0.0    # monotonic-время публикации play — для пересчёта позиции
        self._volume = None    # последняя известная громкость

    def publish(self, obj):
        evt = obj.get("evt")
        # Держим _state_lock на всё время (вместе с рассылкой в очереди), чтобы
        # subscribe_with_snapshot не словил это событие и в снапшоте, и в очереди.
        with self._state_lock:
            if evt == "play":
                self._play = dict(obj)
                self._play_at = time.monotonic()
                if obj.get("volume") is not None:
                    self._volume = obj["volume"]
            elif evt == "stop":
                self._play = None
            elif evt == "volume":
                self._volume = obj.get("value")
            super().publish(obj)

    def _snapshot_locked(self):
        out = []
        if self._volume is not None:
            out.append({"evt": "volume", "value": self._volume})
        if self._play is not None:
            ev = dict(self._play)
            elapsed = max(0.0, time.monotonic() - self._play_at)
            ev["start"] = int((ev.get("start") or 0) + elapsed)  # доигрываем с текущей позиции
            if self._volume is not None:
                ev["volume"] = self._volume
            out.append(ev)
        return out

    def subscribe_with_snapshot(self):
        """Атомарно подписывает клиента и возвращает (queue, события-снапшота).
        Атомарность исключает потерю/дубль текущего состояния на стыке с publish()."""
        with self._state_lock:
            q = self.subscribe()
            return q, self._snapshot_locked()


chat_bus    = Broadcaster()  # /stream  — обычный чат
events_bus  = Broadcaster()  # /events  — алерты (фолловеры/сабы/рейды)
media_bus   = MediaBroadcaster()  # /media   — YouTube-плеер
dice_bus    = Broadcaster()  # /dice    — анимация броска д20
donatty_bus = Broadcaster()  # /donatty — донаты через Donatty + TTS-события (type=tts)
goal_bus    = Broadcaster()  # /goal    — текущий сбор: цель, прогресс, заголовок
image_bus   = Broadcaster()  # /images  — картинки/гифки от VIP/модов на оверлее
