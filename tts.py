"""TTS через Silero v4_ru: фоновая загрузка модели, очередь синтеза.

Поток:
    enqueue(text, source) → worker берёт из очереди → Silero синтезирует →
    WAV кодируется в base64 и публикуется в donatty_bus как событие type=tts.
    Оверлей donatty.html декодирует base64 → ArrayBuffer → AudioContext.decodeAudioData.

Почему base64 в SSE, а не отдельный HTTP GET за WAV:
    OBS Browser Source / CEF использует Chromium-сеть с лимитом 6 параллельных
    HTTP/1.1-коннектов на origin. SSE-стримы оверлеев (chat/donatty/events/dice/
    media/emote_rain) держат ровно эти 6 слотов 24/7 — любой GET /tts/audio/<id>
    встаёт в очередь и никогда не выполняется. Инлайн WAV в само событие обходит
    проблему: лишних коннектов не нужно.

Модель грузится один раз в _loader_thread (~60МБ + torch.hub скачивание при первом
запуске). До загрузки enqueue копит задания, они исполнятся как только модель готова.
"""

import base64
import io
import re
import threading
import time
import queue as _queue
import uuid
import wave

from config import TTS_SPEAKER, TTS_SPEAKER_CHAT
from events import donatty_bus


# ---------- Параметры синтеза ----------

_SPEAKER       = TTS_SPEAKER   # из config / env TTS_SPEAKER, см. config.py
_SPEAKER_CHAT  = TTS_SPEAKER_CHAT  # голос для режима «озвучка всех» (!озвучка)
# 48 кГц — стандартная частота браузерного AudioContext. Если отдадим 24 кГц,
# CEF в OBS будет апсемплить → металлический звон на согласных. На 48 кГц
# ресэмплинга в браузере нет; Silero справляется с внутренним апсемплингом
# лучше, чем дефолтный аудиостек Chromium.
_SAMPLE_RATE   = 48000
_MAX_CHARS     = 200       # длиннее обрезаем
# Короткие чат-реплики ("+", "лол", "да", "ок") не озвучиваем — на 100мс
# fade-in/out от них ничего не остаётся, и в звуковом потоке это просто щелчки.
# Донаты этим порогом не задеваются: enqueue проверяет только _CHAT_SOURCES.
_CHAT_MIN_CHARS = 5


# ---------- Состояние модуля ----------

_model = None
_model_ready = threading.Event()

# Режим озвучки чата: "king" — только сообщения короля доната (дефолт),
# "all" — каждое сообщение в чате. Переключается командой !озвучка.
_chat_mode = "king"
_chat_mode_lock = threading.Lock()

# Источники, для которых работает «один за раз»: новые чат-сообщения дропаются,
# пока проигрывается предыдущее, и сразу после него озвучивается СЛЕДУЮЩЕЕ
# пришедшее, а не всё накопленное. Донаты (source="donation") не дропаются — у
# них есть donation_id, привязанный к оверлей-модалке, очередь обязана выполниться.
_CHAT_SOURCES = frozenset({"chat-all", "king-message"})

# monotonic-таймштамп, до которого считаем, что аудио ещё проигрывается.
# Под одной блокировкой проверяется и обновляется и в enqueue (резервация при
# постановке в очередь), и в воркере (точная длительность после синтеза) — это
# закрывает гонку «два сообщения проскочили gate до того, как воркер выставит
# busy». Браузерному пайплайну (SSE → decodeAudioData → playback) добавляем
# небольшой буфер, чтобы не наложиться на хвост звука.
_busy_until = 0.0
_busy_lock = threading.Lock()
_PLAYBACK_BUFFER_SEC = 0.4  # запас на доставку события и старт декодирования в OBS
# Резервация в enqueue: на момент постановки точная длительность ещё неизвестна
# (синтез не начат), даём заведомо больший потолок. Воркер перепишет на реальное
# значение после _model.apply_tts. 60с покрывает синтез самой длинной фразы
# (_MAX_CHARS=200) на CPU с большим запасом.
_SYNTH_SAFETY_SEC = 60.0


def _set_busy_until(ts):
    global _busy_until
    with _busy_lock:
        _busy_until = ts


def get_chat_mode():
    with _chat_mode_lock:
        return _chat_mode


def toggle_chat_mode():
    """Переключает режим king↔all, возвращает новое значение."""
    global _chat_mode
    with _chat_mode_lock:
        _chat_mode = "all" if _chat_mode == "king" else "king"
        return _chat_mode

# (audio_id, text, source, donation_id, speaker) — donation_id связывает аудио с донатом
# для оверлея; speaker позволяет режиму «озвучка всех» использовать отдельный голос.
_jobs: "_queue.Queue[tuple[str, str, str, str, str]]" = _queue.Queue(maxsize=64)
_log = print  # перенастраивается в start()


# ---------- Утилиты ----------

_URL_RE = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")
_DIGITS_RE = re.compile(r"\d+")
_LATIN_RUN_RE = re.compile(r"[a-zA-Z]+")
# Силеро не любит чистые символы/эмоджи — оставляем буквы/цифры/пробелы/русскую пунктуацию.
_PRINTABLE_RE = re.compile(r"[^\w\s.,!?;:—\-–'\"()«»]+", re.UNICODE)


def _digits_to_words(text):
    """Silero не читает цифры — заменяем «3» на «три». Род — мужской по дефолту;
    основной кейс с правильным родом уже отработан в king._amount_phrase."""
    try:
        from num2words import num2words
    except ImportError:
        return text
    def _sub(m):
        try:
            # Пробелы по краям, чтобы «TestUser10» не склеилось в «тестусердесять».
            # Лишние пробелы потом схлопывает _WS_RE.
            return " " + num2words(int(m.group(0)), lang="ru") + " "
        except Exception:
            return m.group(0)
    return _DIGITS_RE.sub(_sub, text)


# Фонетическая транслитерация Latin → Cyrillic. Silero обучен только на русских
# буквах; латиницу либо игнорирует, либо коверкает. Это приблизительная
# фонетика, но «TestUser» прочитается как «тестусер», что узнаваемо.
_LATIN_DIGRAPHS = [
    ("sch", "щ"), ("tch", "ч"), ("sh",  "ш"), ("ch",  "ч"), ("th",  "т"),
    ("ph",  "ф"), ("ck",  "к"), ("kh",  "х"), ("zh",  "ж"), ("yu",  "ю"),
    ("ya",  "я"), ("yo",  "ё"), ("ye",  "е"), ("ai",  "ай"), ("ei",  "ей"),
    ("oo",  "у"), ("ee",  "и"), ("ea",  "и"), ("ou",  "ау"), ("ow",  "ау"),
    ("ts",  "ц"), ("ks",  "кс"),
]
_LATIN_SINGLES = {
    "a":"а","b":"б","c":"к","d":"д","e":"е","f":"ф","g":"г","h":"х",
    "i":"и","j":"дж","k":"к","l":"л","m":"м","n":"н","o":"о","p":"п",
    "q":"к","r":"р","s":"с","t":"т","u":"у","v":"в","w":"в","x":"кс",
    "y":"й","z":"з",
}


def _translit_latin(text):
    """Подменяем подряд идущие латинские буквы кириллицей пофонетически."""
    def _convert(run):
        w = run.lower()
        for src, dst in _LATIN_DIGRAPHS:
            w = w.replace(src, dst)
        return "".join(_LATIN_SINGLES.get(c, c) for c in w)
    return _LATIN_RUN_RE.sub(lambda m: _convert(m.group(0)), text)


def _clean(text):
    if not text:
        return ""
    text = _URL_RE.sub("", text)
    text = _digits_to_words(text)
    text = _translit_latin(text)
    text = _PRINTABLE_RE.sub(" ", text)
    text = _WS_RE.sub(" ", text).strip()
    if len(text) > _MAX_CHARS:
        text = text[:_MAX_CHARS].rsplit(" ", 1)[0] + "…"
    # Должны остаться хоть какие-то буквы, иначе Silero ломается.
    if not re.search(r"\w", text):
        return ""
    return text


def _tensor_to_wav(audio_tensor):
    """Silero возвращает float32 примерно в [-1, 1]; превращаем в 16-bit PCM WAV.
    Если пики выходят за 1.0 — нормализуем к 0.95, иначе int16-клиппинг даёт
    металлический «хруст» на громких слогах."""
    import numpy as np
    arr = audio_tensor.numpy().astype(np.float32)
    peak = float(np.max(np.abs(arr))) if arr.size else 0.0
    if peak > 0.95:
        arr = arr * (0.95 / peak)
    pcm = (arr * 32767.0).clip(-32768, 32767).astype(np.int16)
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(_SAMPLE_RATE)
        wf.writeframes(pcm.tobytes())
    return buf.getvalue()


# ---------- Загрузка модели ----------

def _load_model():
    global _model
    try:
        import torch  # импорт тут, чтобы при отсутствии torch остальной бот стартовал
    except ImportError as e:
        _log(f"(tts) torch не установлен ({e}) — TTS выключен. pip install torch")
        return
    try:
        _log("(tts) загружаю Silero v4_ru (первый запуск может качать ~60МБ)...")
        model, _ = torch.hub.load(
            repo_or_dir="snakers4/silero-models",
            model="silero_tts",
            language="ru",
            speaker="v4_ru",
            trust_repo=True,
        )
        model.to(torch.device("cpu"))
        _model = model
        _model_ready.set()
        _log(f"(tts) Silero готов, голос={_SPEAKER}")
    except Exception as e:
        _log(f"(tts) не удалось загрузить Silero: {e}")


# ---------- Воркер синтеза ----------

def _worker():
    _model_ready.wait()
    if _model is None:
        return
    while True:
        audio_id, text, source, donation_id, speaker = _jobs.get()
        # busy выставляем ДЛЯ ЛЮБОГО джоба, не только чатового: пока играет донат,
        # чат-озвучка тоже должна быть подавлена, иначе сообщения, пришедшие во
        # время донат-фразы, накопятся в очереди и проиграются подряд после.
        _set_busy_until(time.monotonic() + _SYNTH_SAFETY_SEC)
        try:
            # put_accent — автоматическая расстановка ударений по словарю Silero;
            # put_yo — замена «е» на «ё» где нужно. Оба заметно улучшают чистоту
            # произношения и убирают «зернистость» на сложных словах.
            # SSML-обёртка с prosody rate="90%": Silero тянет темп через DSP без
            # сдвига питча (в отличие от browser-side playbackRate). Экранируем
            # &<> — после _clean остаются только буквы/цифры/пунктуация, но на
            # всякий случай.
            ssml_text = (
                text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            )
            audio = _model.apply_tts(
                ssml_text=f'<speak><prosody rate="90%">{ssml_text}</prosody></speak>',
                speaker=speaker or _SPEAKER, sample_rate=_SAMPLE_RATE,
                put_accent=True, put_yo=True,
            )
            wav = _tensor_to_wav(audio)
        except Exception as e:
            # Снимаем «занят», иначе чат заглохнет на _SYNTH_SAFETY_SEC.
            _set_busy_until(0.0)
            _log(f"(tts) синтез упал ({e!r}); пропуск: {text[:50]!r}")
            # Сообщаем оверлею что для этого доната озвучки не будет —
            # иначе модалка зависнет в ожидании.
            if donation_id:
                donatty_bus.publish({
                    "type": "tts_failed", "donation_id": donation_id,
                })
            continue
        wav_b64 = base64.b64encode(wav).decode("ascii")
        # Точная длительность аудио — прямо из тензора Silero (samples / SR),
        # не зависит от формата WAV-заголовка.
        duration = float(audio.shape[0]) / _SAMPLE_RATE
        _set_busy_until(time.monotonic() + duration + _PLAYBACK_BUFFER_SEC)
        # Едем по donatty_bus вместе с обычными donation-событиями. WAV инлайнен
        # base64 — клиент не делает отдельный HTTP-запрос, обходим лимит коннектов.
        donatty_bus.publish({
            "type": "tts",
            "id": audio_id,
            "donation_id": donation_id,    # связь с конкретным донатом для модалки
            "source": source,
            "text": text,
            "wav_b64": wav_b64,
        })
        _log(f"(tts) озвучено {len(wav)} байт: {text[:50]!r}")


# ---------- Публичное API ----------

def enqueue(text, source="generic", donation_id="", speaker=""):
    """Поставить текст в очередь синтеза. Если модель ещё грузится — задание подождёт.
    donation_id — опциональная корреляция с событием donation (модалка ждёт это аудио).
    speaker — переопределить голос Silero (по умолчанию _SPEAKER)."""
    global _busy_until
    cleaned = _clean(text)
    if not cleaned:
        return None
    is_chat = source in _CHAT_SOURCES
    # Короткие чат-реплики пропускаем — на них fade-in/out съедает почти всё,
    # остаётся щелчок. Донаты прозвучат целиком вне зависимости от длины.
    if is_chat and len(cleaned) < _CHAT_MIN_CHARS:
        return None
    # Режим «озвучка всего чата» использует отдельный голос, чтобы на слух
    # отличаться от короля и донат-сообщений.
    if not speaker and source == "chat-all":
        speaker = _SPEAKER_CHAT
    audio_id = uuid.uuid4().hex
    queued = False
    # Атомарно: проверяем «занято?», ставим в очередь, резервируем busy. Без
    # одной блокировки два чат-сообщения, пришедшие в один тик, оба видели бы
    # _busy_until=0 и оба попали бы в очередь — второе сыграло бы сразу после
    # первого, нарушив «один за раз». Реальную длительность позже впишет воркер.
    with _busy_lock:
        if is_chat and time.monotonic() < _busy_until:
            return None
        try:
            _jobs.put_nowait((audio_id, cleaned, source, donation_id, speaker))
            queued = True
            if is_chat:
                _busy_until = time.monotonic() + _SYNTH_SAFETY_SEC
        except _queue.Full:
            pass
    if not queued:
        _log("(tts) очередь переполнена, сбрасываю задание")
        # Если задание дропнуто — модалка иначе будет ждать аудио бесконечно.
        if donation_id:
            donatty_bus.publish({"type": "tts_failed", "donation_id": donation_id})
        return None
    return audio_id


def start(log=print):
    """Запустить фоновую загрузку модели и воркер. Зовётся один раз из bot.py."""
    global _log
    _log = log
    threading.Thread(target=_load_model, daemon=True, name="tts-loader").start()
    threading.Thread(target=_worker,     daemon=True, name="tts-worker").start()
