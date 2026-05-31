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
import queue as _queue
import uuid
import wave

from config import TTS_SPEAKER
from events import donatty_bus


# ---------- Параметры синтеза ----------

_SPEAKER       = TTS_SPEAKER   # из config / env TTS_SPEAKER, см. config.py
# 48 кГц — стандартная частота браузерного AudioContext. Если отдадим 24 кГц,
# CEF в OBS будет апсемплить → металлический звон на согласных. На 48 кГц
# ресэмплинга в браузере нет; Silero справляется с внутренним апсемплингом
# лучше, чем дефолтный аудиостек Chromium.
_SAMPLE_RATE   = 48000
_MAX_CHARS     = 200       # длиннее обрезаем


# ---------- Состояние модуля ----------

_model = None
_model_ready = threading.Event()
# (audio_id, text, source, donation_id) — donation_id связывает аудио с донатом для оверлея
_jobs: "_queue.Queue[tuple[str, str, str, str]]" = _queue.Queue(maxsize=64)
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
        audio_id, text, source, donation_id = _jobs.get()
        try:
            # put_accent — автоматическая расстановка ударений по словарю Silero;
            # put_yo — замена «е» на «ё» где нужно. Оба заметно улучшают чистоту
            # произношения и убирают «зернистость» на сложных словах.
            audio = _model.apply_tts(
                text=text, speaker=_SPEAKER, sample_rate=_SAMPLE_RATE,
                put_accent=True, put_yo=True,
            )
            wav = _tensor_to_wav(audio)
        except Exception as e:
            _log(f"(tts) синтез упал ({e!r}); пропуск: {text[:50]!r}")
            # Сообщаем оверлею что для этого доната озвучки не будет —
            # иначе модалка зависнет в ожидании.
            if donation_id:
                donatty_bus.publish({
                    "type": "tts_failed", "donation_id": donation_id,
                })
            continue
        wav_b64 = base64.b64encode(wav).decode("ascii")
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

def enqueue(text, source="generic", donation_id=""):
    """Поставить текст в очередь синтеза. Если модель ещё грузится — задание подождёт.
    donation_id — опциональная корреляция с событием donation (модалка ждёт это аудио)."""
    cleaned = _clean(text)
    if not cleaned:
        return None
    audio_id = uuid.uuid4().hex
    try:
        _jobs.put_nowait((audio_id, cleaned, source, donation_id))
    except _queue.Full:
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
