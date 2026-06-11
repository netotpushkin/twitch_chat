"""Общая «безопасная» очистка текста для модерации и TTS.

Лёгкий слой без потери смысла: снять обфускацию (zero-width, точки) и ссылки,
посчитать «полезную» длину. Лоссовые преобразования под конкретного потребителя
(TTS: цифры→слова, транслит) остаются в самом потребителе.
"""

import re

# Захватываем URL двумя ветками:
#   а) явная схема http:// / https:// / www.
#   б) host-вида X.Y[.Z…].TLD из ограниченного белого списка TLD, с обязательным
#      слэш-путём — иначе "node.js", "vue.js", "app.py" и т.п. ложно срабатывают.
_TLD = (
    r"com|net|org|io|co|ru|ua|by|kz|tv|gg|tk|ml|ga|cf|xyz|info|biz|me|cc|de|"
    r"uk|us|app|dev|site|online|store|shop|club|live|stream|link|page|pro|fm"
)
URL_RE = re.compile(
    r"(?i)(?:https?://|www\.)\S+"
    r"|(?<![\w@])[\w-]+(?:\.[\w-]+)*\.(?:" + _TLD + r")\b(?:/\S*)?",
)

# Невидимые юникод-символы, которыми ломают регексы: ZWSP, ZWNJ, ZWJ, WJ, BOM.
_ZERO_WIDTH_RE = re.compile("[​‌‍⁠﻿]")
# Простые приёмы обфускации точки: " . ", "[.]", "(.)" → "."
_DOT_OBFUSCATE_RE = re.compile(r"\s*[\[\(]\s*\.\s*[\]\)]\s*|\s+\.\s+|\s+\.\s*(?=[a-zA-Zа-яА-Я])")


def normalize(text):
    """Снимает обфускацию для устойчивого детекта/модерации: убирает невидимые
    символы (zero-width) и простую обфускацию точек («x [.] com» → «x.com»).
    Смысл/слова сохраняет — годится как ввод для LLM."""
    text = _ZERO_WIDTH_RE.sub("", text)
    text = _DOT_OBFUSCATE_RE.sub(".", text)
    return text


def strip_urls(text, repl=" "):
    """Убирает из текста все ссылки и схлопывает лишние пробелы."""
    return " ".join(URL_RE.sub(repl, text).split())


def meaningful_len(text):
    """Число «полезных» символов (буквы/цифры). Пунктуация, пробелы, эмодзи и
    невидимые знаки не считаются — чтобы набивка не накручивала длину."""
    return sum(1 for ch in text if ch.isalnum())
