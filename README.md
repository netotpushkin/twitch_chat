# twitch_chat

Самописный чат-бот для Twitch с набором интеграций: модерация через LLM, авто-подбор заголовка и тегов стрима, экономика канала (монеты + топ), очередь YouTube-клипов с голосованием, бросок кубика «гейммастер», TTS донатов и «короля доната», приём донатов через Donatty, EventSub-алерты и набор HTML-оверлеев для OBS.

Бот пишется и тестируется под собственный канал — он намеренно простой, без БД и веб-админки. Состояние держится в файлах в `state/`, конфиг — в `.env`.

---

## Возможности

- **IRC-клиент** с keepalive, авто-реконнектом, локальным эхо и rate-limit под лимиты Twitch (20/30s для обычных, 100/30s для мода/VIP/стримера).
- **Команды чата** (`commands.py`):
  - `!ютуб <url>` — добавить YouTube-ролик в очередь (1–6 мин, 10000+ просмотров, не Shorts, не трансляция);
  - `!-` / `!+` — голосование за скип/оставить текущий ролик;
  - `!скип` — моментальный скип (только моды/стример);
  - `!кубик <вопрос>` — бросок д20 с ответом «гейммастера» от LLM;
  - `!монеты` — баланс (моды могут смотреть чужой: `!монеты @ник`);
  - `!топ` — топ-5 по балансу;
  - `!дать @ник N` — перевод монет;
  - `!заголовок` — форсировать авто-подбор заголовка и тегов (только моды/стример).
- **Модерация** (`moderation.py`): детерминированные предфильтры + LLM-классификатор через OpenRouter. Есть `MODERATION_DRY_RUN=1` для обкатки.
- **Автотитлер** (`titler.py`): периодически смотрит на чат и через LLM предлагает новый заголовок и теги, выставляет через Helix.
- **Экономика** (`economy.py`): +монеты за сообщения (с кулдауном) и за watchtime (тикер по `/chat/chatters`). Хранится в SQLite `state/coins.db`.
- **YouTube-очередь** (`youtube.py`): проверка ролика через oEmbed/scrape, голосование за скип с окном `YT_VOTE_WINDOW`, фоновое выполнение в пуле.
- **TTS** (`tts.py`): Silero v4_ru (`aidar / baya / kseniya / xenia / eugene / random`), очередь, воспроизведение в `donatty.html`.
- **Donatty** (`donatty.py`): подключение к виджету по SSE; донаты идут в TTS и в оверлей.
- **EventSub** (`eventsub.py`): фолловы, сабы, ресабы, гифт-паки, рейды → в оверлей алертов.
- **Король доната** (`king.py`): корона переходит к автору **последнего** ненулевого не-анонимного доната — каждое его сообщение в чате после этого озвучивается TTS. Состояние держится в памяти, на рестарте сбрасывается.
- **Сбор** (`goal.py`): инкрементальный мини-сбор. Стартует с 10 ₽, при достижении цель закрывается и сразу начинается следующая (+1 ₽), переплата сбрасывается в 0. Заголовок каждого сбора генерирует LLM. Состояние в `state/goal.json` переживает рестарт; в чат уходит анонс при старте новой цели. Оверлей — `goal.html` (источник `/goal`).
- **Дождь эмоутов** (`emote_bus`) + кубик-оверлей + веб-камера + индекс оверлеев.

---

## Структура

| Файл | Назначение |
|---|---|
| `bot.py` | Точка входа. IRC-цикл, диспатч команд, запуск всех потоков. |
| `config.py` | Загрузка `.env`, константы, `STATE_DIR`, `OVERLAY_TOKEN`. |
| `commands.py` | Канонический список команд бота. |
| `twitch_api.py` | Helix-обёртки, OAuth, бейджи/эмоуты, безопасный HTML. |
| `eventsub.py` | WebSocket EventSub: фолловы/сабы/рейды. |
| `moderation.py` | Предфильтры + LLM-модерация. |
| `titler.py` | Авто-заголовок и теги через LLM + Helix PATCH. |
| `economy.py` | Монеты: чат-награды, watchtime, переводы, топ. |
| `youtube.py` | Проверка ролика, очередь, голосование, скип. |
| `dice.py` | `!кубик` — д20 + ответ LLM. |
| `king.py` | Король доната (звания, TTS). |
| `goal.py` | Инкрементальный сбор: цель, LLM-заголовок, анонс новой цели в чат. |
| `tts.py` | Silero TTS, очередь воспроизведения. |
| `donatty.py` | SSE-клиент Donatty. |
| `openrouter.py` | Клиент OpenRouter (LLM-шлюз). |
| `overlay_server.py` | Локальный HTTP: SSE-стримы, статика, `/test/*`. |
| `events.py` | Внутренние шины (`chat_bus`, `events_bus`, `dice_bus`, ...). |
| `prompt.py` | Интерактивный ввод в терминале (для отправки в чат). |
| `log.py`, `http_pool.py` | Утилиты. |
| `overlays/*.html` | OBS Browser-source оверлеи (chat, alerts, dice, donatty, emote_rain, goal, tts, webcam, youtube). |
| `state/` | Рантайм-стейт (token.json, coins.db, overlay_token.txt, ...). Не в git. |

---

## Установка

Требования: Windows / Linux, Python 3.11+, OBS (для оверлеев).

```bash
pip install -r requirements.txt
```

> `torch` нужен для Silero TTS. Если TTS не нужен — можно временно вырезать импорт `tts` в `bot.py` и не ставить torch.

### Регистрация Twitch-приложения

1. Создай приложение: <https://dev.twitch.tv/console/apps>
2. **OAuth Redirect URL:** `http://localhost:3000`
3. Скопируй `Client ID`.

### `.env`

В корне создай `.env` (он в `.gitignore`):

```ini
# Twitch
TWITCH_CLIENT_ID=твой_client_id
TWITCH_CHANNEL=твой_канал

# OpenRouter (модерация, титлер, кубик) — https://openrouter.ai
OPENROUTER_API_KEY=sk-or-...
OPENROUTER_MODEL=openai/gpt-4o-mini

# Модерация: 1 — на удаление, DRY_RUN=1 — только логи
MODERATION_ENABLED=1
MODERATION_DRY_RUN=0

# Автотитлер
TITLER_ENABLED=1
TITLER_INTERVAL=600
TITLER_MIN_MESSAGES=10
# TITLER_REQUIRED_TAGS=Русский,18plus   # опционально

# TTS-голос Silero v4_ru (aidar/baya/kseniya/xenia/eugene/random)
TTS_SPEAKER=xenia
# Голос для режима «озвучка всего чата» (!озвучка) — отдельный, чтобы отличался от короля
TTS_SPEAKER_CHAT=eugene

# Donatty — оба значения из URL виджета донатов
# https://widgets.donatty.com/donations/?ref=<REF>&token=<TOKEN>
DONATTY_REF=
DONATTY_TOKEN=

# Опционально
# OVERLAY_PORT=5005
```

---

## Запуск

```bash
python bot.py
```

При первом запуске откроется браузер для OAuth (redirect `http://localhost:3000`). После успеха токен сохраняется в `state/token.json` — браузер больше не открывается.

В Windows есть `start.bat` — поднимает бота, ждёт оверлей-сервер на 5005 и запускает OBS.

В терминале будут напечатаны URL-ы оверлеев и тестовых эндпойнтов (`/test/follow`, `/test/sub`, `/test/donation`, `/test/yt/play`, ...). Они защищены `OVERLAY_TOKEN` (см. `state/overlay_token.txt`).

---

## OBS: какие оверлеи добавлять

Все оверлеи — Browser Source с URL вида `http://localhost:5005/<имя>.html`:

| URL | Что показывает |
|---|---|
| `/chat.html` | Чат с бейджами/эмоутами/«королём». |
| `/alerts.html` | Фолловы, сабы, ресабы, гифт-паки, рейды. |
| `/donatty.html` | Донаты + воспроизводит TTS (держать **одну** копию в OBS). |
| `/dice.html` | Анимация кубика. |
| `/youtube.html` | Плеер очереди YouTube. |
| `/emote_rain.html` | Дождь эмоутов/символов. |
| `/webcam.html` | Веб-камера. |
| `/` | Индекс всех оверлеев. |

> Если включён `OVERLAY_TOKEN`, к URL обычных оверлеев он подставляется автоматически при отдаче HTML. Тестовые `/test/*` требуют `?token=...` явно.

---

## Скоупы Twitch

```
chat:read chat:edit
moderator:read:followers moderator:read:chatters
moderator:manage:chat_messages
channel:read:subscriptions
channel:manage:broadcast
```

---

## State

`state/` (не в git):

- `token.json` — Twitch OAuth-токен и `user_id`;
- `overlay_token.txt` — токен для `/test/*`, переживает рестарт;
- `coins.db` — SQLite-база экономики (балансы, watchtime);
- `goal.json` — состояние «инкрементального сбора» (текущая цель, прогресс, LLM-заголовок);
- кэши бейджей и т. п.

Удалить `state/` = сбросить всё (включая логин).

---

## Известные ограничения

- Бот ходит в чат под аккаунтом стримера (один OAuth-токен). Для отдельного бот-аккаунта надо передавать его `user_id` в `moderation.setup`.
- Модерация и кубик стучат в OpenRouter — без `OPENROUTER_API_KEY` они тихо отключаются.
- TTS требует CPU/GPU под Silero (первый запуск качает модель).
- Только Windows-путь к OBS в `start.bat` — для других ОС просто запускай `python bot.py` руками.
