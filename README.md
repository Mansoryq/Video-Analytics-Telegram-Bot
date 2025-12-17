# Video Analytics Telegram Bot

Telegram-бот для аналитики видео-контента на **русском языке**.
Преобразует естественный язык → **SQL-запросы к PostgreSQL** → возвращает **одно число**.

## Возможности

- Локальная работа: **без OpenAI, без облака, без подписок**
- Локальная LLM: **qwen2:1.5b через Ollama**
- Аналитика по:
  - дате публикации
  - креатору
  - просмотрам / лайкам / комментариям / репортам
  - динамике (дельты по снапшотам)
- Ответ **строго одно целое число**

---

## Архитектура

→ Telegram

→ Aiogram 3

→ Ollama (qwen2:1.5b)

→ SQL (SELECT only)

→ Validation

→ PostgreSQL

→ number

→ Telegram


- LLM используется **только** для генерации SQL
- Жёсткий промпт + whitelist SQL
- Запрещены: `DROP`, `DELETE`, `UPDATE`, `UNION`
- Разрешены только `SELECT` к нужным таблицам

---

## Требования

- Python **3.9+**
- PostgreSQL
- Ollama

---

## Быстрый старт (локально)

### 1. Клонировать репозиторий и установить зависимости

```
python -m venv venv
source venv/bin/activate     
pip install -r requirements.txt
```


requirements.txt**:**
```
aiogram>=3.0.0
psycopg2-binary
python-dotenv
aiohttp
```

### **2. Настроить базу данных**

Создайте БД:
```
CREATE DATABASE video_analytics;
```

Примените схему **schema.sql**:
```
CREATE TABLE videos (
    id TEXT PRIMARY KEY,
    creator_id TEXT NOT NULL,
    video_created_at TIMESTAMPTZ NOT NULL,
    views_count BIGINT,
    likes_count BIGINT,
    comments_count BIGINT,
    reports_count BIGINT
);

CREATE TABLE video_snapshots (
    id TEXT PRIMARY KEY,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    views_count BIGINT,
    likes_count BIGINT,
    comments_count BIGINT,
    reports_count BIGINT,
    delta_views_count BIGINT,
    delta_likes_count BIGINT,
    delta_comments_count BIGINT,
    delta_reports_count BIGINT,
    created_at TIMESTAMPTZ NOT NULL
);
```

Положите **videos.json** в корень проекта и загрузите данные:

python load_data.py


### **3. Запустить Ollama**

ollama pull qwen2:1.5b


### **4. Настроить** ****

### **.env**

Создайте файл **.env**:
```
TELEGRAM_BOT_TOKEN=123456789:AAFd...
DATABASE_URL=postgresql://user@localhost:5432/video_analytics
```
> Токен получить у **@BotFather**



### **5. Запуск бота**
```
python bot.py
```

## **Примеры запросов**

* Сколько всего видео есть в системе?
* Сколько видео набрало больше 100000 просмотров?
* Сколько видео у креатора aca106... вышло с 1 ноября 2025 по 5 ноября 2025?
* На сколько просмотров выросли все видео 26 ноября 2025?
* Сколько разных видео получали новые просмотры 27 ноября 2025?

Ответ всегда — **одно целое число**


## **Структура проекта**
```
./
├── .env
├── videos.json
├── schema.sql
├── requirements.txt
├── load_data.py
└── bot.py
```
