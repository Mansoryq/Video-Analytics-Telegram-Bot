import os
import re
import asyncio
import psycopg2
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
DB_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN не задан в .env")

PROMPT_PREFIX = """
Ты — аналитический SQL-ассистент. По запросу на русском языке сгенерируй ровно один корректный SQL-запрос к PostgreSQL, который возвращает **одно целое число**. Никаких пояснений, комментариев, markdown.

### Контекст
Игнорируй пояснения вроде «нужно сложить изменения» или «между замерами».
Если вопрос спрашивает о росте за период, всегда используй готовое поле delta_views_count из таблицы video_snapshots.

### Схема
Таблица videos:
- id (TEXT, UUID)
- creator_id (TEXT, UUID)
- video_created_at (TIMESTAMPTZ, UTC)
- views_count, likes_count, comments_count, reports_count (BIGINT)

Таблица video_snapshots:
- id (TEXT, UUID)
- video_id (TEXT, UUID → videos.id)
- views_count, likes_count, comments_count, reports_count (BIGINT на момент замера)
- delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (BIGINT — изменение за час)
- created_at (TIMESTAMPTZ, UTC)

### Правила
1. Используй ТОЛЬКО указанные колонки и таблицы.
2. ВСЕГДА фильтруй даты через **явный диапазон в UTC**:
   - Один день D: `col >= 'D 00:00:00+00' AND col < 'D+1 00:00:00+00'`
   - Диапазон A–B включительно: `col >= 'A 00:00:00+00' AND col < 'B+1 00:00:00+00'`
   - Время внутри дня: `col >= '2025-11-28 10:00:00+00' AND col < '2025-11-28 15:00:00+00'`
3. Месяцы: январь=01, …, июнь=06, …, декабрь=12.
4. UUID — в одинарных кавычках: `creator_id = '...'`
5. **КРИТИЧЕСКИ ВАЖНО**:
   - Если вопрос про **рост, прирост, изменение, дельта** → используй **только `delta_views_count`**.
   - **НИКОГДА не складывай `delta_views_count` с `views_count`**.
   - `views_count` — итоговое число, `delta_views_count` — изменение за час.
6. Для анализа замеров по креатору — **обязательно JOIN video_snapshots с videos**.
7. Если вопрос про дни публикации видео — работай с таблицей videos и полем video_created_at.
8. Если вопрос про дни замеров или динамику — используй video_snapshots и created_at.

### Примеры
Вопрос: Сколько всего видео?
Ответ: SELECT COUNT(*) FROM videos;

Вопрос: Сколько видео у креатора с id 8b76e572635b400c9052286a56176e03 вышло с 1 по 5 ноября 2025?
Ответ: SELECT COUNT(*) FROM videos WHERE creator_id = '8b76e572635b400c9052286a56176e03' AND video_created_at >= '2025-11-01 00:00:00+00' AND video_created_at < '2025-11-06 00:00:00+00';

Вопрос: Сколько замеров с отрицательным ростом просмотров?
Ответ: SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;

Вопрос: Суммарный рост просмотров всех видео 28 ноября 2025?
Ответ: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at >= '2025-11-28 00:00:00+00' AND created_at < '2025-11-29 00:00:00+00';

Вопрос: Сколько видео с >100000 просмотров?
Ответ: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: Суммарные просмотры видео, опубликованных в июне 2025?
Ответ: SELECT SUM(views_count) FROM videos WHERE video_created_at >= '2025-06-01 00:00:00+00' AND video_created_at < '2025-07-01 00:00:00+00';

Вопрос: На сколько просмотров выросли видео креатора cd87be38b50b4fdd8342bb3c383f3c7d с 10:00 до 15:00 28 ноября 2025?
Ответ: SELECT SUM(s.delta_views_count) FROM video_snapshots s JOIN videos v ON s.video_id = v.id WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d' AND s.created_at >= '2025-11-28 10:00:00+00' AND s.created_at < '2025-11-28 15:00:00+00';

Вопрос: На сколько просмотров суммарно выросли все видео креатора с id ... в промежутке с 10:00 до 15:00 ...? Нужно сложить изменения просмотров между замерами, попадающими в этот интервал.
Ответ: SELECT SUM(s.delta_views_count) FROM ...

Вопрос: Для креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 посчитай, в скольких разных календарных днях ноября 2025 года он публиковал хотя бы одно видео.
Ответ: SELECT COUNT(DISTINCT video_created_at::date) FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63' AND video_created_at >= '2025-11-01 00:00:00+00' AND video_created_at < '2025-12-01 00:00:00+00';

Теперь обработай запрос:
""".strip()

def get_db_connection():
    return psycopg2.connect(DB_URL)

def is_safe_sql(sql: str) -> bool:
    sql_clean = sql.strip().lower()
    if not sql_clean.startswith('select'):
        return False
    dangerous = ['--', '/*', 'drop', 'delete', 'insert', 'update', 'alter', 'exec', 'union', 'pragma']
    if any(d in sql_clean for d in dangerous):
        return False
    if not ('videos' in sql_clean or 'video_snapshots' in sql_clean):
        return False
    return True

async def text_to_sql(query: str) -> str:
    full_prompt = f"{PROMPT_PREFIX}\n\nВопрос: {query}\nОтвет:"
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "qwen2:1.5b",
                "prompt": full_prompt,
                "stream": False,
                "options": {
                    "temperature": 0.0,
                    "num_predict": 120,
                    "stop": ["\n\n", "Вопрос:", "Пример:"]
                }
            },
            timeout=aiohttp.ClientTimeout(total=30)
        ) as resp:
            if resp.status != 200:
                raise Exception(f"Ollama error: {await resp.text()}")
            data = await resp.json()
            raw = data.get("response", "").strip()
    
            for line in raw.splitlines():
                line = line.strip().strip("`")
                if line.lower().startswith("select"):
                    return line
            return raw.splitlines()[0].strip().strip("`") if raw else ""
def execute_sql(sql: str) -> int:
    if not is_safe_sql(sql):
        raise ValueError("Небезопасный SQL")
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            res = cur.fetchone()
            return int(res[0]) if res and res[0] is not None else 0
async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(Command("start"))
    async def start(message: types.Message):
        await message.answer("Привет! Задайте вопрос на русском. Пример: «Сколько видео набрало больше 100000 просмотров?»")

    @dp.message()
    async def handle(message: types.Message):
        if not message.text:
            return
        try:
            sql = await text_to_sql(message.text)
            print(f"[DEBUG] Вопрос: {message.text}")
            print(f"[DEBUG] SQL: {sql}")
            result = execute_sql(sql)
            await message.answer(str(result))
        except Exception as e:
            print(f"[ERROR] {e}")
            await message.answer("Не удалось обработать запрос.")

    print("Бот запущен! Убедитесь, что AI работает.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
