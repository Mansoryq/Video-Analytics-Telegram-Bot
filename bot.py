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
Ты — аналитический SQL-ассистент. Твоя задача — по запросу пользователя на русском языке сгенерировать ровно один SQL-запрос к PostgreSQL, который вернёт одно целое число (без пояснений, без форматирования, без markdown).
Схема базы данных:
Таблица videos:
- id (TEXT, UUID)
- creator_id (TEXT, UUID)
- video_created_at (TIMESTAMPTZ, всегда в UTC)
- views_count, likes_count, comments_count, reports_count (BIGINT)
Таблица video_snapshots:
- id (TEXT, UUID)
- video_id (TEXT, UUID, ссылка на videos.id)
- views_count, likes_count, comments_count, reports_count (BIGINT на момент замера)
- delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (BIGINT — прирост за час)
- created_at (TIMESTAMPTZ, всегда в UTC, время замера)
Правила:
1. Никогда не выдумывай колонки или таблицы. Используй ТОЛЬКО указанные выше.
2. Все даты в данных хранятся в UTC. НЕ ИСПОЛЬЗУЙ ::date — это вызывает ошибки из-за часовых поясов.
3. Всегда фильтруй по дате через явный диапазон в UTC:
   - Для одного дня «D» → column >= 'D 00:00:00+00' AND column < 'D+1 00:00:00+00'
   - Для диапазона «с A по B включительно» → column >= 'A 00:00:00+00' AND column < 'B+1 00:00:00+00'
4. Идентификаторы (UUID) заключай в одинарные кавычки: creator_id = '...'
5. Возвращай ТОЛЬКО SQL-запрос, начинающийся с SELECT. Никаких пояснений, комментариев, markdown.

Примеры:

Вопрос: Сколько всего видео есть в системе?
Ответ: SELECT COUNT(*) FROM videos;

Вопрос: Сколько видео у креатора с id 8b76e572635b400c9052286a56176e03 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?
Ответ: SELECT COUNT(*) FROM videos WHERE creator_id = '8b76e572635b400c9052286a56176e03' AND video_created_at >= '2025-11-01 00:00:00+00' AND video_created_at < '2025-11-06 00:00:00+00';

Вопрос: Сколько видео набрало больше 100000 просмотров за всё время?
Ответ: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: На сколько просмотров в сумме выросли все видео 28 ноября 2025?
Ответ: SELECT SUM(delta_views_count) FROM video_snapshots WHERE created_at >= '2025-11-28 00:00:00+00' AND created_at < '2025-11-29 00:00:00+00';

Вопрос: Сколько разных видео получали новые просмотры 27 ноября 2025?
Ответ: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE created_at >= '2025-11-27 00:00:00+00' AND created_at < '2025-11-28 00:00:00+00' AND delta_views_count > 0;

Теперь обработай следующий запрос пользователя:
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
            await message.answer("Не удалось обработать запрос. Попробуйте чётко сформулировать вопрос.")

    print("Бот запущен! Убедитесь, что Ollama работает.")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
