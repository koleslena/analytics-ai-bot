import asyncio
import logging

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command

from config import BOT_TOKEN, DB_CONFIG, SCHEMA_PATH
from analytics_db import AsyncAnalyticsDB
from analytics_service import AsyncAnalyticsService

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Инициализация объектов
db = AsyncAnalyticsDB(DB_CONFIG, schema_path=SCHEMA_PATH)
analytics = AsyncAnalyticsService(db)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("Задавайте вопросы по статистике видео на русском языке!")

@dp.message(F.text)
async def handle_question(message: types.Message):
    # Отправляем статус "печатает...", пока LLM думает
    await bot.send_chat_action(chat_id=message.chat.id, action="typing")
    
    try:
        answer = await analytics.ask(message.text)
        await message.reply(f"{answer}", parse_mode="Markdown")
    except Exception as e:
        logging.error(f"Error handling question: {e}")
        await message.reply("Произошла ошибка. Попробуйте уточнить вопрос.")

async def main():
    # Открываем соединения при старте
    await db.connect()
    try:
        print("Бот запущен...")
        await dp.start_polling(bot)
    finally:
        # Закрываем пул при выключении
        await db.close()

if __name__ == "__main__":
    asyncio.run(main())

