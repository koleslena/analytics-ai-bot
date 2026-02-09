from openai import AsyncOpenAI
import logging

from analytics_db import AsyncAnalyticsDB
from config import OPENAI_API_KEY, PROMT_PATH, OPENAI_BASE_URL, MODEL_NAME

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncAnalyticsService:
    def __init__(self, db: AsyncAnalyticsDB):
        self.db = db
        with open(PROMT_PATH, 'r', encoding='utf-8') as f:
            self.system_prompt = f.read()
        
        if OPENAI_BASE_URL:
            self.llm = AsyncOpenAI(
                api_key=OPENAI_API_KEY,
                base_url=OPENAI_BASE_URL
            )
        else:
            self.llm = AsyncOpenAI(api_key=OPENAI_API_KEY)

    async def generate_sql(self, question):
        """
        Отправляет вопрос в LLM и возвращает очищенный SQL-запрос.
        Вынесено в отдельный метод для тестирования.
        """
        response = await self.llm.chat.completions.create(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": self.system_prompt},
                {"role": "user", "content": question}
            ],
            temperature=0
        )
        
        # Обработка ответа (учитываем, что может прийти объект или строка)
        if isinstance(response, str):
            sql_query = response.strip()
        else:
            sql_query = response.choices[0].message.content.strip()
        
        # Очистка от Markdown и лишних символов
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        return sql_query

    async def ask(self, question: str):
        # 1. Генерируем SQL через LLM
        sql_query = await self.generate_sql(question)
        # 2. Выполняем в асинхронной БД
        return await self.db.execute_read(sql_query)


