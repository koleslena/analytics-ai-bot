import psycopg
from psycopg_pool import AsyncConnectionPool
import logging

from loader import load_json_to_db

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

class AsyncAnalyticsDB:
    def __init__(self, db_config: dict, schema_path: str = None):
        self.schema_path = schema_path
        # Формируем строку подключения для пула
        self.conninfo = psycopg.conninfo.make_conninfo(**db_config)
        self.pool = None

    async def connect(self):
        """Инициализация пула соединений."""
        if not self.pool:
            self.pool = AsyncConnectionPool(self.conninfo, open=True)
            logger.info("Пул соединений с БД открыт.")
            if self.schema_path:
                await self._initialize_schema()
                await self._load_json()

    async def _load_json(self):
        """Асинхронная проверка и загрузка данных."""
        try:
            async with self.pool.connection() as conn:
                await load_json_to_db(conn)
                logging.info(f"Первичные данные загружены")
        except Exception as e:
            logger.error(f"Ошибка при загрузке JSON: {e}")
            raise
                             
    async def _initialize_schema(self):
        """Выполнение SQL-скрипта инициализации."""
        try:
            with open(self.schema_path, 'r', encoding='utf-8') as f:
                schema_sql = f.read()
            
            async with self.pool.connection() as conn:
                await conn.execute(schema_sql)
                logger.info("Схема БД успешно инициализирована.")
        except Exception as e:
            logger.error(f"Ошибка инициализации схемы: {e}")

    async def execute_read(self, query: str):
        """Безопасное выполнение запроса на чтение."""
        async with self.pool.connection() as conn:
            # Устанавливаем режим Read Only на уровне транзакции
            await conn.execute("SET TRANSACTION READ ONLY")
            async with conn.cursor() as cur:
                await cur.execute(query)
                result = await cur.fetchone()
                return result[0] if result else 0

    async def close(self):
        """Закрытие пула."""
        if self.pool:
            await self.pool.close()

