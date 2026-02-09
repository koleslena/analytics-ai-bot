import json
import httpx
import logging
from config import JSON_URL

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


async def _download_file() -> list:
    """Скачивает JSON файл."""
    
    async with httpx.AsyncClient(follow_redirects=True) as client:

        response = await client.get(JSON_URL)
        content_type = response.headers.get("Content-Type", "")

        if response.status_code == 200:
            logger.info("Файл успешно скачан.")
            if "application/json" in content_type:
                return response.json()
            else:
                try:
                    # Вместо response.json() используем ручной парсинг текста
                    data = json.loads(response.text)
                    logger.info(f"Файл успешно скачан и распарсен (тип: {content_type}).")
                    return data
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка парсинга JSON: {e}")
                    # Выведем кусок текста, чтобы понять, что там внутри
                    logger.debug(f"Начало данных: {response.text[:100]}")
                    return []
        else:
            logger.error(f"Ошибка скачивания: {response.status_code}")
            return []

async def _insert_data(items, cur):
    """Процесс вставки"""

    for item in items:
        # Вставка видео
        await cur.execute("""
            INSERT INTO videos (
                id, creator_id, video_created_at, 
                views_count, likes_count, reports_count, comments_count,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                views_count = EXCLUDED.views_count,
                likes_count = EXCLUDED.likes_count,
                updated_at = EXCLUDED.updated_at;
        """, (
            item['id'], item['creator_id'], item['video_created_at'],
            item['views_count'], item['likes_count'], item['reports_count'], item['comments_count'],
            item['created_at'], item['updated_at']
        ))

        # Вставка снапшотов
        snapshots = item.get('snapshots', [])
        for snp in snapshots:
            await cur.execute("""
                INSERT INTO video_snapshots (
                    id, video_id, views_count, likes_count, reports_count, comments_count,
                    delta_views_count, delta_likes_count, delta_reports_count, delta_comments_count,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                snp['id'], snp['video_id'], snp['views_count'], 
                snp['likes_count'], snp['reports_count'], snp['comments_count'],
                snp['delta_views_count'], snp['delta_likes_count'], 
                snp['delta_reports_count'], snp['delta_comments_count'],
                snp['created_at'], snp['updated_at']
            ))

    logger.info(f"Будет добавлено видео: {len(items)}")


async def load_json_to_db(conn):
    try:        
        async with conn.cursor() as cur:
            # Проверяем, есть ли хотя бы одна запись в таблице videos 
            await cur.execute("SELECT EXISTS (SELECT 1 FROM videos LIMIT 1);")
            exists = (await cur.fetchone())[0]

            if exists:
                logger.info("В базе данных уже есть записи. Загрузка из JSON пропущена.")
                return 

            logger.info(f"База данных пуста. Начинаем загрузку из json...")

            # Читаем файл только если база пуста
            data = await _download_file()

            if data and 'videos' in data:
                await _insert_data(data['videos'], cur)
            
            await conn.commit()
            logger.info(f"Первичная загрузка завершена.")

    except Exception as e:
        logger.error(f"Ошибка при загрузке JSON: {e}")
        raise

