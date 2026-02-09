import pytest

from analytics_service import AsyncAnalyticsService

@pytest.fixture
def analytics_service():
    # Передаем None вместо БД, так как тестируем только генерацию SQL
    return AsyncAnalyticsService(db=None)

async def test_sql_generation_total_videos(analytics_service):
    question = "Сколько всего видео есть в системе?"
    sql = await analytics_service.generate_sql(question)
    
    assert "SELECT" in sql.upper()
    assert "COUNT" in sql.upper()
    assert "videos" in sql.lower()
    # Убеждаемся, что не полезла в снапшоты, когда нужен общий итог
    assert "snapshots" not in sql.lower()

async def test_sql_generation_creator_period(analytics_service):
    question = "Сколько видео у креатора с id user_123 вышло с 1 по 5 ноября 2025?"
    sql = await analytics_service.generate_sql(question)
    
    assert "creator_id" in sql.lower()
    assert "user_123" in sql
    assert "BETWEEN" in sql.upper() or (">=" in sql and "<=" in sql)
    assert "2025-11-01" in sql
    assert "2025-11-05" in sql

async def test_sql_generation_growth_metrics(analytics_service):
    question = "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
    sql = await analytics_service.generate_sql(question)
    
    # Ключевой момент: должен быть SUM по дельтам из снапшотов
    assert "SUM" in sql.upper()
    assert "delta_views_count" in sql.lower()
    assert "video_snapshots" in sql.lower()
    assert "2025-11-28" in sql

async def test_sql_generation_unique_videos(analytics_service):
    question = "Сколько разных видео получали новые просмотры 27 ноября 2025?"
    sql = await analytics_service.generate_sql(question)
    
    assert "DISTINCT" in sql.upper()
    assert "video_id" in sql.lower()
    assert "delta_views_count" in sql.lower()
    assert "2025-11-27" in sql
