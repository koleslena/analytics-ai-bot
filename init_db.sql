-- Скрипт инициализации БД аналитики видео

BEGIN;

-- Очистка (опционально, если нужно пересоздать с нуля)
-- DROP TABLE IF EXISTS video_snapshots;
-- DROP TABLE IF EXISTS videos;

--  Функция для авто-обновления поля updated_at
CREATE OR REPLACE FUNCTION update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Таблица VIDEOS (Итоговая статистика)
-- Используем TEXT для ID, так как в JSON приходят строки типа cace1e34...
CREATE TABLE IF NOT EXISTS videos (
    id TEXT PRIMARY KEY, 
    creator_id TEXT NOT NULL,
    video_created_at TIMESTAMPTZ NOT NULL,
    
    -- Метрики
    views_count BIGINT DEFAULT 0,
    likes_count BIGINT DEFAULT 0,
    comments_count BIGINT DEFAULT 0,
    reports_count BIGINT DEFAULT 0,
    
    -- Служебные поля
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Таблица VIDEO_SNAPSHOTS (Почасовые замеры)
CREATE TABLE IF NOT EXISTS video_snapshots (
    id TEXT PRIMARY KEY,
    video_id TEXT NOT NULL REFERENCES videos(id) ON DELETE CASCADE,
    
    -- Текущие значения
    views_count BIGINT DEFAULT 0,
    likes_count BIGINT DEFAULT 0,
    comments_count BIGINT DEFAULT 0,
    reports_count BIGINT DEFAULT 0,
    
    -- Приращения (дельты)
    delta_views_count INTEGER DEFAULT 0,
    delta_likes_count INTEGER DEFAULT 0,
    delta_comments_count INTEGER DEFAULT 0,
    delta_reports_count INTEGER DEFAULT 0,
    
    -- Время замера
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- ИНДЕКСЫ (важны для Text-to-SQL)
CREATE INDEX IF NOT EXISTS idx_videos_creator ON videos(creator_id);
CREATE INDEX IF NOT EXISTS idx_videos_created_date ON videos(video_created_at);
CREATE INDEX IF NOT EXISTS idx_snapshots_video_id ON video_snapshots(video_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_created_at ON video_snapshots(created_at);

-- ТРИГГЕРЫ
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_update_videos_modtime') THEN
        CREATE TRIGGER trg_update_videos_modtime
            BEFORE UPDATE ON videos
            FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_update_snapshots_modtime') THEN
        CREATE TRIGGER trg_update_snapshots_modtime
            BEFORE UPDATE ON video_snapshots
            FOR EACH ROW EXECUTE PROCEDURE update_modified_column();
    END IF;
END $$;

-- Настраиваем пользователя базы данных на работу в UTC по умолчанию
ALTER ROLE CURRENT_USER SET TIMEZONE TO 'UTC';

COMMIT;
