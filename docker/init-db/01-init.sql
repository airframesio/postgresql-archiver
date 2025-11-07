-- Data Archiver Development Database Initialization
-- Creates sample partitioned tables with test data for archiving

-- Create sample events table with daily partitions
CREATE TABLE IF NOT EXISTS events (
    id BIGSERIAL,
    event_type VARCHAR(50) NOT NULL,
    user_id BIGINT,
    session_id UUID,
    data JSONB,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Create indexes on the parent table
CREATE INDEX IF NOT EXISTS idx_events_created_at ON events (created_at);
CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type);
CREATE INDEX IF NOT EXISTS idx_events_user_id ON events (user_id);
CREATE INDEX IF NOT EXISTS idx_events_data_gin ON events USING GIN (data);

-- Create partitions for January 2024 (one per day)
DO $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    start_date DATE := '2024-01-01';
    end_date DATE := '2024-02-01';
BEGIN
    partition_date := start_date;

    WHILE partition_date < end_date LOOP
        partition_name := 'events_' || TO_CHAR(partition_date, 'YYYY_MM_DD');

        -- Create partition
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF events
            FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            partition_date,
            partition_date + INTERVAL '1 day'
        );

        RAISE NOTICE 'Created partition: %', partition_name;
        partition_date := partition_date + INTERVAL '1 day';
    END LOOP;
END $$;

-- Generate sample data for January 2024
-- This creates realistic event data across all partitions
DO $$
DECLARE
    event_types TEXT[] := ARRAY['page_view', 'click', 'purchase', 'signup', 'login', 'logout', 'search', 'download'];
    current_date TIMESTAMP;
    event_count INTEGER;
    total_events INTEGER := 0;
BEGIN
    -- Seed random generator for reproducible data
    PERFORM setseed(0.5);

    -- Generate events for each day in January 2024
    FOR i IN 0..30 LOOP
        current_date := '2024-01-01'::TIMESTAMP + (i || ' days')::INTERVAL;

        -- Generate between 100-500 events per day
        event_count := 100 + floor(random() * 400)::INTEGER;

        FOR j IN 1..event_count LOOP
            INSERT INTO events (event_type, user_id, session_id, data, created_at)
            VALUES (
                event_types[1 + floor(random() * array_length(event_types, 1))::INTEGER],
                floor(random() * 10000)::BIGINT,
                gen_random_uuid(),
                jsonb_build_object(
                    'page', '/page/' || floor(random() * 100)::TEXT,
                    'duration_ms', floor(random() * 60000)::INTEGER,
                    'browser', CASE floor(random() * 4)::INTEGER
                        WHEN 0 THEN 'Chrome'
                        WHEN 1 THEN 'Firefox'
                        WHEN 2 THEN 'Safari'
                        ELSE 'Edge'
                    END,
                    'country', CASE floor(random() * 5)::INTEGER
                        WHEN 0 THEN 'US'
                        WHEN 1 THEN 'UK'
                        WHEN 2 THEN 'DE'
                        WHEN 3 THEN 'FR'
                        ELSE 'JP'
                    END,
                    'referrer', CASE floor(random() * 3)::INTEGER
                        WHEN 0 THEN 'google'
                        WHEN 1 THEN 'facebook'
                        ELSE 'direct'
                    END
                ),
                current_date + (random() * INTERVAL '24 hours')
            );
        END LOOP;

        total_events := total_events + event_count;
        RAISE NOTICE 'Generated % events for %', event_count, current_date::DATE;
    END LOOP;

    RAISE NOTICE 'Total events generated: %', total_events;
END $$;

-- Create a summary view for easy data exploration
CREATE OR REPLACE VIEW events_summary AS
SELECT
    DATE(created_at) as event_date,
    event_type,
    COUNT(*) as event_count,
    COUNT(DISTINCT user_id) as unique_users,
    COUNT(DISTINCT session_id) as unique_sessions
FROM events
GROUP BY DATE(created_at), event_type
ORDER BY event_date, event_type;

-- Create a partition summary view
CREATE OR REPLACE VIEW partition_summary AS
SELECT
    schemaname,
    tablename as partition_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
    (SELECT COUNT(*)
     FROM pg_class c
     JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relname = tablename
     AND n.nspname = schemaname) as exists
FROM pg_tables
WHERE tablename LIKE 'events_%'
ORDER BY tablename;

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO archiver;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO archiver;

-- Display summary
DO $$
DECLARE
    total_count BIGINT;
    partition_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO total_count FROM events;
    SELECT COUNT(*) INTO partition_count FROM pg_tables WHERE tablename LIKE 'events_%';

    RAISE NOTICE '';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Database Initialization Complete!';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Total events: %', total_count;
    RAISE NOTICE 'Total partitions: %', partition_count;
    RAISE NOTICE '';
    RAISE NOTICE 'Useful queries:';
    RAISE NOTICE '  SELECT * FROM events_summary;';
    RAISE NOTICE '  SELECT * FROM partition_summary;';
    RAISE NOTICE '  SELECT COUNT(*) FROM events;';
    RAISE NOTICE '';
    RAISE NOTICE 'Sample archiver command:';
    RAISE NOTICE '  data-archiver \';
    RAISE NOTICE '    --table events \';
    RAISE NOTICE '    --start-date 2024-01-01 \';
    RAISE NOTICE '    --end-date 2024-01-31 \';
    RAISE NOTICE '    --workers 2';
    RAISE NOTICE '========================================';
END $$;
