-- V015__partition_maintenance.sql
-- Обслуживание почасовых партиций message_lifecycle_history и dlr_correlation.
-- Не завязано на pg_partman (портируемость важнее — не все окружения будут
-- иметь это расширение установленным); функции ниже вызываются любым внешним
-- планировщиком (k8s CronJob, pg_cron, приложение) раз в час.
--
-- Партиция кодирует час в имени (YYYYMMDD_HH24), поэтому drop-функция
-- разбирает имя, а не partition bound expression — проще и надёжнее.

CREATE OR REPLACE FUNCTION messaging.create_lifecycle_history_partition(p_hour_start TIMESTAMPTZ)
RETURNS TEXT AS $$
DECLARE
    v_partition_name TEXT;
    v_hour_end TIMESTAMPTZ;
BEGIN
    v_hour_end := p_hour_start + INTERVAL '1 hour';
    v_partition_name := 'message_lifecycle_history_' || to_char(p_hour_start, 'YYYYMMDD_HH24');
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS messaging.%I PARTITION OF messaging.message_lifecycle_history
         FOR VALUES FROM (%L) TO (%L)',
        v_partition_name, p_hour_start, v_hour_end
    );
    RETURN v_partition_name;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION messaging.drop_old_lifecycle_history_partitions(p_retain_hours INT DEFAULT 72)
RETURNS INT AS $$
DECLARE
    r RECORD;
    v_cutoff TIMESTAMPTZ := date_trunc('hour', now()) - (p_retain_hours || ' hours')::interval;
    v_partition_hour TIMESTAMPTZ;
    v_dropped INT := 0;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        -- relkind IN ('r','p') — только сами таблицы/партиции, НЕ их индексы.
        -- Без этого фильтра LIKE-паттерн совпадает и с именами индексов
        -- партиции (например ..._pkey, ..._message_id_idx), цикл пытается
        -- DROP TABLE и по ним — большинство таких вызовов молча no-op'ают
        -- через IF EXISTS (объект уже исчез каскадно вместе с таблицей),
        -- но v_dropped всё равно инкрементируется на каждой такой "лишней"
        -- итерации, завышая счётчик. Найдено реальным тестом на PostgreSQL,
        -- не было видно на уровне одной только спецификации.
        WHERE n.nspname = 'messaging' AND c.relname LIKE 'message_lifecycle_history_%'
              AND c.relkind IN ('r', 'p')
    LOOP
        BEGIN
            v_partition_hour := to_timestamp(
                substring(r.relname FROM 'history_(\d{8}_\d{2})'), 'YYYYMMDD_HH24'
            );
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;
        END;
        IF v_partition_hour IS NOT NULL AND v_partition_hour < v_cutoff THEN
            EXECUTE format('DROP TABLE IF EXISTS messaging.%I', r.relname);
            v_dropped := v_dropped + 1;
        END IF;
    END LOOP;
    RETURN v_dropped;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION dlr.create_correlation_partition(p_hour_start TIMESTAMPTZ)
RETURNS TEXT AS $$
DECLARE
    v_partition_name TEXT;
    v_hour_end TIMESTAMPTZ;
BEGIN
    v_hour_end := p_hour_start + INTERVAL '1 hour';
    v_partition_name := 'dlr_correlation_' || to_char(p_hour_start, 'YYYYMMDD_HH24');
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS dlr.%I PARTITION OF dlr.dlr_correlation
         FOR VALUES FROM (%L) TO (%L)',
        v_partition_name, p_hour_start, v_hour_end
    );
    RETURN v_partition_name;
END;
$$ LANGUAGE plpgsql;

-- Retention по умолчанию 48ч — консервативный запас поверх типичного DLR SLA
-- (capacity_model.md §1 использовал 4ч как допущение для расчёта объёма).
-- Реальный SLA — per-operator, требует уточнения; параметр можно вызывать
-- с другим значением per-partition набора, если операторы разойдутся сильно.
CREATE OR REPLACE FUNCTION dlr.drop_old_correlation_partitions(p_retain_hours INT DEFAULT 48)
RETURNS INT AS $$
DECLARE
    r RECORD;
    v_cutoff TIMESTAMPTZ := date_trunc('hour', now()) - (p_retain_hours || ' hours')::interval;
    v_partition_hour TIMESTAMPTZ;
    v_dropped INT := 0;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_class c
        JOIN pg_namespace n ON c.relnamespace = n.oid
        -- relkind-фильтр по той же причине, что в messaging-версии выше.
        WHERE n.nspname = 'dlr' AND c.relname LIKE 'dlr_correlation_%'
              AND c.relkind IN ('r', 'p')
    LOOP
        BEGIN
            v_partition_hour := to_timestamp(
                substring(r.relname FROM 'correlation_(\d{8}_\d{2})'), 'YYYYMMDD_HH24'
            );
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;
        END;
        IF v_partition_hour IS NOT NULL AND v_partition_hour < v_cutoff THEN
            EXECUTE format('DROP TABLE IF EXISTS dlr.%I', r.relname);
            v_dropped := v_dropped + 1;
        END IF;
    END LOOP;
    RETURN v_dropped;
END;
$$ LANGUAGE plpgsql;

-- Бутстрап: создать партиции на текущий час + следующие 4 часа для обеих
-- таблиц, чтобы после применения миграций в них сразу можно было писать.
DO $$
DECLARE
    v_hour TIMESTAMPTZ := date_trunc('hour', now());
    i INT;
BEGIN
    FOR i IN 0..4 LOOP
        PERFORM messaging.create_lifecycle_history_partition(v_hour + (i || ' hours')::interval);
        PERFORM dlr.create_correlation_partition(v_hour + (i || ' hours')::interval);
    END LOOP;
END $$;
