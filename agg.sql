CREATE TABLE IF NOT EXISTS data(
    id bigserial primary key,
    github_id bigint not null,
    type text not null,
    public bool not null,
    created_at timestamptz NOT NULL,
    actor jsonb,
    repo jsonb,
    org jsonb,
    payload jsonb
);

BEGIN;

LOCK TABLE data;

CREATE TABLE IF NOT EXISTS data_1_aggregated_hourly(
   type text not null,
   created_at timestamptz not null,
   value bigint
);

CREATE TABLE IF NOT EXISTS data_1_aggregated_daily(
   type text not null,
   created_at timestamptz not null,
   value bigint
);

CREATE TABLE IF NOT EXISTS data_1_queue_hourly(
   type text not null,
   created_at timestamptz not null,
   diff bigint
);

CREATE TABLE IF NOT EXISTS data_1_queue_daily(
   type text not null,
   created_at timestamptz not null,
   diff bigint
);

CREATE OR REPLACE FUNCTION data_queue_hourly()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
    CASE TG_OP
    WHEN 'INSERT' THEN
        INSERT INTO data_1_queue_hourly(type, created_at, diff)
        VALUES (NEW.type, date_trunc('hour', NEW.created_at), +1);
    WHEN 'UPDATE' THEN
        INSERT INTO data_1_queue_hourly(type, created_at, diff)
        VALUES (OLD.type, date_trunc('hour', OLD.created_at), -1);

        INSERT INTO data_1_queue_hourly(type, created_at, diff)
        VALUES (NEW.type, date_trunc('hour', NEW.created_at), +1);
    WHEN 'DELETE' THEN
        INSERT INTO data_1_queue_hourly(type, created_at, diff)
        VALUES (OLD.type, date_trunc('hour', OLD.created_at), -1);
    WHEN 'TRUNCATE' THEN
        RAISE 'truncation not supported right now';
    END CASE;

    IF random() < 0.0001 THEN
       PERFORM data_1_flush_queue();
    END IF;

    RETURN NULL;
END;
$body$;


CREATE OR REPLACE FUNCTION data_queue_daily()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
    CASE TG_OP
    WHEN 'INSERT' THEN
        INSERT INTO data_1_queue_daily(type, created_at, diff)
        VALUES (NEW.type, date_trunc('day', NEW.created_at), +NEW.value);
    WHEN 'UPDATE' THEN
        INSERT INTO data_1_queue_daily(type, created_at, diff)
        VALUES (OLD.type, date_trunc('day', OLD.created_at), -OLD.value);

        INSERT INTO data_1_queue_daily(type, created_at, diff)
        VALUES (NEW.type, date_trunc('day', NEW.created_at), +NEW.value);

        /* verify type doesn't change */
    WHEN 'DELETE' THEN
        INSERT INTO data_1_queue_daily(type, created_at, diff)
        VALUES (OLD.type, date_trunc('day', OLD.created_at), -NEW.value);
    WHEN 'TRUNCATE' THEN
        RAISE 'truncation not supported right now';
    END CASE;

    IF random() < 0.01 THEN
       PERFORM data_1_flush_queue();
    END IF;

    RETURN NULL;
END;
$body$;


DROP TRIGGER IF EXISTS data_queue_hourly ON data;
DROP TRIGGER IF EXISTS data_queue_hourly_trunc ON data;

CREATE TRIGGER data_queue_hourly
AFTER INSERT OR UPDATE OR DELETE
ON data
FOR EACH ROW
EXECUTE PROCEDURE data_queue_hourly();

CREATE TRIGGER data_queue_hourly_trunc
BEFORE TRUNCATE /* FIXME */
ON data
FOR EACH STATEMENT
EXECUTE PROCEDURE data_queue_hourly();

CREATE TRIGGER data_queue_daily
AFTER INSERT OR UPDATE OR DELETE
ON data_1_aggregated_hourly
FOR EACH ROW
EXECUTE PROCEDURE data_queue_daily();

CREATE TRIGGER data_queue_daily_trunc
BEFORE TRUNCATE /* FIXME */
ON data_1_aggregated_hourly
FOR EACH STATEMENT
EXECUTE PROCEDURE data_queue_daily();


CREATE OR REPLACE VIEW data_1_hourly AS
SELECT type, created_at, SUM(value)
FROM (
    SELECT type, created_at, value
    FROM data_1_aggregated_hourly
  UNION ALL
    SELECT type, created_at, diff AS value
    FROM data_1_queue_hourly
) combine
GROUP BY type, created_at
;

CREATE OR REPLACE VIEW data_1_daily AS
SELECT type, date_trunc('day', created_at), SUM(value)
FROM (
    SELECT type, created_at, value
    FROM data_1_aggregated_daily
  UNION ALL
    SELECT type, created_at, diff AS value
    FROM data_1_queue_daily
  UNION ALL
    SELECT type, created_at, diff AS value
    FROM data_1_queue_hourly
) combine
GROUP BY type, created_at
;

CREATE OR REPLACE VIEW data_1_hourly_uncached AS
SELECT type, date_trunc('hour', created_at) created_at, count(*)
FROM data
GROUP BY 1, 2;

CREATE OR REPLACE VIEW data_1_daily_uncached AS
SELECT type, date_trunc('day', created_at) created_at, count(*)
FROM data
GROUP BY 1, 2;

CREATE OR REPLACE VIEW data_1_hourly_stale AS
SELECT type, created_at, value
FROM data_1_aggregated_hourly
;

CREATE OR REPLACE VIEW data_1_daily_stale AS
SELECT type, created_at, value
FROM data_1_aggregated_daily
;

-- initial load: hourly, automatically fills daily queue
INSERT INTO data_1_aggregated_hourly(type, created_at, value) SELECT type, date_trunc('hour', created_at), count(*) FROM data GROUP BY 1, 2 ORDER BY 1;

COMMIT;
