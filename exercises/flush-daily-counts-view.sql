CREATE OR REPLACE FUNCTION flush_daily_counts_queue()
RETURNS bool
LANGUAGE plpgsql
AS $body$
DECLARE
    v_inserts int;
    v_updates int;
    v_prunes int;
BEGIN
    IF NOT pg_try_advisory_xact_lock('data_daily_counts_queue'::regclass::oid::bigint) THEN
         RAISE NOTICE 'skipping queue flush';
         RETURN false;
    END IF;

    WITH
    aggregated_queue AS (
        SELECT created_date, SUM(diff) AS value
        FROM data_daily_counts_queue
        GROUP BY created_date
    ),
    preexist AS (
        SELECT *,
            EXISTS(
                SELECT *
                FROM data_daily_counts_cached h
                WHERE h.created_date = aggregated_queue.created_date
            ) does_exist
        FROM aggregated_queue
    ),
    perform_updates AS (
        UPDATE data_daily_counts_cached AS h
        SET value = h.value + preexist.value
        FROM preexist
        WHERE preexist.does_exist AND h.created_date = preexist.created_date
        RETURNING 1
    ),
    perform_inserts AS (
        INSERT INTO data_daily_counts_cached
        SELECT created_date, value
        FROM preexist
        WHERE NOT preexist.does_exist
        RETURNING 1
    ),
    perform_prune AS (
        DELETE FROM data_daily_counts_queue
        RETURNING 1
    )
    SELECT
        (SELECT count(*) FROM perform_updates) updates,
        (SELECT count(*) FROM perform_inserts) inserts,
        (SELECT count(*) FROM perform_prune) prunes
    INTO v_updates, v_inserts, v_prunes;

    RAISE NOTICE 'performed queue (hourly) flush: % inserts, % updates, % prunes', v_inserts, v_updates, v_prunes;

    RETURN true;
END;
$body$;

