CREATE OR REPLACE FUNCTION data_1_flush_queue()
RETURNS bool
LANGUAGE plpgsql
AS $body$
DECLARE
    v_inserts int;
    v_updates int;
    v_prunes int;
BEGIN
    LOCK data_1_aggregated_hourly IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;

    WITH
    preagg AS (
        SELECT type, created_at, SUM(diff) AS value
        FROM data_1_queue_hourly
        GROUP BY type, created_at
    ),
    preexist AS (
        SELECT *,
            EXISTS(
                SELECT *
                FROM data_1_aggregated_hourly h
                WHERE (h.type, h.created_at) IS NOT DISTINCT FROM (preagg.type, preagg.created_at)
            ) does_exist
        FROM preagg
    ),
    perform_updates AS (
        UPDATE data_1_aggregated_hourly AS h
        SET value = h.value + preexist.value
        FROM preexist
        WHERE preexist.does_exist
            AND (h.type, h.created_at) IS NOT DISTINCT FROM (preexist.type, preexist.created_at)
        RETURNING 1
    ),
    perform_inserts AS (
        INSERT INTO data_1_aggregated_hourly
        SELECT type, created_at, value
        FROM preexist
        WHERE NOT preexist.does_exist
        RETURNING 1
    ),
    perform_prune AS (
        DELETE FROM data_1_queue_hourly
        RETURNING 1
    )
    SELECT
        (SELECT count(*) FROM perform_updates) updates,
        (SELECT count(*) FROM perform_inserts) inserts,
        (SELECT count(*) FROM perform_prune) prunes
    INTO v_updates, v_inserts, v_prunes
        ;

    RAISE NOTICE 'performed queue (hourly) flush: % inserts, % updates, % prunes', v_inserts, v_updates, v_prunes;

    RETURN true;
EXCEPTION WHEN lock_not_available THEN
    RAISE NOTICE 'skipping queue (hourly) flush';
    RETURN false;
END;
$body$;


CREATE OR REPLACE FUNCTION data_1_flush_daily_queue()
RETURNS bool
LANGUAGE plpgsql
AS $body$
DECLARE
    v_inserts int;
    v_updates int;
    v_prunes int;
BEGIN
    LOCK data_1_aggregated_daily IN SHARE UPDATE EXCLUSIVE MODE NOWAIT;

    WITH
    preagg AS (
        SELECT type, created_at, SUM(diff) AS value
        FROM data_1_queue_daily
        GROUP BY type, created_at
    ),
    preexist AS (
        SELECT *,
            EXISTS(
                SELECT *
                FROM data_1_aggregated_daily h
                WHERE (h.type, h.created_at) IS NOT DISTINCT FROM (preagg.type, preagg.created_at)
            ) does_exist
        FROM preagg
    ),
    perform_updates AS (
        UPDATE data_1_aggregated_daily AS h
        SET value = h.value + preexist.value
        FROM preexist
        WHERE preexist.does_exist
            AND (h.type, h.created_at) IS NOT DISTINCT FROM (preexist.type, preexist.created_at)
        RETURNING 1
    ),
    perform_inserts AS (
        INSERT INTO data_1_aggregated_daily
        SELECT type, created_at, value
        FROM preexist
        WHERE NOT preexist.does_exist
        RETURNING 1
    ),
    perform_prune AS (
        DELETE FROM data_1_queue_daily
        RETURNING 1
    )
    SELECT
        (SELECT count(*) FROM perform_updates) updates,
        (SELECT count(*) FROM perform_inserts) inserts,
        (SELECT count(*) FROM perform_prune) prunes
    INTO v_updates, v_inserts, v_prunes
        ;

    RAISE NOTICE 'performed queue (daily) flush: % inserts, % updates, % prunes', v_inserts, v_updates, v_prunes;
    RETURN true;
EXCEPTION WHEN lock_not_available THEN
    RAISE NOTICE 'skipping queue (daily) flush';
    RETURN false;
END;
$body$;
