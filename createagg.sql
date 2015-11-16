TRUNCATE data;

DROP SCHEMA IF EXISTS pagg_queues CASCADE;
DROP SCHEMA IF EXISTS pagg_data CASCADE;
DROP SCHEMA IF EXISTS pagg CASCADE;
CREATE SCHEMA pagg_queues;
CREATE SCHEMA pagg_data;
CREATE SCHEMA pagg;

/*
 * Determine the resulting type of an expression, trying to avoid having to
 * execute code.
 *
 * This isn't perfect, but seems to mostly work well. We could do much better
 * in C.
 */
CREATE OR REPLACE FUNCTION pagg.expr_of_type(p_tablename regclass, p_expr text)
    RETURNS text
    LANGUAGE plpgsql
    VOLATILE
    AS $b$
DECLARE
    v_sql text;
    v_exprtype text;
BEGIN
    v_sql = format($sql$SELECT pg_typeof((SELECT %s FROM %s LIMIT 0));$sql$,
        p_expr, p_tablename::regclass);
    EXECUTE v_sql INTO v_exprtype;
    RETURN v_exprtype;
END;
$b$;

CREATE OR REPLACE FUNCTION pagg.create_cascaded_rollup(
    tablename regclass,
    rollupname name,
    group_by text[] DEFAULT '{}',
    group_by_names text[] DEFAULT NULL,
    cascade text[] DEFAULT '{}',
    cascade_names name[] DEFAULT '{}',
    cascade_name name DEFAULT NULL,
    agg_count text[]  DEFAULT '{}',
    agg_count_names name[]  DEFAULT '{}',
    agg_sum text[]  DEFAULT '{}',
    agg_sum_names text[]  DEFAULT '{}'
)
RETURNS void
LANGUAGE plpgsql
VOLATILE
AS $b$
DECLARE
    v_cascnum int;
    v_cascname name;
    v_cascexpr text;

    v_sql text;

    v_curtarget name;
    v_incby text = '1'; -- FIXME, doesn't work for SUM
BEGIN

    IF array_length(cascade, 1) <> array_length(cascade_names, 1) THEN
        RAISE 'cascade and cascade_names parameters out of sync';
    END IF;

    IF group_by_names IS NOT NULL AND array_length(group_by, 1) <> array_length(group_by_names, 1) THEN
        RAISE 'cascade and cascade_names parameters out of sync';
    END IF;

    v_curtarget = tablename;

    /* create queue tables for the cascades */
    /* create data tables for the cascades */
    FOR v_cascnum IN 1 .. array_length(cascade, 1) LOOP
    DECLARE
        v_cascname name;
        v_casctype text;
        v_cascexpr text;

        v_grpnum int;
        v_grpname name;
        v_grpexpr text;
        v_grptype name;

        v_aggnum int;
        v_aggname name;
        v_aggexpr text;

        v_coldef text[] := '{}';
        v_coldef_queue text[] := '{}';

        v_final_grpnames text[] := '{}';
        v_new_grpnames text[] := '{}';
        v_old_grpnames text[] := '{}';
        v_full_grpnames text[] := '{}';

    BEGIN
        v_cascname = cascade_names[v_cascnum];
        v_cascexpr = cascade[v_cascnum];
        v_casctype = pagg.expr_of_type(tablename, v_cascexpr);

        /* build data structures related to the group by clauses */
        FOR v_grpnum IN 1 .. array_length(group_by, 1) LOOP
            v_grpexpr = group_by[v_grpnum];
            IF group_by_names IS NULL THEN
                /* FIXME: better name generation */
                v_grpname = group_by[v_grpnum];
            ELSE
                v_grpname = group_by_names[v_grpnum];
            END IF;
            v_final_grpnames = v_final_grpnames || (quote_ident(v_grpname));
            v_full_grpnames = v_full_grpnames || (quote_ident(v_grpname));
            v_new_grpnames = v_new_grpnames || ('NEW.'||quote_ident(v_grpname));
            v_old_grpnames = v_old_grpnames || ('OLD.'||quote_ident(v_grpname));

            v_grptype = pagg.expr_of_type(tablename, v_grpname);

            v_coldef = v_coldef || (quote_ident(v_grpname) ||' '||v_grptype);
        END LOOP;

        v_full_grpnames = v_full_grpnames || quote_ident(cascade_name);
        v_coldef = v_coldef || (quote_ident(cascade_name) ||' '||v_casctype);
        v_coldef_queue = v_coldef;

        FOR v_aggnum IN 1 .. array_length(agg_count, 1) LOOP
            v_aggname = agg_count_names[v_aggnum];
            v_aggexpr = agg_count[v_aggnum];

            v_coldef = v_coldef || (quote_ident(v_aggname) ||' int8');
            v_coldef_queue = v_coldef_queue || (quote_ident(v_aggname||'_diff') ||' int8');
        END LOOP;

        EXECUTE format('CREATE TABLE pagg_data.%s_%s(%s)', rollupname, v_cascname, array_to_string(v_coldef, ', '));
        EXECUTE format('CREATE TABLE pagg_queues.%s_queue_%s(%s)', rollupname, v_cascname, array_to_string(v_coldef_queue, ', '));

        /* TODO: We can actually combine all of these into one aggregation table? */
        FOR v_aggnum IN 1 .. array_length(agg_count, 1) LOOP
            v_aggname = agg_count_names[v_aggnum];
            v_aggexpr = agg_count[v_aggnum];

            v_sql := format($trig$
CREATE OR REPLACE FUNCTION %2$s()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
    CASE TG_OP
    -- FIXME: dynamic expr & grouping columns
    WHEN 'INSERT' THEN
        INSERT INTO %9$s(%7$s, %13$s, %8$s)
        VALUES (%5$s, (SELECT %10$s FROM (SELECT NEW.*) f), +(SELECT %12$s FROM (SELECT NEW.*) f));
    WHEN 'UPDATE' THEN
        INSERT INTO %9$s(%7$s, %13$s, %8$s)
        VALUES (%6$s, (SELECT %10$s FROM (SELECT OLD.*) f), -(SELECT %12$s FROM (SELECT OLD.*) f));

        INSERT INTO %9$s(%7$s, %13$s, %8$s)
        VALUES (%5$s, (SELECT %10$s FROM (SELECT NEW.*) f), +(SELECT %12$s FROM (SELECT NEW.*) f));
    WHEN 'DELETE' THEN
        INSERT INTO %9$s(%7$s, %13$s, %8$s)
        VALUES (%6$s, (SELECT %10$s FROM (SELECT OLD.*) f), -(SELECT %12$s FROM (SELECT OLD.*) f));
    WHEN 'TRUNCATE' THEN
        RAISE 'truncation not supported right now';
    END CASE;

    IF random() < 0.0001 THEN
       PERFORM %11$s(false);
    END IF;

    RETURN NULL;
END;
$body$;
CREATE TRIGGER %4$s
AFTER INSERT OR UPDATE OR DELETE
ON %1$s
FOR EACH ROW
EXECUTE PROCEDURE %2$s();
            $trig$,
            v_curtarget,
            'pagg.'||quote_ident(rollupname||'_queue_'||v_aggname||'_'||v_cascname),
            'pagg_data.'||quote_ident(rollupname||'_'||v_cascname),
            quote_ident(rollupname||'_queue_'||v_aggname||'_'||v_cascname),
            array_to_string(v_new_grpnames, ', '),
            array_to_string(v_old_grpnames, ', '),
            array_to_string(v_final_grpnames, ', '),
            quote_ident(v_aggname||'_diff'),
            'pagg_queues.'||quote_ident(rollupname||'_queue_'||v_cascname),
            v_cascexpr,
            'pagg.'||quote_ident(rollupname||'_flush_'||v_aggname||'_'||v_cascname),
            v_incby,
            cascade_name
            );
            /* FIXME: Improve parameter ordering */

            --RAISE NOTICE '%', v_sql;
            EXECUTE v_sql;

            -- create function used to flush the relevant queue
            v_sql := format($sql$
CREATE OR REPLACE FUNCTION %1$s(p_wait bool DEFAULT false)
RETURNS bool
LANGUAGE plpgsql
AS $body$
DECLARE
    v_inserts int8;
    v_updates int8;
    v_prunes int8;
BEGIN
    /*
     * Acquire a lock preventing concurrent queue flushes. These would make it
     * harder/more expensive to be correct.
     */
    IF p_wait THEN
        RAISE NOTICE 'waiting for lock';
        PERFORM pg_advisory_xact_lock('%2$s'::regclass::oid::bigint);
    ELSE
        IF NOT pg_try_advisory_xact_lock('%2$s'::regclass::oid::bigint) THEN
            RAISE NOTICE 'skipping queue flush';
            RETURN false;
        END IF;
    END IF;

    WITH
    preagg AS (
        SELECT %4$s, SUM(%5$s) AS %5$s
        FROM %2$s
        GROUP BY %4$s
    ),
    preexist AS (
        SELECT *,
            EXISTS(
                SELECT *
                FROM %6$s materialized
                WHERE (%9$s) IS NOT DISTINCT FROM (%10$s)
            ) does_exist
        FROM preagg
    ),
    perform_updates AS (
        UPDATE %6$s AS materialized
        SET %7$s = materialized.%7$s + preexist.%5$s
        FROM preexist
        WHERE preexist.does_exist
            AND (%9$s) IS NOT DISTINCT FROM (%8$s)
        RETURNING 1
    ),
    perform_inserts AS (
        INSERT INTO %6$s /* FIXME: add column list */
        SELECT %4$s, %5$s
        FROM preexist
        WHERE NOT preexist.does_exist
        RETURNING 1
    ),
    perform_prune AS (
        DELETE FROM %2$s
        RETURNING 1
    )
    SELECT
        (SELECT count(*) FROM perform_updates) updates,
        (SELECT count(*) FROM perform_inserts) inserts,
        (SELECT count(*) FROM perform_prune) prunes
    INTO v_updates, v_inserts, v_prunes;

    RAISE NOTICE 'performed queue (hourly) flush: %% inserts, %% updates, %% prunes', v_inserts, v_updates, v_prunes;

    RETURN true;
END;
$body$;
                $sql$,
                -- 1$: funcname
                'pagg.'||quote_ident(rollupname||'_flush_'||v_aggname||'_'||v_cascname),
                -- 2$: queuetablename
                'pagg_queues.'||quote_ident(rollupname||'_queue_'||v_cascname),
                -- 3$: aggname
                v_aggname,
                -- 4$: grpcols unadorned
                array_to_string(v_full_grpnames, ', '),
                -- 5$: diffcol
                quote_ident(v_aggname||'_diff'),
                -- 6$: mattablename
                'pagg_data.'||quote_ident(rollupname||'_'||v_cascname),
                -- 7$: valuecol
                quote_ident(v_aggname),
                -- 8$: preexist_grp
                array_to_string(ARRAY(SELECT 'preexist.'||v FROM unnest(v_full_grpnames) v(v)), ', '),
                -- 9$: materialized_grp
                array_to_string(ARRAY(SELECT 'materialized.'||v FROM unnest(v_full_grpnames) v(v)), ', '),
                --10$: preagg_grp
                array_to_string(ARRAY(SELECT 'preagg.'||v FROM unnest(v_full_grpnames) v(v)), ', ')
                );
            EXECUTE v_sql;

            v_incby = quote_ident(v_aggname);
            v_curtarget = 'pagg_data.'||quote_ident(rollupname||'_'||v_cascname);
        END LOOP; /* around count aggregates */
    END;
    END LOOP;  /* around aggregation granularities */

    /* create views to actually query the data */
END;
$b$;

/*
SELECT pagg.create_cascaded_rollup(
    tablename := 'data',
    rollupname := 'data_by_type',
    group_by := ARRAY['type'],
    cascade := ARRAY[$$date_trunc('hour', created_at)$$, $$date_trunc('day', created_at)$$, $$date_trunc('month', created_at)$$],
    cascade_names := ARRAY['hourly', 'daily', 'monthly'],
    cascade_name := 'created_at',
    agg_count := ARRAY['*'],
    agg_count_names := ARRAY['countstar']
);

SELECT pagg.create_cascaded_rollup(
    tablename := 'data',
    rollupname := 'data_by_repo',
    group_by := ARRAY[$$repo->'name'$$],
    group_by_names := ARRAY['reponame'],
    cascade := ARRAY[$$date_trunc(created_at, 'hour')$$, $$date_trunc(created_at, 'day')$$, $$date_trunc(created_at, 'month')$$],
    cascade_names := ARRAY['hourly', 'daily', 'monthly'],
    cascade_name := 'created_at',
    agg_sum := ARRAY[$$payload->>'size'$$],
    agg_sum_names := ARRAY['num_commits']
);
*/
