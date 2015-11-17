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

    v_viewsources text[] = '{}';

    v_fillsql text = '';

BEGIN

    IF array_length(cascade, 1) <> array_length(cascade_names, 1) THEN
        RAISE 'cascade and cascade_names parameters out of sync';
    END IF;

    IF group_by_names IS NOT NULL AND array_length(group_by, 1) <> array_length(group_by_names, 1) THEN
        RAISE 'cascade and cascade_names parameters out of sync';
    END IF;

    IF array_length(agg_count, 1) > 0 AND array_length(agg_sum, 1) > 0 THEN
        RAISE 'sum and count cannot be done together currently';
    END IF;

    IF array_length(agg_count, 1) > 1 OR array_length(agg_sum, 1) > 1 THEN
        RAISE 'currently only one aggregate can be computed at once';
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
        v_full_grpnames text[] := '{}';

        v_final_grpexprs text[] := '{}';
        v_full_grpexprs text[] := '{}';

        v_queues text;
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

            v_final_grpexprs = v_final_grpexprs || v_grpexpr;

            v_final_grpnames = v_final_grpnames || (quote_ident(v_grpname));
            v_full_grpnames = v_full_grpnames || (quote_ident(v_grpname));

            v_grptype = pagg.expr_of_type(tablename, v_grpexpr);

            v_coldef = v_coldef || (quote_ident(v_grpname) ||' '||v_grptype);
        END LOOP;

        v_full_grpexprs = v_final_grpexprs || v_cascexpr;

        IF v_cascnum <> 1 THEN
            v_final_grpexprs = v_final_grpnames;
        END IF;

        v_full_grpnames = v_full_grpnames || quote_ident(cascade_name);
        v_coldef = v_coldef || (quote_ident(cascade_name) ||' '||v_casctype);
        v_coldef_queue = v_coldef;

        -- create a column for the aggregated column expression
        IF array_length(agg_count, 1) > 0 THEN
            v_aggname = agg_count_names[1];
            v_aggexpr = agg_count[1];

            IF v_aggexpr <> '*' THEN
                RAISE 'only * is supported as an aggregate right now, not %', v_grpexpr;
                -- FIXME: implementing counting simple columns ought to be
                -- pretty simple
            END IF;

            -- higher levels use the column
            IF v_cascnum = 1 THEN
               v_incby = 1;
            END IF;
        ELSE
            v_aggname = agg_sum_names[1];
            v_aggexpr = agg_sum[1];

            -- higher levels use the column
            IF v_cascnum = 1 THEN
               v_incby = v_aggexpr;
            END IF;
        END IF;

        -- FIXME: Improve type detection
        v_coldef = v_coldef || (quote_ident(v_aggname) ||' int8');
        v_coldef_queue = v_coldef_queue || (quote_ident(v_aggname||'_diff') ||' int8');

        --FOR v_aggnum IN SELECT * FROM generate_series(1, array_length(agg_count, 1)) LOOP
        --    v_aggname = agg_count_names[v_aggnum];
        --    v_aggexpr = agg_count[v_aggnum];
	--
        --    IF v_aggexpr <> '*' THEN
        --        RAISE 'only * is supported as an aggregate right now, not %', v_grpexpr;
        --        -- FIXME: implementing counting simple columns ought to be
        --        -- pretty simple
        --    END IF;
	--
        --END LOOP;

        EXECUTE format('CREATE TABLE pagg_data.%s_mat_%s(%s)',
            rollupname, v_cascname, array_to_string(v_coldef, ', '));
        EXECUTE format('CREATE TABLE pagg_queues.%s_queue_%s(%s)',
            rollupname, v_cascname, array_to_string(v_coldef_queue, ', '));
        -- need an index, to make updates faster
        EXECUTE format('CREATE INDEX ON pagg_data.%s_mat_%s(%s);',
            rollupname, v_cascname, array_to_string(v_full_grpnames, ', '));
        EXECUTE format('CREATE INDEX ON pagg_data.%s_mat_%s(%s, %s);',
            rollupname, v_cascname, array_to_string(v_final_grpnames, ', '),
            v_cascexpr
            );
        EXECUTE format('CREATE INDEX ON pagg_queues.%s_queue_%s(%s, %s);',
            rollupname, v_cascname, array_to_string(v_final_grpnames, ', '),
            v_cascexpr
            );
        --EXECUTE format('CREATE INDEX ON pagg_queues.%s_queue_%s(%s);',
        --    rollupname, v_cascname, array_to_string(v_full_grpexprs, ', '));

        /*
         * create
         *  a) trigger on the data (and finer grained) tables into the next
         *     queue tables
         *  b) queue flush function
         *
         * Right now only one aggregation is supported, should instead process
         * all and put them into one aggregation table?
         */
        IF array_length(agg_count, 1) > 0 THEN
            v_aggname = agg_count_names[1];
            v_aggexpr = agg_count[1];
            IF v_cascnum = 1 THEN
               v_incby = 1;
            END IF;
        ELSE
            v_aggname = agg_sum_names[1];
            v_aggexpr = agg_sum[1];
            IF v_cascnum = 1 THEN
               v_incby = v_aggexpr;
            END IF;
        END IF;

        /* create trigger */
        v_sql := format($trig$
-- function inserting into the queue
CREATE OR REPLACE FUNCTION %2$s()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
    CASE TG_OP
    -- FIXME: dynamic expr & grouping columns
    WHEN 'INSERT' THEN
        INSERT INTO %9$s(%5$s, %13$s, %8$s)
        SELECT %6$s, %10$s, +(%12$s) FROM (SELECT NEW.*) f;
    WHEN 'UPDATE' THEN
        INSERT INTO %9$s(%5$s, %13$s, %8$s)
        SELECT %6$s, %10$s, -(%12$s) FROM (SELECT OLD.*) f;

        INSERT INTO %9$s(%5$s, %13$s, %8$s)
        SELECT %6$s, %10$s, +(%12$s) FROM (SELECT OLD.*) f;
    WHEN 'DELETE' THEN
        INSERT INTO %9$s(%5$s, %13$s, %8$s)
        SELECT %6$s, %10$s, -(%12$s) FROM (SELECT OLD.*) f;
    WHEN 'TRUNCATE' THEN
        RAISE 'truncation not supported right now';
    END CASE;

    RETURN NULL;
END;
$body$;

-- function occasionally, at xact end, triggering queue flushes
--
-- This is triggered via a deferred constraint trigger, so we a) only
-- flush once in batch imports b) only acquire the queue lock at the
-- end of an xact.
CREATE OR REPLACE FUNCTION %15$s()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $body$
BEGIN
    IF random() < 0.0001 THEN
       PERFORM %11$s(false);
    END IF;

    RETURN NULL;
END;
$body$;

-- queue trigger
CREATE TRIGGER %4$s
AFTER INSERT OR UPDATE OR DELETE
ON %1$s
FOR EACH ROW
EXECUTE PROCEDURE %2$s();

-- consider queue flush trigger
CREATE CONSTRAINT TRIGGER %14$s
AFTER INSERT OR UPDATE OR DELETE
ON %1$s
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE PROCEDURE %15$s();

-- trigger preventing truncate
CREATE TRIGGER %16$s
AFTER TRUNCATE
ON %1$s
FOR EACH STATEMENT
EXECUTE PROCEDURE %2$s();

            $trig$,
            -- 1$: target table (data table or finer grained table)
            v_curtarget,
            -- 2$: procedurename
            'pagg.'||quote_ident(rollupname||'_queue_'||v_aggname||'_'||v_cascname),
            -- 3$: materialized table name
            'pagg_data.'||quote_ident(rollupname||'_mat_'||v_cascname),
            -- 4$: triggername DML
            quote_ident(rollupname||'_queue_'||v_aggname||'_'||v_cascname),
            -- 5$: group by columns
            array_to_string(v_final_grpnames, ', '),
            -- 6$: group by expressions
            array_to_string(v_final_grpexprs, ', '),
            -- 7$: group by columns
            'unused',
            -- 8$: queue aggregation column name
            quote_ident(v_aggname||'_diff'),
            -- 9$: queue table name
            'pagg_queues.'||quote_ident(rollupname||'_queue_'||v_cascname),
            -- 10$: cascade expression (for group by)
            v_cascexpr,
            -- 11$: flush function table name
            'pagg.'||quote_ident(rollupname||'_flush_'||v_aggname||'_'||v_cascname),
            -- 12$: expression to increment/decrement values with
            v_incby,
            -- 13$: cascading step column name
            cascade_name,
            -- 14$: triggername queue flush
            quote_ident(rollupname||'_consider_flush_'||v_aggname||'_'||v_cascname),
            -- 15$: functionname  queue flush
            'pagg.'||quote_ident(rollupname||'_consider_flush_'||v_aggname||'_'||v_cascname),
            -- 16$: triggername queue flush
            quote_ident(rollupname||'_truncate_'||v_aggname||'_'||v_cascname)
            );
         /* FIXME: Improve parameter ordering */

         --RAISE NOTICE '%', v_sql;
         EXECUTE v_sql;

         -- create queue flush function
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

    IF NOT EXISTS(SELECT * FROM %2$s) THEN
        RAISE NOTICE 'skipping empty queue';
        RETURN false;
    END IF;

    ANALYZE %2$s;
    ANALYZE %6$s;

    WITH
    aggregated_queue AS (
        SELECT %4$s, SUM(%5$s) AS %5$s
        FROM %2$s
        GROUP BY %4$s
    ),
    preexist AS (
        SELECT *,
            EXISTS(
                SELECT *
                FROM %6$s materialized
                -- FIXME: IS NOT DISTINCT would handle NULLs, but doesn't end up with index scans
                WHERE (%9$s) = (%10$s)
            ) does_exist
        FROM aggregated_queue
    ),
    perform_updates AS (
        UPDATE %6$s AS materialized
        SET %7$s = materialized.%7$s + preexist.%5$s
        FROM preexist
        WHERE preexist.does_exist
            AND (%9$s) = (%8$s)
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

    RAISE NOTICE 'performed queue (%11$s) flush: %% inserts, %% updates, %% prunes', v_inserts, v_updates, v_prunes;

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
            'pagg_data.'||quote_ident(rollupname||'_mat_'||v_cascname),
            -- 7$: valuecol
            quote_ident(v_aggname),
            -- 8$: preexist_grp
            array_to_string(ARRAY(SELECT 'preexist.'||v FROM unnest(v_full_grpnames) v(v)), ', '),
            -- 9$: materialized_grp
            array_to_string(ARRAY(SELECT 'materialized.'||v FROM unnest(v_full_grpnames) v(v)), ', '),
            --10$: aggregated_queue_grp
            array_to_string(ARRAY(SELECT 'aggregated_queue.'||v FROM unnest(v_full_grpnames) v(v)), ', '),
            --11$: cascname
            v_cascname
            );
        EXECUTE v_sql;

        /*
         * append select from queue table to array of already existing
         * queue table selects. The less granular steps need the queues
         * from the more granular ones.
         */
         v_sql = format($sql$
    SELECT %2$s, %3$s, %5$s AS %4$s
    FROM %1$s
$sql$,
            -- 1$: queuetable
            'pagg_queues.'||quote_ident(rollupname||'_queue_'||v_cascname),
            -- 2$: groupcols
            array_to_string(v_final_grpnames, ', '),
            -- 3$: cascadename
            cascade_name,
            -- 4$: valuecol name
            quote_ident(v_aggname),
            -- 5$: valuecol_diff
            quote_ident(v_aggname||'_diff')
            );

        v_viewsources = v_viewsources || v_sql;

        -- build a select from all queue tables up to this granularity
        v_queues = array_to_string(v_viewsources, '
  UNION ALL
');

        -- CREATE VIEW from this granularities mat table + all queues
        v_sql := format($sql$
CREATE VIEW %1$s AS
SELECT %3$s, %5$s AS %4$s, SUM(%7$s) AS %6$s
FROM (
     SELECT %3$s, %4$s, %7$s
     FROM %2$s
   UNION ALL
%9$s -- all queue tables follow
) combine
GROUP BY %3$s, %5$s
            $sql$,
            -- 1$: viewtablename
            'pagg.'||quote_ident(rollupname||'_'||v_cascname),
            -- 2$: materialized table
            'pagg_data.'||quote_ident(rollupname||'_mat_'||v_cascname),
            -- 3$: groupcols
            array_to_string(v_final_grpnames, ', '),
            -- 4$: cascadename
            cascade_name,
            -- 5$: cacadeexpr
            v_cascexpr,
            -- 6$: aggname
            v_aggname,
            -- 7$: valuecol
            quote_ident(v_aggname),
            -- 8$: valuecol_diff
            quote_ident(v_aggname||'_diff'),
            -- 9$: queue table selects, UNION ALL'ed
            v_queues
            );

        EXECUTE v_sql;

        -- create sql to run to later actually fill the aggregation tables
        -- want to only do that on the lowest aggregation level - the
        -- upper ones will be filled automatically
        IF v_cascnum = 1 THEN
           v_fillsql = v_fillsql || format($sql$
INSERT INTO %1$s
SELECT %2$s, %3$s, %4$s
FROM %5$s
GROUP BY %2$s, %3$s;
$sql$,
            -- 1$: mattablename
            'pagg_data.'||quote_ident(rollupname||'_mat_'||v_cascname),
            -- 2$: grpcols
            array_to_string(v_final_grpexprs, ', '),
            -- 3$: cascadexpr
            v_cascexpr,
            -- 4$: countexpr
            'count(*)',
            -- 5$: tblname
            tablename::text
            );
        END IF;

        v_incby = quote_ident(v_aggname);
        v_curtarget = 'pagg_data.'||quote_ident(rollupname||'_mat_'||v_cascname);
    END;
    END LOOP;  /* around aggregation granularities */

    RAISE NOTICE 'filling materialized tables with pre-existing data';
    EXECUTE v_fillsql;
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
    rollupname := 'data_by_type_repo',
    group_by := ARRAY['type', $$repo->>'name'$$],
    group_by_names := ARRAY['type', 'reponame'],
    cascade := ARRAY[$$date_trunc('hour', created_at)$$, $$date_trunc('day', created_at)$$, $$date_trunc('month', created_at)$$],
    cascade_names := ARRAY['hourly', 'daily', 'monthly'],
    cascade_name := 'created_at',
--    agg_count := ARRAY['*'],
--    agg_count_names := ARRAY['countstar']
    agg_sum := ARRAY[$$(payload->>'distinct_size')::int8$$],
    agg_sum_names := ARRAY['num_commits']
);

*/
