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


