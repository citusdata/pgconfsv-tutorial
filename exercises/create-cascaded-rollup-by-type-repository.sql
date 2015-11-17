SELECT pagg.create_cascaded_rollup(
    tablename := 'data',
    rollupname := 'data_by_type_repo',
    group_by := ARRAY['type', $$repo->>'name'$$],
    group_by_names := ARRAY['type', 'reponame'],
    cascade := ARRAY[$$date_trunc('hour', created_at)$$, $$date_trunc('day', created_at)$$, $$date_trunc('month', created_at)$$],
    cascade_names := ARRAY['hourly', 'daily', 'monthly'],
    cascade_name := 'created_at',
    agg_sum := ARRAY[$$(payload->>'distinct_size')::int8$$],
    agg_sum_names := ARRAY['num_commits']
);


