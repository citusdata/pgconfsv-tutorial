CREATE TABLE IF NOT EXISTS data(
    id bigserial primary key,
    github_id bigint not null,
    type text not null,
    public bool not null,
    created_at timestamp NOT NULL,
    actor jsonb,
    repo jsonb,
    org jsonb,
    payload jsonb
);
