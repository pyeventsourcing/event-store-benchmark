pub const CREATE_EVENTS_SEQUENCE: &str = r#"
CREATE SEQUENCE IF NOT EXISTS mt_events_sequence;
"#;

pub const CREATE_STREAMS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS mt_streams (
    id                  uuid PRIMARY KEY,
    type                varchar(500) NULL,
    version             integer NULL,
    timestamp           timestamptz NOT NULL DEFAULT (now()),
    snapshot            jsonb NULL,
    snapshot_version    integer NULL,
    created             timestamptz NOT NULL DEFAULT (now()),
    tenant_id           varchar(255) DEFAULT 'DEFAULT',
    is_archived         boolean DEFAULT FALSE
);
"#;

pub const CREATE_EVENTS_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS mt_events (
    seq_id      bigint PRIMARY KEY DEFAULT nextval('mt_events_sequence'),
    id          uuid NOT NULL,
    stream_id   uuid REFERENCES mt_streams (id) ON DELETE CASCADE,
    version     integer NOT NULL,
    data        jsonb NOT NULL,
    type        varchar(500) NOT NULL,
    timestamp   timestamptz NOT NULL DEFAULT (now()),
    tenant_id   varchar(255) DEFAULT 'DEFAULT',
    mt_dotnet_type varchar(500) NULL,
    is_archived boolean DEFAULT FALSE,
    UNIQUE (stream_id, version)
);
"#;

pub const CREATE_EVENT_TAG_TABLE_PREFIX: &str = "CREATE TABLE IF NOT EXISTS mt_event_tag_";

pub const INSERT_EVENT_TAG: &str = r#"
INSERT INTO mt_event_tag_{suffix} (value, seq_id)
VALUES ($1, $2)
ON CONFLICT DO NOTHING;
"#;

pub fn get_insert_tag_sql(suffix: &str) -> String {
    INSERT_EVENT_TAG.replace("{suffix}", suffix)
}

pub fn get_create_tag_table_sql(suffix: &str) -> String {
    format!(
        r#"
CREATE TABLE IF NOT EXISTS mt_event_tag_{suffix} (
    value       text NOT NULL,
    seq_id      bigint NOT NULL REFERENCES mt_events (seq_id),
    PRIMARY KEY (value, seq_id)
);
"#,
        suffix = suffix
    )
}
