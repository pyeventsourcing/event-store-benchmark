use tokio_postgres::{Client, Error, GenericClient};
use uuid::Uuid;
use serde_json::Value;
use crate::dcb::EventTagQuery;

pub const CREATE_APPEND_EVENTS_FUNCTION: &str = r#"
CREATE OR REPLACE FUNCTION mt_quick_append_events(
    stream_id uuid,
    stream_type varchar,
    tenantid varchar,
    event_ids uuid[],
    event_types varchar[],
    dotnet_types varchar[],
    bodies jsonb[],
    tag_string_values varchar[]
) RETURNS int[] AS $$
DECLARE
    event_version int;
    index int;
    seq int;
    actual_tenant varchar;
    return_value int[];
BEGIN
    -- 1. Determine current stream version and create stream if needed
    SELECT version INTO event_version FROM mt_streams WHERE id = stream_id;
    
    IF event_version IS NULL THEN
        event_version = 0;
        INSERT INTO mt_streams (id, type, version, timestamp, tenant_id) 
        VALUES (stream_id, stream_type, 0, now(), tenantid);
    ELSE
        IF tenantid IS NOT NULL THEN
            SELECT tenant_id INTO actual_tenant FROM mt_streams WHERE id = stream_id;
            IF actual_tenant != tenantid THEN
                RAISE EXCEPTION 'The tenantid does not match the existing stream';
            END IF;
        END IF;
    END IF;

    -- 2. Iterate through events and insert
    index := 1;
    -- return_value[1] is the new stream version (optional, Marten returns it)
    return_value := ARRAY[event_version + array_length(event_ids, 1)];

    FOR index IN 1..array_length(event_ids, 1) LOOP
        seq := nextval('mt_events_sequence');
        return_value := array_append(return_value, seq);

        event_version := event_version + 1;

        INSERT INTO mt_events
            (seq_id, id, stream_id, version, data, type, tenant_id, mt_dotnet_type, timestamp)
        VALUES
            (seq, event_ids[index], stream_id, event_version, bodies[index], event_types[index], tenantid, dotnet_types[index], (now() at time zone 'utc'));

        -- Handle string tags
        IF tag_string_values[index] IS NOT NULL THEN
            INSERT INTO mt_event_tag_string (value, seq_id) 
            VALUES (tag_string_values[index], seq) 
            ON CONFLICT DO NOTHING;
        END IF;
    END LOOP;

    -- 3. Update stream version
    UPDATE mt_streams SET version = event_version, timestamp = now() WHERE id = stream_id;

    RETURN return_value;
END
$$ LANGUAGE plpgsql;
"#;

pub async fn quick_append_events(
    client: &Client,
    stream_id: Uuid,
    stream_type: &str,
    tenant_id: &str,
    event_ids: &[Uuid],
    event_types: &[&str],
    dotnet_types: &[Option<String>],
    bodies: &[Value],
    tags: &[Option<String>],
) -> Result<Vec<i32>, Error> {
    let result: Vec<i32> = client.query_one(
        "SELECT mt_quick_append_events($1, $2, $3, $4, $5, $6, $7, $8)",
        &[
            &stream_id,
            &stream_type,
            &tenant_id,
            &event_ids,
            &event_types,
            &dotnet_types,
            &bodies,
            &tags,
        ]
    ).await?.get(0);

    Ok(result)
}

pub async fn insert_tag(
    client: &impl GenericClient,
    tag_type: &str,
    tag_value: &str,
    seq_id: i64,
) -> Result<u64, Error> {
    let sql = crate::schema::get_insert_tag_sql(tag_type);
    client.execute(&sql, &[&tag_value, &seq_id]).await
}

pub async fn insert_event(
    client: &impl GenericClient,
    event_data: &Value,
    event_type: &str,
    mt_dotnet_type: &Option<String>,
    event_id: &Uuid,
    stream_id: &Uuid,
    version: i32,
    timestamp: &chrono::DateTime<chrono::Utc>,
    tenant_id: &str,
    seq_id: i64,
) -> Result<i64, Error> {
    let rows = client.query(
        "INSERT INTO mt_events (data, type, mt_dotnet_type, id, stream_id, version, timestamp, tenant_id, seq_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING seq_id",
        &[event_data, &event_type, mt_dotnet_type, event_id, stream_id, &version, timestamp, &tenant_id, &seq_id]
    ).await?;
    
    Ok(rows[0].get(0))
}

pub async fn insert_stream(
    client: &impl GenericClient,
    id: &Uuid,
    stream_type: &str,
    version: i32,
    tenant_id: &str,
) -> Result<u64, Error> {
    client.execute(
        "INSERT INTO mt_streams (id, type, version, tenant_id) VALUES ($1, $2, $3, $4)",
        &[id, &stream_type, &version, &tenant_id]
    ).await
}

pub async fn get_stream_version(
    client: &impl GenericClient,
    stream_id: &Uuid,
) -> Result<i32, Error> {
    let row = client.query_opt(
        "SELECT version FROM mt_streams WHERE id = $1",
        &[stream_id]
    ).await?;
    
    Ok(row.map(|r| r.get(0)).unwrap_or(0))
}

pub async fn update_stream_version(
    client: &impl GenericClient,
    stream_id: &Uuid,
    version: i32,
) -> Result<u64, Error> {
    client.execute(
        "UPDATE mt_streams SET version = $1, timestamp = now() WHERE id = $2",
        &[&version, stream_id]
    ).await
}

pub struct TaggedEvent {
    pub id: uuid::Uuid,
    pub stream_id: uuid::Uuid,
    pub version: i32,
    pub data: Value,
    pub event_type: String,
    pub dotnet_type: Option<String>,
    pub tags: Vec<String>,
}

pub async fn rich_append_events(
    client: &mut Client,
    query: Option<&EventTagQuery<'_>>,
    events: Vec<TaggedEvent>
) -> Result<(bool, Vec<i64>), Error> {
    // 1. Start transaction
    let tx = client.transaction().await?;

    // 2. Consistency check
    if let Some(q) = query {
        let last_seen = q.last_seen_sequence;
        let tag_values: Vec<String> = q.conditions.iter().map(|c| c.tag_value.to_string()).collect();
        let conflict: bool = tx.query_one(
            "SELECT EXISTS (SELECT 1 FROM mt_event_tag_string t0 INNER JOIN mt_events e ON t0.seq_id = e.seq_id WHERE t0.seq_id > $1 AND t0.value = ANY($2))",
            &[&last_seen, &tag_values]
        ).await?.get(0);

        if conflict {
            tx.rollback().await?;
            return Ok((false, Vec::new()));
        }
    }

    // 3. Append operations
    let mut seq_ids = Vec::new();

    // Prepare statements for reuse
    let stream_stmt = tx.prepare("INSERT INTO mt_streams (id, type, version, tenant_id) VALUES ($1, 'default', $2, 'DEFAULT') ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version").await?;
    let event_stmt = tx.prepare("INSERT INTO mt_events (data, type, mt_dotnet_type, id, stream_id, version, timestamp, tenant_id, seq_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, nextval('mt_events_sequence')) RETURNING seq_id").await?;
    let tag_stmt = tx.prepare("INSERT INTO mt_event_tag_string (value, seq_id) VALUES ($1, $2) ON CONFLICT DO NOTHING").await?;

    for event in &events {
        // stream upsert
        tx.execute(&stream_stmt, &[&event.stream_id, &event.version]).await?;

        // event insert
        let timestamp = chrono::Utc::now();
        let row = tx.query_one(
            &event_stmt,
            &[&event.data, &event.event_type, &event.dotnet_type, &event.id, &event.stream_id, &event.version, &timestamp, &"DEFAULT"]
        ).await?;
        let seq_id: i64 = row.get(0);
        seq_ids.push(seq_id);

        // tag inserts
        for tag in &event.tags {
            tx.execute(&tag_stmt, &[tag, &seq_id]).await?;
        }
    }

    // 4. Commit
    tx.commit().await?;

    Ok((true, seq_ids))
}
