use crate::MartenError;
use crate::read::{evaluate_append_condition, EventTagQuery};
use std::collections::HashMap;
use tokio_postgres::{Client, Error, GenericClient};
use uuid::Uuid;
use serde_json::Value;

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

pub struct NewEvent {
    pub id: uuid::Uuid,
    pub stream_id: uuid::Uuid,
    pub version: i32,
    pub data: Value,
    pub event_type: String,
    pub dotnet_type: Option<String>,
    pub tags: Vec<String>,
    pub sequence: i64,
}

pub async fn conditional_rich_append_events(
    client: &mut Client,
    events: Vec<NewEvent>,
    query: &EventTagQuery<'_>,
) -> Result<Vec<i64>, MartenError> {
    let result = evaluate_append_condition(client, query).await?;
    if !result {
        rich_append_events(client, events).await.map_err(MartenError::from)
    } else {
        Err(MartenError::AppendConditionFailed)
    }
}

pub async fn rich_append_events(
    client: &mut Client,
    events: Vec<NewEvent>
) -> Result<Vec<i64>, Error> {
    let tx = client.transaction().await?;
    
    let mut seq_ids = Vec::new();
    let mut max_versions: HashMap<Uuid, i32> = HashMap::new();
    let mut new_streams = std::collections::HashSet::new();

    for event in &events {
        let entry = max_versions.entry(event.stream_id).or_insert(event.version);
        if event.version > *entry {
            *entry = event.version;
        }
        if event.version == 1 {
            new_streams.insert(event.stream_id);
        }
    }

    for (stream_id, version) in &max_versions {
        if new_streams.contains(stream_id) {
            insert_stream(&tx, stream_id, "default", *version, "DEFAULT").await?;
        } else {
            update_stream_version(&tx, stream_id, *version).await?;
        }
    }

    for event in &events {
        let timestamp = chrono::Utc::now();
        let seq_id = insert_event(
            &tx,
            &event.data,
            &event.event_type,
            &event.dotnet_type,
            &event.id,
            &event.stream_id,
            event.version,
            &timestamp,
            "DEFAULT",
            event.sequence,
        ).await?;
        
        seq_ids.push(seq_id);

        for tag in &event.tags {
            insert_tag(&tx, "string", tag, seq_id).await?;
        }
    }

    tx.commit().await?;
    Ok(seq_ids)
}
