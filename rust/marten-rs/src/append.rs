use tokio_postgres::{Client, Error};
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
