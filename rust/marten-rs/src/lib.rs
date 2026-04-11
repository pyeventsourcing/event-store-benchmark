pub mod schema;
pub mod append;
pub mod dcb;

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::NoTls;
    use uuid::Uuid;
    use serde_json::json;

    #[tokio::test]
    async fn test_postgres_integration() -> Result<(), Box<dyn std::error::Error>> {
        let connection_string = "host=localhost user=marten password=marten dbname=marten";
        let (client, connection) = match tokio_postgres::connect(connection_string, NoTls).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to connect to Postgres: {}. Skipping test as database might not be available in this environment.", e);
                return Ok(());
            }
        };

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Cleanup
        client.batch_execute("DROP FUNCTION IF EXISTS mt_append_events(uuid, varchar, varchar, uuid[], varchar[], varchar[], jsonb[], varchar[])").await?;
        client.batch_execute("DROP TABLE IF EXISTS mt_event_tag_test").await?;
        client.batch_execute("DROP TABLE IF EXISTS mt_event_tag_string").await?;
        client.batch_execute("DROP TABLE IF EXISTS mt_events").await?;
        client.batch_execute("DROP TABLE IF EXISTS mt_streams").await?;
        client.batch_execute("DROP SEQUENCE IF EXISTS mt_events_sequence").await?;

        // Create schema
        client.batch_execute(schema::CREATE_EVENTS_SEQUENCE).await?;
        client.batch_execute(schema::CREATE_STREAMS_TABLE).await?;
        client.batch_execute(schema::CREATE_EVENTS_TABLE).await?;
        client.batch_execute(&schema::get_create_tag_table_sql("string")).await?;

        // Create append function
        client.batch_execute(append::CREATE_APPEND_EVENTS_FUNCTION).await?;

        // Test insertion via mt_append_events
        let stream_id = Uuid::new_v4();
        let event_id1 = Uuid::new_v4();
        let event_id2 = Uuid::new_v4();
        let event_data1 = json!({"foo": "bar"});
        let event_data2 = json!({"baz": "qux"});

        let event_ids = vec![event_id1, event_id2];
        let event_types = vec!["test_event_1", "test_event_2"];
        let dotnet_types: Vec<Option<String>> = vec![None, None];
        let bodies = vec![event_data1.clone(), event_data2.clone()];
        let tags = vec![Some("tag1".to_string()), Some("tag2".to_string())];

        let result: Vec<i32> = client.query_one(
            "SELECT mt_append_events($1, $2, $3, $4, $5, $6, $7, $8)",
            &[
                &stream_id,
                &"test_stream",
                &"DEFAULT",
                &event_ids,
                &event_types,
                &dotnet_types,
                &bodies,
                &tags,
            ]
        ).await?.get(0);

        // Marten's result is [new_version, seq_id1, seq_id2, ...]
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], 2); // new version

        // Verify data
        let rows = client.query(
            "SELECT e.data, t.value FROM mt_events e JOIN mt_event_tag_string t ON e.seq_id = t.seq_id WHERE e.stream_id = $1 ORDER BY e.version",
            &[&stream_id]
        ).await?;

        assert_eq!(rows.len(), 2);
        let data1: serde_json::Value = rows[0].get(0);
        let tag1: &str = rows[0].get(1);
        let data2: serde_json::Value = rows[1].get(0);
        let tag2: &str = rows[1].get(1);

        assert_eq!(data1, event_data1);
        assert_eq!(tag1, "tag1");
        assert_eq!(data2, event_data2);
        assert_eq!(tag2, "tag2");

        // Test multi-statement approach (Rich Append) for multiple tags of the same type
        let stream_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let event_data = json!({"multi": "tags"});

        // 1. Insert stream
        client.execute(
            "INSERT INTO mt_streams (id, type) VALUES ($1, $2)",
            &[&stream_id, &"multi_tag_stream"]
        ).await?;

        // 2. Insert event
        let rows = client.query(
            "INSERT INTO mt_events (id, stream_id, version, data, type) VALUES ($1, $2, $3, $4, $5) RETURNING seq_id",
            &[&event_id, &stream_id, &1i32, &event_data, &"multi_tag_event"]
        ).await?;
        let seq_id: i64 = rows[0].get(0);

        // 3. Insert multiple tags (multi-statement approach)
        let insert_tag_sql = schema::get_insert_tag_sql("string");
        client.execute(&insert_tag_sql, &[&"tagA", &seq_id]).await?;
        client.execute(&insert_tag_sql, &[&"tagB", &seq_id]).await?;

        // 4. Verify both tags are present
        let rows = client.query(
            "SELECT value FROM mt_event_tag_string WHERE seq_id = $1 ORDER BY value",
            &[&seq_id]
        ).await?;

        assert_eq!(rows.len(), 2);
        let tag_a: &str = rows[0].get(0);
        let tag_b: &str = rows[1].get(0);
        assert_eq!(tag_a, "tagA");
        assert_eq!(tag_b, "tagB");

        // Test DCB (Dynamic Consistency Boundaries) check
        // 1. Get current sequence
        let row = client.query_one("SELECT last_value FROM mt_events_sequence", &[]).await?;
        let current_seq: i64 = row.get(0);

        // 2. Append a new tagged event
        let stream_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        client.execute(
            "INSERT INTO mt_streams (id, type) VALUES ($1, $2)",
            &[&stream_id, &"dcb_stream"]
        ).await?;
        let row = client.query_one(
            "INSERT INTO mt_events (id, stream_id, version, data, type) VALUES ($1, $2, $3, $4, $5) RETURNING seq_id",
            &[&event_id, &stream_id, &1i32, &json!({"dcb": "test"}), &"dcb_event"]
        ).await?;
        let new_seq: i64 = row.get(0);
        client.execute(&schema::get_insert_tag_sql("string"), &[&"dcb-tag", &new_seq]).await?;

        // 3. Check DCB with last_seen_sequence = current_seq (before append)
        // This should return TRUE (conflict detected)
        let query = dcb::EventTagQuery::new(current_seq)
            .with_tag_and_type("dcb-tag", "dcb_event");
            
        let dcb_sql = dcb::generate_dcb_exists_sql(&query);
        let conflict: bool = client.query_one(&dcb_sql, &[]).await?.get(0);
        assert!(conflict, "Expected DCB conflict not detected");

        // Test select_events_for_query
        let events = dcb::select_events_for_query(&client, &query).await?;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], json!({"dcb": "test"}));

        // 4. Check DCB with last_seen_sequence = new_seq (after append)
        // This should return FALSE (no conflict)
        let query_no_conflict = dcb::EventTagQuery::new(new_seq)
            .with_tag_and_type("dcb-tag", "dcb_event");
            
        let dcb_sql_no_conflict = dcb::generate_dcb_exists_sql(&query_no_conflict);
        let no_conflict: bool = client.query_one(&dcb_sql_no_conflict, &[]).await?.get(0);
        assert!(!no_conflict, "Unexpected DCB conflict detected");

        // Test append_events_conditionally
        let mut client = client; // need to take ownership or use &mut
        let cond_query = dcb::EventTagQuery::new(new_seq)
            .with_tag("dcb-tag");
            
        let cond_events = vec![
            dcb::TaggedEvent {
                id: Uuid::new_v4(),
                stream_id: Uuid::new_v4(),
                version: 1,
                data: json!({"cond": "append"}),
                event_type: "cond_event".to_string(),
                tags: vec!["dcb-tag".to_string()],
            }
        ];
        
        // This should SUCCEED because no new events with "dcb-tag" since new_seq
        let success = dcb::append_events_conditionally(&mut client, Some(&cond_query), cond_events).await?;
        assert!(success);
        
        // Now try to append again with the same query - should FAIL because we just added an event with "dcb-tag"
        let cond_events2 = vec![
            dcb::TaggedEvent {
                id: Uuid::new_v4(),
                stream_id: Uuid::new_v4(),
                version: 1,
                data: json!({"cond": "fail"}),
                event_type: "cond_event".to_string(),
                tags: vec!["dcb-tag".to_string()],
            }
        ];
        let success2 = dcb::append_events_conditionally(&mut client, Some(&cond_query), cond_events2).await?;
        assert!(!success2);

        // Test append_events_conditionally with None query
        let cond_events_none = vec![
            dcb::TaggedEvent {
                id: Uuid::new_v4(),
                stream_id: Uuid::new_v4(),
                version: 1,
                data: json!({"cond": "none"}),
                event_type: "cond_event".to_string(),
                tags: vec!["dcb-tag".to_string()],
            }
        ];
        // This should ALWAYS SUCCEED because there is no DCB check
        let success_none = dcb::append_events_conditionally(&mut client, None, cond_events_none).await?;
        assert!(success_none);

        Ok(())
    }
}
