pub mod schema;
pub mod append;

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

        Ok(())
    }
}
