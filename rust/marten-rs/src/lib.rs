pub mod schema;

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
        client.batch_execute("DROP TABLE IF EXISTS mt_event_tag_test").await?;
        client.batch_execute("DROP TABLE IF EXISTS mt_events").await?;
        client.batch_execute("DROP TABLE IF EXISTS mt_streams").await?;
        client.batch_execute("DROP SEQUENCE IF EXISTS mt_events_sequence").await?;

        // Create schema
        client.batch_execute(schema::CREATE_EVENTS_SEQUENCE).await?;
        client.batch_execute(schema::CREATE_STREAMS_TABLE).await?;
        client.batch_execute(schema::CREATE_EVENTS_TABLE).await?;
        client.batch_execute(&schema::get_create_tag_table_sql("test")).await?;

        // Test insertion
        let stream_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let event_data = json!({"foo": "bar"});

        // Insert stream
        client.execute(
            "INSERT INTO mt_streams (id, type) VALUES ($1, $2)",
            &[&stream_id, &"test_stream"]
        ).await?;

        // Insert event
        client.execute(
            "INSERT INTO mt_events (id, stream_id, version, data, type) VALUES ($1, $2, $3, $4, $5)",
            &[&event_id, &stream_id, &1i32, &event_data, &"test_event"]
        ).await?;

        // Get seq_id
        let row = client.query_one("SELECT seq_id FROM mt_events WHERE id = $1", &[&event_id]).await?;
        let seq_id: i64 = row.get(0);

        // Insert tag
        client.execute(
            "INSERT INTO mt_event_tag_test (value, seq_id) VALUES ($1, $2)",
            &[&"tag1", &seq_id]
        ).await?;

        // Verify data
        let rows = client.query(
            "SELECT e.data, t.value FROM mt_events e JOIN mt_event_tag_test t ON e.seq_id = t.seq_id WHERE e.id = $1",
            &[&event_id]
        ).await?;

        assert_eq!(rows.len(), 1);
        let data: serde_json::Value = rows[0].get(0);
        let tag: &str = rows[0].get(1);

        assert_eq!(data, event_data);
        assert_eq!(tag, "tag1");

        Ok(())
    }
}
