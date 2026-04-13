pub mod schema;
pub mod append;
pub mod read;

pub async fn get_next_sequence_numbers(client: &tokio_postgres::Client, count: usize) -> Result<Vec<i64>, tokio_postgres::Error> {
    if count == 0 {
        return Ok(Vec::new());
    }
    
    let rows = client.query(
        "SELECT nextval('mt_events_sequence') FROM generate_series(1, $1::int)",
        &[&(count as i32)]
    ).await?;
    
    let mut seq_ids = Vec::with_capacity(count);
    for row in rows {
        seq_ids.push(row.get(0));
    }
    
    Ok(seq_ids)
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_postgres::NoTls;
    use uuid::Uuid;
    use serde_json::json;
    use chrono;
    use tokio_postgres::Client;
    use serial_test::serial;

    async fn setup_postgres_client() -> Result<Option<Client>, Box<dyn std::error::Error>> {
        let connection_string = "host=localhost user=marten password=marten dbname=marten";
        let (client, connection) = match tokio_postgres::connect(connection_string, NoTls).await {
            Ok(c) => c,
            Err(e) => {
                eprintln!("Failed to connect to Postgres: {}. Skipping test as database might not be available in this environment.", e);
                return Ok(None);
            }
        };

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        // Cleanup
        let _ = client.batch_execute("DROP FUNCTION IF EXISTS mt_quick_append_events(uuid, varchar, varchar, uuid[], varchar[], varchar[], jsonb[], varchar[])").await;
        let _ = client.batch_execute("DROP TABLE IF EXISTS mt_event_tag_test").await;
        let _ = client.batch_execute("DROP TABLE IF EXISTS mt_event_tag_string").await;
        let _ = client.batch_execute("DROP TABLE IF EXISTS mt_events").await;
        let _ = client.batch_execute("DROP TABLE IF EXISTS mt_streams").await;
        let _ = client.batch_execute("DROP SEQUENCE IF EXISTS mt_events_sequence").await;

        // Create schema
        client.batch_execute(schema::CREATE_EVENTS_SEQUENCE).await?;
        client.batch_execute(schema::CREATE_STREAMS_TABLE).await?;
        client.batch_execute(schema::CREATE_EVENTS_TABLE).await?;
        client.batch_execute(&schema::get_create_tag_table_sql("string")).await?;

        // Create append function
        client.batch_execute(append::CREATE_APPEND_EVENTS_FUNCTION).await?;

        Ok(Some(client))
    }

    #[tokio::test]
    #[serial]
    async fn test_sql_statements() -> Result<(), Box<dyn std::error::Error>> {
        let client = match setup_postgres_client().await? {
            Some(c) => c,
            None => return Ok(()),
        };

        // Test multi-statement approach (Rich Append) for multiple tags of the same type
        let stream_id = Uuid::new_v4();
        let event_id = Uuid::new_v4();
        let event_data = json!({"multi": "tags"});

        // 1. Insert stream
        let stream_version = append::get_stream_version(&client, &stream_id).await?;
        assert_eq!(stream_version, 0);
        append::insert_stream(&client, &stream_id, "multi_tag_stream", 1i32, "DEFAULT").await?;

        // 2. Insert event
        let timestamp = chrono::Utc::now();
        let seq_ids = get_next_sequence_numbers(&client, 1).await?;
        let seq_id_to_insert = seq_ids[0];
        let seq_id = append::insert_event(
            &client,
            &event_data,
            "multi_tag_event",
            &None::<String>,
            &event_id,
            &stream_id,
            1i32,
            &timestamp,
            "DEFAULT",
            seq_id_to_insert,
        ).await?;
        assert_eq!(seq_id, seq_id_to_insert);

        // 3. Insert multiple tags (multi-statement approach)
        append::insert_tag(&client, "string", "tagA", seq_id).await?;
        append::insert_tag(&client, "string", "tagB", seq_id).await?;

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

        // 5. Verify current stream version is 1
        let stream_version = append::get_stream_version(&client, &stream_id).await?;
        assert_eq!(stream_version, 1);

        // 6. Update to next version and insert another event
        let next_version = stream_version + 1;
        let event_id2 = Uuid::new_v4();
        let timestamp = chrono::Utc::now();
        let seq_ids = get_next_sequence_numbers(&client, 1).await?;
        let seq_id_to_insert = seq_ids[0];
        
        let seq_id2 = append::insert_event(
            &client,
            &json!({"second": "event"}),
            "multi_tag_event",
            &None::<String>,
            &event_id2,
            &stream_id,
            next_version,
            &timestamp,
            "DEFAULT",
            seq_id_to_insert,
        ).await?;
        assert_eq!(seq_id2, seq_id_to_insert);

        append::insert_tag(&client, "string", "tagC", seq_id2).await?;
        append::update_stream_version(&client, &stream_id, next_version).await?;

        // 7. Verify the final stream version
        let final_version = append::get_stream_version(&client, &stream_id).await?;
        assert_eq!(final_version, 2);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_mt_quick_append_events() -> Result<(), Box<dyn std::error::Error>> {
        let client = match setup_postgres_client().await? {
            Some(c) => c,
            None => return Ok(()),
        };

        // Test insertion via mt_quick_append_events
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

        let result = append::mt_quick_append_events(
            &client,
            stream_id,
            "test_stream",
            "DEFAULT",
            &event_ids,
            &event_types,
            &dotnet_types,
            &bodies,
            &tags,
        ).await?;

        // Marten's result is [new_version, seq_id1, seq_id2, ...]
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], 2); // new version
        assert_eq!(result[1], 1); // new seq number
        assert_eq!(result[2], 2); // new seq number

        // Verify data
        let event_rows = client.query(
            "SELECT seq_id, id, stream_id, version, data, type, tenant_id, mt_dotnet_type, is_archived FROM mt_events ORDER BY seq_id",
            &[]
        ).await?;

        assert_eq!(event_rows.len(), 2);

        // First event
        let row1 = &event_rows[0];
        assert_eq!(row1.get::<_, i64>(0), 1); // seq_id
        assert_eq!(row1.get::<_, Uuid>(1), event_id1); // id
        assert_eq!(row1.get::<_, Uuid>(2), stream_id); // stream_id
        assert_eq!(row1.get::<_, i32>(3), 1); // version
        assert_eq!(row1.get::<_, serde_json::Value>(4), event_data1); // data
        assert_eq!(row1.get::<_, &str>(5), "test_event_1"); // type
        assert_eq!(row1.get::<_, &str>(6), "DEFAULT"); // tenant_id
        assert_eq!(row1.get::<_, Option<String>>(7), None); // mt_dotnet_type
        assert_eq!(row1.get::<_, bool>(8), false); // is_archived

        // Second event
        let row2 = &event_rows[1];
        assert_eq!(row2.get::<_, i64>(0), 2); // seq_id
        assert_eq!(row2.get::<_, Uuid>(1), event_id2); // id
        assert_eq!(row2.get::<_, Uuid>(2), stream_id); // stream_id
        assert_eq!(row2.get::<_, i32>(3), 2); // version
        assert_eq!(row2.get::<_, serde_json::Value>(4), event_data2); // data
        assert_eq!(row2.get::<_, &str>(5), "test_event_2"); // type
        assert_eq!(row2.get::<_, &str>(6), "DEFAULT"); // tenant_id
        assert_eq!(row2.get::<_, Option<String>>(7), None); // mt_dotnet_type
        assert_eq!(row2.get::<_, bool>(8), false); // is_archived

        let tag_rows = client.query(
            "SELECT value, seq_id FROM mt_event_tag_string ORDER BY seq_id",
            &[]
        ).await?;

        assert_eq!(tag_rows.len(), 2);
        assert_eq!(tag_rows[0].get::<_, &str>(0), "tag1");
        assert_eq!(tag_rows[0].get::<_, i64>(1), 1);
        assert_eq!(tag_rows[1].get::<_, &str>(0), "tag2");
        assert_eq!(tag_rows[1].get::<_, i64>(1), 2);

        Ok(())
    }

    // #[tokio::test]
    // #[serial]
    // async fn test_dcb_marten_style_append() -> Result<(), Box<dyn std::error::Error>> {
    //     let client = match setup_postgres_client().await? {
    //         Some(c) => c,
    //         None => return Ok(()),
    //     };
    //
    //     // Test DCB (Dynamic Consistency Boundaries) check
    //     // 1. Get current sequence
    //     let is_called: bool = client.query_one("SELECT is_called FROM mt_events_sequence", &[]).await?.get(0);
    //     let current_seq: i64 = if !is_called { 0 } else { client.query_one("SELECT last_value FROM mt_events_sequence", &[]).await?.get(0) };
    //
    //     // 2. Append a new tagged event
    //     let dcb_stream_id = Uuid::new_v4();
    //     let event_id = Uuid::new_v4();
    //     append::insert_stream(&client, &dcb_stream_id, "dcb_stream", 1i32, "DEFAULT").await?;
    //     let timestamp = chrono::Utc::now();
    //     let seq_ids = get_next_sequence_numbers(&client, 1).await?;
    //     let seq_id_to_insert = seq_ids[0];
    //     let new_seq = append::insert_event(
    //         &client,
    //         &json!({"dcb": "test"}),
    //         "dcb_event",
    //         &Some("DotNetType".to_string()),
    //         &event_id,
    //         &dcb_stream_id,
    //         1i32,
    //         &timestamp,
    //         "DEFAULT",
    //         seq_id_to_insert,
    //     ).await?;
    //     assert_eq!(new_seq, seq_id_to_insert);
    //     append::insert_tag(&client, "string", "dcb-tag", new_seq).await?;
    //
    //     // 3. Check DCB with last_seen_sequence = current_seq (before append)
    //     // This should return TRUE (conflict detected)
    //     let query = dcb::EventTagQuery::new(current_seq)
    //         .with_tag("dcb-tag");
    //
    //     let conflict = dcb::evaluate_append_condition(&client, &query).await?;
    //     assert!(conflict, "Expected DCB conflict not detected");
    //
    //     // Test select_events_for_query
    //     let events = dcb::select_events_for_query(&client, &query).await?;
    //     assert_eq!(events.len(), 1);
    //     assert_eq!(events[0].data, json!({"dcb": "test"}));
    //     assert_eq!(events[0].event_type, "dcb_event");
    //     assert_eq!(events[0].dotnet_type, Some("DotNetType".to_string()));
    //     assert_eq!(events[0].id, event_id);
    //     assert_eq!(events[0].stream_id, dcb_stream_id);
    //     assert_eq!(events[0].version, 1);
    //     // Marten's fetch does not return tags; it only retrieves matching events.
    //
    //     // Test with multiple tags
    //     let stream_id_multi = Uuid::new_v4();
    //     let event_id_multi = Uuid::new_v4();
    //     client.execute(
    //         "INSERT INTO mt_streams (id, type, version, tenant_id) VALUES ($1, $2, $3, $4)",
    //         &[&stream_id_multi, &"multi_tag_stream", &1i32, &"DEFAULT"]
    //     ).await?;
    //     let timestamp = chrono::Utc::now();
    //     let seq_ids = get_next_sequence_numbers(&client, 1).await?;
    //     let seq_id_to_insert = seq_ids[0];
    //     let multi_seq = append::insert_event(
    //         &client,
    //         &json!({"multi": "tags"}),
    //         "multi_tag_event",
    //         &None::<String>,
    //         &event_id_multi,
    //         &stream_id_multi,
    //         1i32,
    //         &timestamp,
    //         "DEFAULT",
    //         seq_id_to_insert,
    //     ).await?;
    //     assert_eq!(multi_seq, seq_id_to_insert);
    //     append::insert_tag(&client, "string", "tag-1", multi_seq).await?;
    //     append::insert_tag(&client, "string", "tag-2", multi_seq).await?;
    //
    //     let query_multi = dcb::EventTagQuery::new(current_seq).with_tag("tag-1");
    //     let events_multi = dcb::select_events_for_query(&client, &query_multi).await?;
    //     assert_eq!(events_multi.len(), 1);
    //     assert_eq!(events_multi[0].data, json!({"multi": "tags"}));
    //     // Note: Marten returns matching events without their tags.
    //
    //     // 4. Check DCB with last_seen_sequence = new_seq (after append)
    //     // This should return FALSE (no conflict)
    //     let query_no_conflict = dcb::EventTagQuery::new(new_seq)
    //         .with_tag("dcb-tag");
    //
    //     let no_conflict = dcb::evaluate_append_condition(&client, &query_no_conflict).await?;
    //     assert!(!no_conflict, "Unexpected DCB conflict detected");
    //
    //     // Test rich_append_events
    //     let stream_id = Uuid::new_v4();
    //     let cond_query = dcb::EventTagQuery::new(new_seq)
    //         .with_tag("dcb-tag");
    //
    //     let cond_events = vec![
    //         append::NewEvent {
    //             id: Uuid::new_v4(),
    //             stream_id,
    //             version: 1,
    //             data: json!({"cond": "append"}),
    //             event_type: "cond_event".to_string(),
    //             dotnet_type: Some("CondDotNetType".to_string()),
    //             tags: vec!["dcb-tag".to_string()],
    //         }
    //     ];
    //
    //     // This should SUCCEED because no new events with "dcb-tag" since new_seq
    //     let mut client = client;
    //     let (success, seq_ids) = append::conditional_rich_append_events(&mut client, Some(&cond_query), cond_events).await?;
    //     assert!(success);
    //     assert_eq!(seq_ids.len(), 1);
    //
    //     // Verify result of first conditional append
    //     let results = dcb::select_events_for_query(&client, &cond_query).await?;
    //     assert_eq!(results.len(), 1);
    //     assert_eq!(results[0].data, json!({"cond": "append"}));
    //     assert_eq!(results[0].dotnet_type, Some("CondDotNetType".to_string()));
    //
    //     // Now try to append again with the same query - should FAIL because we just added an event with "dcb-tag"
    //     let cond_events2 = vec![
    //         append::NewEvent {
    //             id: Uuid::new_v4(),
    //             stream_id,
    //             version: 2,
    //             data: json!({"cond": "fail"}),
    //             event_type: "cond_event".to_string(),
    //             dotnet_type: None,
    //             tags: vec!["dcb-tag".to_string()],
    //         }
    //     ];
    //     let (success2, seq_ids2) = append::conditional_rich_append_events(&mut client, Some(&cond_query), cond_events2).await?;
    //     assert!(!success2);
    //     assert_eq!(seq_ids2.len(), 0);
    //
    //     // Verify result of second conditional append (should NOT contain the failed event)
    //     let results2 = dcb::select_events_for_query(&client, &cond_query).await?;
    //     assert_eq!(results2.len(), 1);
    //     assert_eq!(results2[0].data, json!({"cond": "append"}));
    //
    //     // Test rich_append_events with None query
    //     let cond_events_none = vec![
    //         append::NewEvent {
    //             id: Uuid::new_v4(),
    //             stream_id: Uuid::new_v4(),
    //             version: 1,
    //             data: json!({"cond": "none"}),
    //             event_type: "cond_event".to_string(),
    //             dotnet_type: None,
    //             tags: vec!["dcb-tag".to_string()],
    //         }
    //     ];
    //     // cond_events_none stream_id should have version 1
    //     // (We know its index and it was the only one in the batch)
    //     let none_stream_id = cond_events_none[0].stream_id;
    //     let (success_none, seq_ids_none) = append::conditional_rich_append_events(&mut client, None, cond_events_none).await?;
    //     assert!(success_none);
    //     assert_eq!(seq_ids_none.len(), 1);
    //
    //     // Verify result of append without query (should be able to see it using its own tag)
    //     let query_none = dcb::EventTagQuery::new(new_seq).with_tag("dcb-tag");
    //     let results_none = dcb::select_events_for_query(&client, &query_none).await?;
    //     // Should have "cond": "append" and "cond": "none"
    //     assert_eq!(results_none.len(), 2);
    //     let datas: Vec<serde_json::Value> = results_none.into_iter().map(|e| e.data).collect();
    //     assert!(datas.contains(&json!({"cond": "append"})));
    //     assert!(datas.contains(&json!({"cond": "none"})));
    //
    //     // Check stream version
    //     let rows = client.query("SELECT id, version FROM mt_streams", &[]).await?;
    //     let mut versions = std::collections::HashMap::new();
    //     for row in rows {
    //         let id: Uuid = row.get(0);
    //         let version: Option<i32> = row.get(1);
    //         versions.insert(id, version);
    //     }
    //
    //     // stream_id from first conditional append should have version 1 (second append failed)
    //     assert_eq!(versions.get(&stream_id).cloned().flatten(), Some(1));
    //     // cond_events_none stream_id should have version 1
    //     assert_eq!(versions.get(&none_stream_id).cloned().flatten(), Some(1));
    //
    //     Ok(())
    // }
    //
    // #[tokio::test]
    // #[serial]
    // async fn test_throughput() -> Result<(), Box<dyn std::error::Error>> {
    //     let mut client = match setup_postgres_client().await? {
    //         Some(c) => c,
    //         None => return Ok(()),
    //     };
    //
    //     let payload_size = 256;
    //     let iterations = 1000; // Increased to get more representative throughput
    //     let total_events = iterations;
    //     let payload_data = "a".repeat(payload_size);
    //
    //     println!("Starting throughput test: {} iterations of 1 event with {} byte payload", iterations, payload_size);
    //
    //     let start = std::time::Instant::now();
    //
    //     for _ in 0..iterations {
    //         let events = vec![append::NewEvent {
    //             id: Uuid::new_v4(),
    //             stream_id: Uuid::new_v4(),
    //             version: 1,
    //             data: json!({"payload": payload_data}),
    //             event_type: "benchmark_event".to_string(),
    //             dotnet_type: None,
    //             tags: vec!["benchmark".to_string()],
    //         }];
    //
    //         let (success, _) = append::conditional_rich_append_events(&mut client, None, events).await?;
    //         assert!(success);
    //     }
    //
    //     let duration = start.elapsed();
    //     let eps = total_events as f64 / duration.as_secs_f64();
    //
    //     println!("Throughput: {:.2} events/second", eps);
    //     println!("Total time: {:?}", duration);
    //
    //     Ok(())
    // }
}
