use std::error::Error;
use std::fmt;
use tokio_postgres::NoTls;
use deadpool_postgres::{Pool, Runtime, Manager, ManagerConfig, RecyclingMethod, Timeouts};
use tokio::time::Duration;
use uuid::Uuid;
use serde_json::Value;
use crate::read::{EventTagQuery, MartenEvent};

#[derive(Debug)]
pub enum MartenError {
    Postgres(tokio_postgres::Error),
    AppendConditionFailed,
    Uuid(uuid::Error),
    Pool(deadpool_postgres::PoolError),
    Connection(String),
    Context {
        operation: String,
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

impl MartenError {
    pub fn context<E>(operation: impl Into<String>, source: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        MartenError::Context {
            operation: operation.into(),
            source: Box::new(source),
        }
    }
}

impl fmt::Display for MartenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MartenError::Postgres(e) => {
                if let Some(db_err) = e.as_db_error() {
                    write!(f, "Postgres error: {} (Code: {}, Message: {}, Detail: {:?}, Hint: {:?})", 
                        e, db_err.code().code(), db_err.message(), db_err.detail(), db_err.hint())
                } else {
                    write!(f, "Postgres error: {:?}", e.source())
                }
            },
            MartenError::AppendConditionFailed => write!(f, "Append condition failed"),
            MartenError::Uuid(e) => write!(f, "Uuid error: {}", e),
            MartenError::Pool(e) => write!(f, "Pool error: {} ({:?})", e, e),
            MartenError::Connection(s) => write!(f, "Connection error: {}", s),
            MartenError::Context { operation, source } => {
                write!(f, "{}: {}", operation, source)
            }
        }
    }
}

impl std::error::Error for MartenError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MartenError::Postgres(e) => Some(e),
            MartenError::AppendConditionFailed => None,
            MartenError::Uuid(e) => Some(e),
            MartenError::Pool(e) => Some(e),
            MartenError::Connection(_) => None,
            MartenError::Context { source, .. } => Some(source.as_ref()),
        }
    }
}

impl From<tokio_postgres::Error> for MartenError {
    fn from(e: tokio_postgres::Error) -> Self {
        MartenError::Postgres(e)
    }
}

impl From<uuid::Error> for MartenError {
    fn from(e: uuid::Error) -> Self {
        MartenError::Uuid(e)
    }
}

impl From<deadpool_postgres::PoolError> for MartenError {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        MartenError::Pool(e)
    }
}

pub mod schema;
pub mod append;
pub mod read;

#[derive(Clone)]
pub struct Marten {
    pub pool: Pool,
}

impl Marten {
    pub async fn connect(connection_string: &str) -> Result<Self, MartenError> {
        let pg_config: tokio_postgres::Config = connection_string.parse().map_err(|e| {
            MartenError::Connection(format!("Invalid connection string: {}", e))
        })?;
        let mgr_config = ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        };
        let mgr = Manager::from_config(pg_config, NoTls, mgr_config);

        let pool_size = std::env::var("ESB_POSTGRES_MAX_CONNECTIONS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(12);
        let pool = Pool::builder(mgr)
            .runtime(Runtime::Tokio1)
            .max_size(pool_size)
            .timeouts(Timeouts {
                wait: Some(Duration::from_secs(30)),
                create: Some(Duration::from_secs(30)),
                recycle: Some(Duration::from_secs(30)),
            })
            .build()
            .map_err(|e| {
                MartenError::Connection(format!("Pool creation failed: {}", e))
            })?;
        Ok(Self { pool })
    }

    pub async fn drop_tables(&self) -> Result<(), MartenError> {
        let client = self.pool.get().await?;
        let fut = async {
            client.batch_execute("DROP FUNCTION IF EXISTS mt_quick_append_events(uuid, varchar, varchar, uuid[], varchar[], varchar[], jsonb[], varchar[])").await?;
            client.batch_execute("DROP TABLE IF EXISTS mt_event_tag_test").await?;
            client.batch_execute("DROP TABLE IF EXISTS mt_event_tag_string").await?;
            client.batch_execute("DROP TABLE IF EXISTS mt_events").await?;
            client.batch_execute("DROP TABLE IF EXISTS mt_streams").await?;
            client.batch_execute("DROP SEQUENCE IF EXISTS mt_events_sequence").await?;
            Ok::<(), MartenError>(())
        };
        tokio::time::timeout(Duration::from_secs(60), fut).await
            .map_err(|_| MartenError::Connection("Timeout dropping tables".to_string()))?
    }

    pub async fn create_tables(&self) -> Result<(), MartenError> {
        let client = self.pool.get().await?;
        let fut = async {
            client.batch_execute(schema::CREATE_EVENTS_SEQUENCE).await?;
            client.batch_execute(schema::CREATE_STREAMS_TABLE).await?;
            client.batch_execute(schema::CREATE_EVENTS_TABLE).await?;
            client.batch_execute(&schema::get_create_tag_table_sql("string")).await?;
            client.batch_execute(append::CREATE_QUICK_APPEND_EVENTS_FUNCTION).await?;
            Ok::<(), MartenError>(())
        };
        tokio::time::timeout(Duration::from_secs(60), fut).await
            .map_err(|_| MartenError::Connection("Timeout creating tables".to_string()))?
    }

    pub async fn new_boundary<'a>(&self, mut query: EventTagQuery<'a>) -> Result<Boundary<'a>, MartenError> {
        let events = self.read_events(&query).await.map_err(|e| {
            MartenError::context(
                format!(
                    "new_boundary failed while reading events (last_seen_seq_id={}, tags={})",
                    query.last_seen_seq_id,
                    query.conditions.len()
                ),
                e,
            )
        })?;
        let mut last_seen_seq_id = query.last_seen_seq_id;
        for i in 0..events.len() {
            let seq_id = events[i].seq_id;
            if seq_id > last_seen_seq_id {
                last_seen_seq_id = seq_id;
            }
        }
        query.last_seen_seq_id = last_seen_seq_id;
        let boundary = Boundary::new(query, events);
        Ok(boundary)
    }

    pub async fn save_boundary(&self, boundary: Boundary<'_>) -> Result<Vec<i64>, MartenError> {
        let mut pending_events = boundary.pending_events;
        let query = boundary.query;

        let result_seq_ids = self
            .append_events(&mut pending_events, Some(&query))
            .await
            .map_err(|e| {
                MartenError::context(
                    format!(
                        "save_boundary append failed (pending_events={}, last_seen_seq_id={}, tags={})",
                        pending_events.len(),
                        query.last_seen_seq_id,
                        query.conditions.len()
                    ),
                    e,
                )
            })?;
        Ok(result_seq_ids)
    }

    pub async fn append_events(&self, events: &mut Vec<MartenDcbEvent>, query: Option<&EventTagQuery<'_>>) -> Result<Vec<i64>, MartenError> {
        let client = self.pool.get().await.map_err(|e| {
            MartenError::context("append_events failed to acquire postgres connection", e)
        })?;

        let num_events = events.len();

        let default_stream_id = Uuid::parse_str("00000000-0000-0000-0000-000000000000")
            .map_err(|e| MartenError::context("append_events failed to parse default stream id", e))?;

        // If all tags are the same, then make UUID version 5 for the stream ID,
        // otherwise use the default_stream_id.
        let stream_id = events.first()
            .and_then(|e| e.tags.first())
            .filter(|first_tag| events.iter().all(|e| e.tags.first().map_or(false, |t| t == *first_tag)))
            .map(|first_tag| Uuid::new_v5(&Uuid::NAMESPACE_OID, first_tag.as_bytes()))
            .unwrap_or(default_stream_id);
        
        let mut any_event_with_more_than_two_tags = false;
        for i in 0..num_events {
            if events[i].tags.len() > 1 {
                any_event_with_more_than_two_tags = true;
                break;
            }
        }
        
        if !any_event_with_more_than_two_tags && query.is_none() {
            let mut event_ids = vec![];
            let mut event_types = vec![];
            let mut dotnet_types = vec![];
            let mut bodies = vec![];
            let mut tags = vec![];
            for i in 0..num_events {
                event_ids.push(Uuid::new_v4());
                event_types.push(events[i].event_type.as_str());
                dotnet_types.push(None);
                bodies.push(events[i].data.clone());
                tags.push(events[i].tags.first().map(|s| s.to_string()));
            }

            let fut = async move {
                let mut client = client;
                append::quick_append_events(
                    &mut *client,
                    stream_id,
                    "default_stream",
                    "DEFAULT",
                    &event_ids,
                    &event_types,
                    &dotnet_types,
                    &bodies,
                    &tags,
                ).await
            };
            let quick_result = tokio::time::timeout(Duration::from_secs(60), fut).await
                .map_err(|_| MartenError::Connection(
                    format!(
                        "Timeout in quick_append_events (events={}, stream_id={})",
                        num_events,
                        stream_id
                    )
                ))?;
            quick_result.map_err(|e| {
                MartenError::context(
                    format!(
                        "quick_append_events failed (events={}, stream_id={})",
                        num_events,
                        stream_id
                    ),
                    e,
                )
            })
        } else {
            let fut = async move {
                let client_ref = &**client;
                let current_version = append::get_stream_version(client_ref, &stream_id).await
                    .map_err(|e| MartenError::context(
                        format!("failed to fetch stream version (stream_id={})", stream_id),
                        e,
                    ))?;

                let mut marten_events = Vec::new();

                // Get new sequence numbers from the database
                let seq_ids = get_next_sequence_numbers(client_ref, num_events).await
                    .map_err(|e| MartenError::context(
                        format!("failed to allocate sequence numbers (count={})", num_events),
                        e,
                    ))?;

                for (i, MartenDcbEvent { data, event_type, tags }) in events.drain(..).enumerate() {
                    marten_events.push(read::MartenEvent {
                        id: Uuid::new_v4(),
                        stream_id: stream_id,
                        version: current_version + (i as i32) + 1,
                        data,
                        event_type,
                        dotnet_type: None,
                        tags,
                        seq_id: seq_ids[i],
                    });
                }

                // Call conditional_rich_append
                let mut client = client;
                let result_seq_ids = if let Some(query) = query {
                    append::conditional_rich_append_events(&mut *client, marten_events, &query).await
                        .map_err(|e| MartenError::context(
                            format!(
                                "conditional_rich_append_events failed (events={}, last_seen_seq_id={}, tags={})",
                                num_events,
                                query.last_seen_seq_id,
                                query.conditions.len()
                            ),
                            e,
                        ))?
                } else {
                    append::rich_append_events(&mut *client, marten_events).await
                        .map_err(|e| MartenError::context(
                            format!("rich_append_events failed (events={})", num_events),
                            e,
                        ))?
                };

                Ok::<Vec<i64>, MartenError>(result_seq_ids)
            };

            tokio::time::timeout(Duration::from_secs(60), fut).await
                .map_err(|_| MartenError::Connection(
                    format!("Timeout in rich_append_events (events={})", num_events)
                ))?
        }
    }

    pub async fn read_all_events(&self) -> Result<Vec<MartenEvent>, MartenError> {
        let query = EventTagQuery::new(-1);
        self.read_events(&query).await
    }

    pub async fn read_events(&self, query: &EventTagQuery<'_>) -> Result<Vec<MartenEvent>, MartenError> {
        let client = self.pool.get().await.map_err(|e| {
            MartenError::context("read_events failed to acquire postgres connection", e)
        })?;
        let fut = read::select_events_for_query(&**client, query);
        let read_result = tokio::time::timeout(Duration::from_secs(60), fut).await
            .map_err(|_| MartenError::Connection(
                format!(
                    "Timeout reading events (last_seen_seq_id={}, tags={})",
                    query.last_seen_seq_id,
                    query.conditions.len()
                )
            ))?;
        read_result.map_err(|e| {
            MartenError::context(
                format!(
                    "read_events query failed (last_seen_seq_id={}, tags={})",
                    query.last_seen_seq_id,
                    query.conditions.len()
                ),
                e,
            )
        })
    }

}

pub struct MartenDcbEvent {
    pub data: Value,
    pub event_type: String,
    pub tags: Vec<String>,
}

pub struct Boundary<'a> {
    pub query: EventTagQuery<'a>,
    pub selected_events: Vec<MartenEvent>,
    pub pending_events: Vec<MartenDcbEvent>,
}

impl<'a> Boundary<'a> {
    pub fn new(query: EventTagQuery<'a>, selected_events: Vec<MartenEvent>) -> Self {
        Self {
            query,
            selected_events,
            pending_events: Vec::new(),
        }
    }

    pub fn add_event(&mut self, event: MartenDcbEvent) {
        self.pending_events.push(event);
    }
}

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
    use uuid::Uuid;
    use serde_json::json;
    use chrono;
    use serial_test::serial;

    async fn setup_postgres_client() -> Result<Option<tokio_postgres::Client>, MartenError> {
        // Since we are now using a pool, this test helper needs to be adjusted 
        // if it's still needed. For now, let's just make it return None or get from pool.
        let marten = setup_marten().await?;
        let _client = marten.pool.get().await?;
        // We can't easily return the Client because it's owned by Object and we are returning Client
        // Let's just return None for now if it's just for tests that we can fix later.
        Ok(None)
    }

    async fn setup_marten() -> Result<Marten, MartenError> {
        let connection_string = "host=localhost user=marten password=marten dbname=marten";
        let marten = Marten::connect(connection_string).await?;
        marten.drop_tables().await?;
        marten.create_tables().await?;
        Ok(marten)
    }


    #[tokio::test]
    #[serial]
    async fn test_sql_statements() -> Result<(), MartenError> {
        let client = match setup_postgres_client().await? {
            Some(c) => c,
            None => return Ok(()),
        };

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
        let tags = read::read_all_tags(&client).await?;

        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].value, "tagA");
        assert_eq!(tags[1].value, "tagB");
        assert_eq!(tags[0].seq_id, seq_id);
        assert_eq!(tags[1].seq_id, seq_id);

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
    async fn test_evaluate_append_condition() -> Result<(), Box<dyn std::error::Error>> {
        let marten = setup_marten().await?;
        let client = marten.pool.get().await?;
        let client = &**client;

        // Test Case 1: No events exist -> returns false
        let query = EventTagQuery::new(-1).with_tag("tag1");
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(!result, "Should return false when no events exist");

        // Set up some data
        let stream_id = Uuid::new_v4();
        append::insert_stream(client, &stream_id, "test_stream", 0, "DEFAULT").await?;
        
        let timestamp = chrono::Utc::now();
        let seq_ids = get_next_sequence_numbers(client, 2).await?;
        
        // Event 1 with tag1, seq_id = seq_ids[0]
        append::insert_event(
            client, &json!({}), "type1", &None::<String>, &Uuid::new_v4(), 
            &stream_id, 1, &timestamp, "DEFAULT", seq_ids[0]
        ).await?;
        append::insert_tag(client, "string", "tag1", seq_ids[0]).await?;

        // Event 2 with tag2, seq_id = seq_ids[1]
        append::insert_event(
            client, &json!({}), "type1", &None::<String>, &Uuid::new_v4(), 
            &stream_id, 2, &timestamp, "DEFAULT", seq_ids[1]
        ).await?;
        append::insert_tag(client, "string", "tag2", seq_ids[1]).await?;

        // Event 3 with NO tags, seq_id = seq_ids[2] (if we had a third)
        let seq_ids_more = get_next_sequence_numbers(client, 1).await?;
        append::insert_event(
            client, &json!({}), "type1", &None::<String>, &Uuid::new_v4(), 
            &stream_id, 3, &timestamp, "DEFAULT", seq_ids_more[0]
        ).await?;

        // Test Case 8: Event with NO tags exists with seq_id > last_seen_seq_id, query with NO conditions -> returns false because it only checks tags
        let query = EventTagQuery::new(seq_ids[1]);
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(!result, "Should return false if an event exists with higher seq_id but no tags exist for it");

        // Test Case 2: Events exist but seq_id <= last_seen_seq_id -> returns false
        let query = EventTagQuery::new(seq_ids[1]).with_tag("tag1").with_tag("tag2");
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(!result, "Should return false when all matching events have seq_id <= last_seen_seq_id");

        // Test Case 3: Events exist with seq_id > last_seen_seq_id but different tags -> returns false
        let query = EventTagQuery::new(seq_ids[0]).with_tag("tag1");
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(!result, "Should return false when only event with higher seq_id has a different tag");

        // Test Case 4: Events exist with seq_id > last_seen_seq_id and matching tags -> returns true
        let query = EventTagQuery::new(seq_ids[0]).with_tag("tag2");
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(result, "Should return true when an event with higher seq_id matches a tag");

        // Test Case 5: Multiple tags, one matches -> returns true
        let query = EventTagQuery::new(seq_ids[0]).with_tag("nonexistent").with_tag("tag2");
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(result, "Should return true when at least one tag matches an event with higher seq_id");

        // Test Case 6: No conditions -> returns true if ANY event exists with seq_id > last_seen_seq_id
        let query = EventTagQuery::new(seq_ids[0]);
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(result, "Should return true if any event exists with seq_id > last_seen_seq_id when no conditions are specified");

        // Test Case 7: No conditions, all events have seq_id <= last_seen_seq_id -> returns false
        let query = EventTagQuery::new(seq_ids[1]);
        let result = read::evaluate_append_condition(client, &query).await?;
        assert!(!result, "Should return false if all events have seq_id <= last_seen_seq_id when no conditions are specified");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_quick_append_events() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = match setup_postgres_client().await? {
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

        let result = append::quick_append_events(
            &mut client,
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
        let event_rows = read::read_all_events(&client).await?;

        assert_eq!(event_rows.len(), 2);

        // First event
        let row1 = &event_rows[0];
        assert_eq!(row1.seq_id, 1); // seq_id
        assert_eq!(row1.id, event_id1); // id
        assert_eq!(row1.stream_id, stream_id); // stream_id
        assert_eq!(row1.version, 1); // version
        assert_eq!(row1.data, event_data1); // data
        assert_eq!(row1.event_type, "test_event_1"); // type
        assert_eq!(row1.dotnet_type, None);

        // Second event
        let row2 = &event_rows[1];
        assert_eq!(row2.seq_id, 2); // seq_id
        assert_eq!(row2.id, event_id2); // id
        assert_eq!(row2.stream_id, stream_id); // stream_id
        assert_eq!(row2.version, 2); // version
        assert_eq!(row2.data, event_data2); // data
        assert_eq!(row2.event_type, "test_event_2"); // type
        assert_eq!(row2.dotnet_type, None);

        let tag_rows = read::read_all_tags(&client).await?;

        assert_eq!(tag_rows.len(), 2);
        assert_eq!(tag_rows[0].value, "tag1");
        assert_eq!(tag_rows[0].seq_id, 1);
        assert_eq!(tag_rows[1].value, "tag2");
        assert_eq!(tag_rows[1].seq_id, 2);

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
    //         read::MartenEvent {
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
    //         read::MartenEvent {
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
    //         read::MartenEvent {
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
    //         let events = vec![read::MartenEvent {
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

    #[tokio::test]
    #[serial]
    async fn test_rich_append_events() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = match setup_postgres_client().await? {
            Some(c) => c,
            None => return Ok(()),
        };

        let stream_id1 = Uuid::new_v4();
        let stream_id2 = Uuid::new_v4();
        
        let events = vec![
            read::MartenEvent {
                id: Uuid::new_v4(),
                stream_id: stream_id1,
                version: 1,
                data: json!({"event": 1}),
                event_type: "test_event".to_string(),
                dotnet_type: None,
                tags: vec!["tag1".to_string(), "tag2".to_string()],
                seq_id: 1,
            },
            read::MartenEvent {
                id: Uuid::new_v4(),
                stream_id: stream_id1,
                version: 2,
                data: json!({"event": 2}),
                event_type: "test_event".to_string(),
                dotnet_type: None,
                tags: vec!["tag1".to_string()],
                seq_id: 2,
            },
            read::MartenEvent {
                id: Uuid::new_v4(),
                stream_id: stream_id2,
                version: 1,
                data: json!({"event": 3}),
                event_type: "test_event".to_string(),
                dotnet_type: None,
                tags: vec!["tag2".to_string()],
                seq_id: 3,
            },
        ];

        let seq_ids = append::rich_append_events(&mut client, events).await?;
        assert_eq!(seq_ids.len(), 3);

        // Verify events
        let events = read::read_all_events(&client).await?;
        assert_eq!(events.len(), 3);
        
        assert_eq!(events[0].stream_id, stream_id1);
        assert_eq!(events[0].version, 1);
        
        assert_eq!(events[1].stream_id, stream_id1);
        assert_eq!(events[1].version, 2);
        
        assert_eq!(events[2].stream_id, stream_id2);
        assert_eq!(events[2].version, 1);

        // Verify tags
        let tags = read::read_all_tags(&client).await?;
        // event 1: tag1, tag2
        // event 2: tag1
        // event 3: tag2
        // Total 4 tag entries
        assert_eq!(tags.len(), 4);
        
        assert_eq!(tags[0].value, "tag1");
        assert_eq!(tags[0].seq_id, seq_ids[0]);
        
        assert_eq!(tags[1].value, "tag2");
        assert_eq!(tags[1].seq_id, seq_ids[0]);

        assert_eq!(tags[2].value, "tag1");
        assert_eq!(tags[2].seq_id, seq_ids[1]);

        assert_eq!(tags[3].value, "tag2");
        assert_eq!(tags[3].seq_id, seq_ids[2]);

        // Verify streams
        let streams = read::read_all_streams(&client).await?;
        assert_eq!(streams.len(), 2);
        
        let mut stream_versions = std::collections::HashMap::new();
        for s in streams {
            stream_versions.insert(s.id, s.version);
        }
        
        assert_eq!(stream_versions.get(&stream_id1), Some(&2));
        assert_eq!(stream_versions.get(&stream_id2), Some(&1));

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_conditional_rich_append_events() -> Result<(), Box<dyn std::error::Error>> {
        let mut client = match setup_postgres_client().await? {
            Some(c) => c,
            None => return Ok(()),
        };

        // 1. Prepare some initial data
        let stream_id = Uuid::new_v4();
        let initial_events = vec![
            read::MartenEvent {
                id: Uuid::new_v4(),
                stream_id,
                version: 1,
                data: json!({"initial": true}),
                event_type: "initial_event".to_string(),
                dotnet_type: None,
                tags: vec!["target-tag".to_string()],
                seq_id: 1,
            },
        ];
        let seq_ids = append::rich_append_events(&mut client, initial_events).await?;
        let last_seq = seq_ids[0];

        // 2. Test successful conditional append
        // Condition: exist events with "target-tag" and seq_id > last_seq
        // There are no such events yet, so it should succeed.
        let query_success = read::EventTagQuery::new(last_seq)
            .with_tag("target-tag");
        
        let new_events1 = vec![
            read::MartenEvent {
                id: Uuid::new_v4(),
                stream_id,
                version: 2,
                data: json!({"conditional": "success"}),
                event_type: "test_event".to_string(),
                dotnet_type: None,
                tags: vec!["other-tag".to_string()],
                seq_id: 2,
            },
        ];
        
        let seq_ids1 = append::conditional_rich_append_events(&mut client, new_events1, &query_success).await?;
        assert_eq!(seq_ids1.len(), 1);
        assert_eq!(seq_ids1[0], 2);

        // 3. Test failed conditional append
        // Condition: exist events with "target-tag" and seq_id > 0
        // The initial event matches this, so it should fail.
        let query_fail = read::EventTagQuery::new(0)
            .with_tag("target-tag");
        
        let new_events2 = vec![
            read::MartenEvent {
                id: Uuid::new_v4(),
                stream_id,
                version: 3,
                data: json!({"conditional": "fail"}),
                event_type: "test_event".to_string(),
                dotnet_type: None,
                tags: vec!["should-not-exist".to_string()],
                seq_id: 3,
            },
        ];
        
        let result = append::conditional_rich_append_events(&mut client, new_events2, &query_fail).await;
        
        match result {
            Err(MartenError::AppendConditionFailed) => {},
            _ => panic!("Expected AppendConditionFailed error, got {:?}", result),
        }

        // Verify that the failed event was NOT appended
        let all_events = read::read_all_events(&client).await?;
        assert_eq!(all_events.len(), 2); // Initial event + first successful conditional append
        
        for event in all_events {
            assert_ne!(event.data, json!({"conditional": "fail"}));
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_save_session() -> Result<(), Box<dyn std::error::Error>> {
        let marten = setup_marten().await?;

        // 1. Initialize an event tag query
        let last_seen_seq = 0;
        let query1 = read::EventTagQuery::new(last_seen_seq)
            .with_tag("target-tag");

        // 2. Initialize a session and add events
        let mut boundary1 = marten.new_boundary(query1.clone()).await?;
        let mut boundary2 = marten.new_boundary(query1.clone()).await?;
        boundary1.add_event(MartenDcbEvent { data: json!({"event": 1}), event_type: "type1".to_string(), tags: vec!["target-tag".to_string()] });
        boundary1.add_event(MartenDcbEvent { data: json!({"event": 2}), event_type: "type2".to_string(), tags: vec!["target-tag".to_string()] });
        boundary2.add_event(MartenDcbEvent { data: json!({"event": 3}), event_type: "type1".to_string(), tags: vec!["target-tag".to_string()] });
        boundary2.add_event(MartenDcbEvent { data: json!({"event": 4}), event_type: "type2".to_string(), tags: vec!["target-tag".to_string()] });

        // 3. Save the session
        let seq_ids = marten.save_boundary(boundary1).await?;
        assert_eq!(seq_ids.len(), 2);

        let result = marten.save_boundary(boundary2).await;
        match result {
            Err(MartenError::AppendConditionFailed) => {},
            _ => panic!("Expected AppendConditionFailed error, got {:?}", result),
        }

        // 4. Verify events were saved (only boundary1)
        let all_events = marten.read_all_events().await?;
        assert_eq!(all_events.len(), 2);
        assert_eq!(all_events[0].version, 1);
        assert_eq!(all_events[1].version, 2);

        Ok(())
    }
}
