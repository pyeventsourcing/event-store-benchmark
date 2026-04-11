pub struct DcbCondition<'a> {
    pub tag_value: &'a str,
    pub event_type: Option<&'a str>,
}

pub struct EventTagQuery<'a> {
    pub conditions: Vec<DcbCondition<'a>>,
    pub last_seen_sequence: i64,
}

impl<'a> EventTagQuery<'a> {
    pub fn new(last_seen_sequence: i64) -> Self {
        Self {
            conditions: Vec::new(),
            last_seen_sequence,
        }
    }

    pub fn with_tag(mut self, value: &'a str) -> Self {
        self.conditions.push(DcbCondition {
            tag_value: value,
            event_type: None,
        });
        self
    }

    pub fn with_tag_and_type(mut self, tag_value: &'a str, event_type: &'a str) -> Self {
        self.conditions.push(DcbCondition {
            tag_value,
            event_type: Some(event_type),
        });
        self
    }
}

use tokio_postgres::{Client, Error};
use serde_json::Value;

pub fn generate_select_events_sql(query: &EventTagQuery) -> String {
    let mut sql = String::from("SELECT e.seq_id, e.id, e.stream_id, e.version, e.data, e.type FROM mt_events e");
    
    // Marten joins to the tag table(s) to apply filters
    sql.push_str(" LEFT JOIN mt_event_tag_string t0 ON e.seq_id = t0.seq_id");
    
    sql.push_str(&format!(" WHERE (e.seq_id > {})", query.last_seen_sequence));

    if !query.conditions.is_empty() {
        sql.push_str(" AND (");
        for (i, condition) in query.conditions.iter().enumerate() {
            if i > 0 {
                sql.push_str(" OR ");
            }
            sql.push_str(&format!("(t0.value = '{}'", condition.tag_value));
            if let Some(event_type) = condition.event_type {
                sql.push_str(&format!(" AND e.type = '{}'", event_type));
            }
            sql.push_str(")");
        }
        sql.push_str(")");
    }
    
    sql.push_str(" ORDER BY e.seq_id");
    sql
}

pub fn generate_dcb_exists_sql(query: &EventTagQuery) -> String {
    let mut sql = String::from("SELECT EXISTS (SELECT 1 FROM mt_event_tag_string t0");

    // Join to mt_events only if we need event type filtering
    let has_event_type_filter = query.conditions.iter().any(|c| c.event_type.is_some());
    if has_event_type_filter {
        sql.push_str(" INNER JOIN mt_events e ON t0.seq_id = e.seq_id");
    }

    sql.push_str(&format!(" WHERE (t0.seq_id > {})", query.last_seen_sequence));

    if !query.conditions.is_empty() {
        // Build OR conditions
        sql.push_str(" AND (");
        for (i, condition) in query.conditions.iter().enumerate() {
            if i > 0 {
                sql.push_str(" OR ");
            }

            sql.push_str(&format!("(t0.value = '{}'", condition.tag_value));

            if let Some(event_type) = condition.event_type {
                sql.push_str(&format!(" AND e.type = '{}'", event_type));
            }

            sql.push_str(")");
        }
        sql.push_str(")");
    }

    sql.push_str(" LIMIT 1)");
    sql
}

pub struct RecordedEvent {
    pub seq_id: i64,
    pub id: uuid::Uuid,
    pub stream_id: uuid::Uuid,
    pub version: i32,
    pub data: Value,
    pub event_type: String,
}

pub async fn select_events_for_query(client: &Client, query: &EventTagQuery<'_>) -> Result<Vec<RecordedEvent>, Error> {
    let sql = generate_select_events_sql(query);
    let rows = client.query(&sql, &[]).await?;
    let mut events = Vec::new();
    let mut last_seq_id = -1;
    for row in rows {
        let seq_id: i64 = row.get(0);
        
        // Marten's query with LEFT JOIN might return duplicates if multiple tags match.
        // In HandleAsync it just does events.Add(@event), but since we want the result 
        // to be clean and match Marten's eventual de-duplicated aggregate state, 
        // we'll filter out consecutive duplicates based on seq_id (which is ORDER BY'd).
        if seq_id == last_seq_id {
            continue;
        }
        last_seq_id = seq_id;

        events.push(RecordedEvent {
            seq_id,
            id: row.get(1),
            stream_id: row.get(2),
            version: row.get(3),
            data: row.get(4),
            event_type: row.get(5),
        });
    }
    Ok(events)
}

pub struct TaggedEvent {
    pub id: uuid::Uuid,
    pub stream_id: uuid::Uuid,
    pub version: i32,
    pub data: Value,
    pub event_type: String,
    pub tags: Vec<String>,
}

pub async fn append_events_conditionally(
    client: &mut Client,
    query: Option<&EventTagQuery<'_>>,
    events: Vec<TaggedEvent>
) -> Result<bool, Box<dyn std::error::Error>> {
    let tx = client.transaction().await?;
    
    // We can use a multi-statement query to reduce round trips.
    // PostgreSQL allows multiple statements separated by semicolons in a single query call.
    // However, tokio-postgres `query` and `execute` methods only return the result of the first statement.
    // To get all results, one would typically use `simple_query` which doesn't support parameters,
    // or use a more advanced approach.
    
    // BUT, we can use a PL/pgSQL `DO` block or a custom function to execute everything in one round trip.
    // Marten's "Rich Append" actually sends individual statements but relies on Npgsql's internal pipelining.
    // In tokio-postgres, pipelining is automatic if you don't await immediately, but since we use `&mut tx`,
    // we have to await. 
    
    // So, let's replicate the intent by building a single multi-statement SQL for the appends
    // and use CTEs (Common Table Expressions) to link them, ensuring only one round trip for all appends.

    let mut sql = String::new();
    let mut params: Vec<Box<dyn tokio_postgres::types::ToSql + Sync>> = Vec::new();
    let mut param_index = 1;

    // 1. Check DCB
    if let Some(query) = query {
        let exists_sql = generate_dcb_exists_sql(query);
        let conflict: bool = tx.query_one(&exists_sql, &[]).await?.get(0);
        
        if conflict {
            tx.rollback().await?;
            return Ok(false);
        }
    }
    
    // 2. Append events in a single batch query using CTEs
    if !events.is_empty() {
        sql.push_str("WITH ");
        for (i, event) in events.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            
            // Stream creation
            sql.push_str(&format!("s{} AS (INSERT INTO mt_streams (id, type) VALUES (${}, 'default') ON CONFLICT (id) DO NOTHING), ", i, param_index));
            params.push(Box::new(event.stream_id));
            param_index += 1;

            // Event insertion
            sql.push_str(&format!(
                "e{} AS (INSERT INTO mt_events (id, stream_id, version, data, type) VALUES (${}, ${}, ${}, ${}, ${}) RETURNING seq_id)",
                i, param_index, param_index + 1, param_index + 2, param_index + 3, param_index + 4
            ));
            params.push(Box::new(event.id));
            params.push(Box::new(event.stream_id));
            params.push(Box::new(event.version));
            params.push(Box::new(event.data.clone()));
            params.push(Box::new(event.event_type.clone()));
            param_index += 5;
            
            // Tag insertion
            if !event.tags.is_empty() {
                sql.push_str(&format!(", t{} AS (INSERT INTO mt_event_tag_string (value, seq_id) ", i));
                for (j, tag) in event.tags.iter().enumerate() {
                    if j > 0 {
                        sql.push_str(" UNION ALL ");
                    }
                    sql.push_str(&format!("SELECT ${}, seq_id FROM e{}", param_index, i));
                    params.push(Box::new(tag.clone()));
                    param_index += 1;
                }
                sql.push_str(" ON CONFLICT DO NOTHING)");
            }
        }
        sql.push_str(" SELECT 1;");

        let params_ref: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = params.iter().map(|b| b.as_ref() as &(dyn tokio_postgres::types::ToSql + Sync)).collect();
        tx.execute(&sql, &params_ref).await?;
    }

    tx.commit().await?;
    Ok(true)
}
