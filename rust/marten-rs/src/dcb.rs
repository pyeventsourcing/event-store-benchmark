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
    sql.push_str(" INNER JOIN mt_event_tag_string t0 ON e.seq_id = t0.seq_id");
    
    // We already have t0 joined, so we can use it.
    // If we have multiple conditions, they are ORed in Marten's EventTagQuery if they are for the same tag table?
    // Actually, Marten's EventTagQuery usually filters events that match ANY of the conditions if they are combined.
    
    sql.push_str(&format!(" WHERE e.seq_id > {}", query.last_seen_sequence));

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

    sql.push_str(&format!(" WHERE t0.seq_id > {}", query.last_seen_sequence));

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

pub async fn select_events_for_query(client: &Client, query: &EventTagQuery<'_>) -> Result<Vec<Value>, Error> {
    let sql = generate_select_events_sql(query);
    let rows = client.query(&sql, &[]).await?;
    let mut events = Vec::new();
    for row in rows {
        let data: Value = row.get(4);
        events.push(data);
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
    
    // 1. Check DCB
    if let Some(query) = query {
        let exists_sql = generate_dcb_exists_sql(query);
        let conflict: bool = tx.query_one(&exists_sql, &[]).await?.get(0);
        
        if conflict {
            tx.rollback().await?;
            return Ok(false);
        }
    }
    
    // 2. Append events
    for event in events {
        // We might need to ensure stream exists
        tx.execute(
            "INSERT INTO mt_streams (id, type) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            &[&event.stream_id, &"default"]
        ).await?;

        let rows = tx.query(
            "INSERT INTO mt_events (id, stream_id, version, data, type) VALUES ($1, $2, $3, $4, $5) RETURNING seq_id",
            &[&event.id, &event.stream_id, &event.version, &event.data, &event.event_type]
        ).await?;
        let seq_id: i64 = rows[0].get(0);
        
        for tag in event.tags {
            tx.execute(
                "INSERT INTO mt_event_tag_string (value, seq_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&tag, &seq_id]
            ).await?;
        }
    }
    
    tx.commit().await?;
    Ok(true)
}
