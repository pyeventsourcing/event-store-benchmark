pub struct TagCondition<'a> {
    pub tag_value: &'a str,
}

pub struct EventTagQuery<'a> {
    pub last_seen_sequence: i64,
    pub conditions: Vec<TagCondition<'a>>,
}

impl<'a> EventTagQuery<'a> {
    pub fn new(last_seen_sequence: i64) -> Self {
        Self {
            last_seen_sequence,
            conditions: Vec::new(),
        }
    }

    pub fn with_tag(mut self, tag_value: &'a str) -> Self {
        self.conditions.push(TagCondition { tag_value });
        self
    }
}

use tokio_postgres::{Client, Error};
use serde_json::Value;

pub fn generate_select_events_sql(query: &EventTagQuery) -> String {
    let mut sql = String::from("SELECT e.seq_id, e.id, e.stream_id, e.version, e.data, e.type, e.mt_dotnet_type FROM mt_events e");
    
    // Marten joins to the tag table(s) to apply filters
    sql.push_str(" LEFT JOIN mt_event_tag_string t0 ON e.seq_id = t0.seq_id");
    
    sql.push_str(&format!(" WHERE (e.seq_id > {})", query.last_seen_sequence));

    if !query.conditions.is_empty() {
        sql.push_str(" AND (");
        for (i, condition) in query.conditions.iter().enumerate() {
            if i > 0 {
                sql.push_str(" OR ");
            }
            sql.push_str(&format!("(t0.value = '{}')", condition.tag_value));
        }
        sql.push_str(")");
    }
    
    sql.push_str(" ORDER BY e.seq_id");
    sql
}

pub fn generate_dcb_exists_sql(query: &EventTagQuery) -> String {
    let mut sql = String::from("SELECT EXISTS (SELECT 1 FROM mt_event_tag_string t0");

    sql.push_str(&format!(" WHERE (t0.seq_id > {})", query.last_seen_sequence));

    if !query.conditions.is_empty() {
        // Build OR conditions
        sql.push_str(" AND (");
        for (i, condition) in query.conditions.iter().enumerate() {
            if i > 0 {
                sql.push_str(" OR ");
            }

            sql.push_str(&format!("(t0.value = '{}')", condition.tag_value));
        }
        sql.push_str(")");
    }

    sql.push_str(")");
    sql
}

pub struct RecordedEvent {
    pub seq_id: i64,
    pub id: uuid::Uuid,
    pub stream_id: uuid::Uuid,
    pub version: i32,
    pub data: Value,
    pub event_type: String,
    pub dotnet_type: Option<String>,
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
            dotnet_type: row.get(6),
        });
    }
    Ok(events)
}
