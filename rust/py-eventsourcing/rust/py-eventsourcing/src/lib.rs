use tokio_postgres::{Client, NoTls};
use postgres_types::{ToSql, FromSql};
use anyhow::{Result, anyhow};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "dcb_event_tt")]
pub struct DcbEventTt {
    #[postgres(name = "type")]
    pub type_name: String,
    pub data: Option<Vec<u8>>,
    pub tags: Vec<String>,
}

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "dcb_query_item_tt")]
pub struct DcbQueryItemTt {
    pub types: Vec<String>,
    pub tags: Vec<String>,
}

#[derive(Clone, Debug)]
pub struct DcbEvent {
    pub type_name: String,
    pub data: Vec<u8>,
    pub tags: Vec<String>,
}

#[derive(Debug)]
pub struct DcbSequencedEvent {
    pub event: DcbEvent,
    pub position: i64,
}

pub struct DcbQueryItem {
    pub types: Vec<String>,
    pub tags: Vec<String>,
}

pub struct DcbQuery {
    pub items: Vec<DcbQueryItem>,
}

pub struct DcbAppendCondition {
    pub fail_if_events_match: DcbQuery,
    pub after: Option<i64>,
}

pub struct PostgresDCBRecorderTT {
    client: Client,
    pub schema: String,
    pub events_table: String,
    pub tags_table: String,
    pub event_type_name: String,
    pub query_item_type_name: String,
    pub unconditional_append_fn: String,
    pub conditional_append_fn: String,
}

impl PostgresDCBRecorderTT {
    pub fn from_client(client: Client, schema: &str) -> Self {
        let events_table = "dcb_events_tt_main".to_string();
        let tags_table = "dcb_events_tt_tag".to_string();

        Self {
            client,
            schema: schema.to_string(),
            events_table,
            tags_table,
            event_type_name: "dcb_event_tt".to_string(),
            query_item_type_name: "dcb_query_item_tt".to_string(),
            unconditional_append_fn: "dcb_unconditional_append_tt".to_string(),
            conditional_append_fn: "dcb_conditional_append_tt".to_string(),
        }
    }

    pub async fn connect(config: &str, schema: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(config, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Self::from_client(client, schema))
    }

    pub async fn create_tables(&self) -> Result<()> {
        let schema = &self.schema;
        let event_type = &self.event_type_name;
        let query_item_type = &self.query_item_type_name;
        let events_table = &self.events_table;
        let tags_table = &self.tags_table;
        let unconditional_append = &self.unconditional_append_fn;
        let conditional_append = &self.conditional_append_fn;
        let channel = format!("{}_{}", schema, events_table).replace(".", "_");

        let batch = format!(r#"
            DO $$
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid WHERE t.typname = '{event_type}' AND n.nspname = '{schema}') THEN
                    CREATE TYPE {schema}.{event_type} AS (
                        type text,
                        data bytea,
                        tags text[]
                    );
                END IF;
                IF NOT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON t.typnamespace = n.oid WHERE t.typname = '{query_item_type}' AND n.nspname = '{schema}') THEN
                    CREATE TYPE {schema}.{query_item_type} AS (
                        types text[],
                        tags text[]
                    );
                END IF;
            END
            $$;

            CREATE TABLE IF NOT EXISTS {schema}.{events_table} (
                id bigserial PRIMARY KEY,
                type text NOT NULL,
                data bytea,
                tags text[] NOT NULL
            );
            CREATE UNIQUE INDEX IF NOT EXISTS {events_table}_idx_id_type ON {schema}.{events_table} (id) INCLUDE (type);
            CREATE TABLE IF NOT EXISTS {schema}.{tags_table} (
                tag text,
                main_id bigint REFERENCES {schema}.{events_table} (id)
            );
            CREATE INDEX IF NOT EXISTS {tags_table}_idx_tag_main_id ON {schema}.{tags_table} (tag, main_id);

            CREATE OR REPLACE FUNCTION {schema}.{unconditional_append}(
                new_events {schema}.{event_type}[]
            ) RETURNS SETOF bigint
            LANGUAGE plpgsql AS $$
            BEGIN
                RETURN QUERY
                WITH new_data AS (
                    SELECT * FROM unnest(new_events)
                ),
                inserted AS (
                    INSERT INTO {schema}.{events_table} (type, data, tags)
                    SELECT type, data, tags
                    FROM new_data
                    RETURNING id, tags
                ),
                expanded_tags AS (
                    SELECT ins.id AS main_id, tag
                    FROM inserted ins,
                         unnest(ins.tags) AS tag
                ),
                tag_insert AS (
                    INSERT INTO {schema}.{tags_table} (tag, main_id)
                    SELECT tag, main_id
                    FROM expanded_tags
                )
                SELECT MAX(id) FROM inserted;
                PERFORM pg_notify('{channel}', '');
            END
            $$;

            CREATE OR REPLACE FUNCTION {schema}.{conditional_append}(
                query_items {schema}.{query_item_type}[],
                after_id bigint,
                new_events {schema}.{event_type}[]
            ) RETURNS SETOF bigint
            LANGUAGE plpgsql AS $$
            DECLARE
                conflict_exists boolean;
            BEGIN
                LOCK TABLE {schema}.{events_table} IN EXCLUSIVE MODE;

                WITH query_items_cte AS (
                    SELECT * FROM unnest(query_items) WITH ORDINALITY
                ),
                initial_matches AS (
                    SELECT
                        t.main_id,
                        qi.ordinality,
                        t.tag,
                        qi.tags AS required_tags,
                        qi.types AS allowed_types
                    FROM query_items_cte qi
                    JOIN {schema}.{tags_table} t
                      ON t.tag = ANY(qi.tags)
                    WHERE t.main_id > COALESCE(after_id, 0)
                ),
                matched_groups AS (
                    SELECT
                        main_id,
                        ordinality,
                        COUNT(DISTINCT tag) AS matched_tag_count,
                        array_length(required_tags, 1) AS required_tag_count,
                        allowed_types
                    FROM initial_matches
                    GROUP BY main_id, ordinality, required_tag_count, allowed_types
                ),
                qualified_ids AS (
                    SELECT main_id, allowed_types
                    FROM matched_groups
                    WHERE matched_tag_count = required_tag_count
                ),
                filtered_ids AS (
                    SELECT m.id
                    FROM {schema}.{events_table} m
                    JOIN qualified_ids q ON q.main_id = m.id
                    WHERE
                        m.id > COALESCE(after_id, 0)
                        AND (
                            array_length(q.allowed_types, 1) IS NULL
                            OR array_length(q.allowed_types, 1) = 0
                            OR m.type = ANY(q.allowed_types)
                        )
                    LIMIT 1
                )
                SELECT EXISTS (SELECT 1 FROM filtered_ids)
                INTO conflict_exists;

                IF NOT conflict_exists THEN
                    RETURN QUERY
                    WITH new_data AS (
                        SELECT * FROM unnest(new_events)
                    ),
                    inserted AS (
                        INSERT INTO {schema}.{events_table} (type, data, tags)
                        SELECT type, data, tags
                        FROM new_data
                        RETURNING id, tags
                    ),
                    expanded_tags AS (
                        SELECT ins.id AS main_id, tag
                        FROM inserted ins,
                             unnest(ins.tags) AS tag
                    ),
                    tag_insert AS (
                        INSERT INTO {schema}.{tags_table} (tag, main_id)
                        SELECT tag, main_id
                        FROM expanded_tags
                    )
                    SELECT MAX(id) FROM inserted;
                    PERFORM pg_notify('{channel}', '');
                END IF;
                RETURN;
            END
            $$;
        "#, 
        schema=schema, event_type=event_type, query_item_type=query_item_type, 
        events_table=events_table, tags_table=tags_table, 
        unconditional_append=unconditional_append, conditional_append=conditional_append,
        channel=channel);

        self.client.batch_execute(&batch).await?;
        Ok(())
    }

    pub async fn drop_tables(&self) -> Result<()> {
        let schema = &self.schema;
        let batch = format!(r#"
            DROP FUNCTION IF EXISTS {schema}.{unconditional_append}({schema}.{event_type}[]);
            DROP FUNCTION IF EXISTS {schema}.{conditional_append}({schema}.{query_item_type}[], bigint, {schema}.{event_type}[]);
            DROP TABLE IF EXISTS {schema}.{tags_table};
            DROP TABLE IF EXISTS {schema}.{events_table};
            DROP TYPE IF EXISTS {schema}.{query_item_type};
            DROP TYPE IF EXISTS {schema}.{event_type};
        "#, 
        schema=schema, 
        unconditional_append=self.unconditional_append_fn,
        conditional_append=self.conditional_append_fn,
        tags_table=self.tags_table,
        events_table=self.events_table,
        query_item_type=self.query_item_type_name,
        event_type=self.event_type_name);

        self.client.batch_execute(&batch).await?;
        Ok(())
    }

    pub async fn append(&self, events: Vec<DcbEvent>, condition: Option<DcbAppendCondition>) -> Result<i64> {
        if events.is_empty() {
            return Err(anyhow!("Cannot append empty events list"));
        }
        let pg_events: Vec<DcbEventTt> = events.into_iter().map(|e| DcbEventTt {
            type_name: e.type_name,
            data: Some(e.data),
            tags: e.tags,
        }).collect();

        if let Some(cond) = condition {
            if cond.fail_if_events_match.items.is_empty() {
                return self.unconditional_append(pg_events).await;
            }

            if cond.fail_if_events_match.items.iter().all(|q| !q.tags.is_empty()) {
                let pg_query_items: Vec<DcbQueryItemTt> = cond.fail_if_events_match.items.into_iter().map(|i| DcbQueryItemTt {
                    types: i.types,
                    tags: i.tags,
                }).collect();

                let after = cond.after.unwrap_or(0);
                
                let row = self.client.query_one(
                    &format!("SELECT {} FROM {}.{}($1, $2, $3)", self.conditional_append_fn, self.schema, self.conditional_append_fn),
                    &[&pg_query_items, &after, &pg_events]
                ).await?;

                let res: Option<i64> = row.get(0);
                res.ok_or_else(|| anyhow!("IntegrityError: Append condition failed"))
            } else {
                let after = cond.after.unwrap_or(0);
                self.client.batch_execute("BEGIN;").await?;
                let res = (async {
                    self.client.batch_execute(&format!("LOCK TABLE {}.{} IN EXCLUSIVE MODE", self.schema, self.events_table)).await?;
                    let failed = self.read(Some(cond.fail_if_events_match), Some(after), Some(1)).await?;
                    if !failed.is_empty() {
                        return Err(anyhow!("IntegrityError: Append condition failed"));
                    }
                    self.unconditional_append(pg_events).await
                }).await;

                if res.is_ok() {
                    self.client.batch_execute("COMMIT;").await?;
                } else {
                    let _ = self.client.batch_execute("ROLLBACK;").await;
                }
                res
            }
        } else {
            self.unconditional_append(pg_events).await
        }
    }

    async fn unconditional_append(&self, pg_events: Vec<DcbEventTt>) -> Result<i64> {
        let row = self.client.query_one(
            &format!("SELECT {} FROM {}.{}($1)", self.unconditional_append_fn, self.schema, self.unconditional_append_fn),
            &[&pg_events]
        ).await?;
        let res: i64 = row.get(0);
        Ok(res)
    }

    pub async fn read(&self, query: Option<DcbQuery>, after: Option<i64>, limit: Option<i64>) -> Result<Vec<DcbSequencedEvent>> {
        let after_val = after.unwrap_or(0);
        let limit_val = limit.unwrap_or(i64::MAX);

        let rows = if let Some(q) = query {
            if q.items.iter().all(|i| !i.tags.is_empty()) && !q.items.is_empty() {
                let pg_query_items: Vec<DcbQueryItemTt> = q.items.into_iter().map(|i| DcbQueryItemTt {
                    types: i.types,
                    tags: i.tags,
                }).collect();

                let sql = format!(r#"
                    WITH query_items AS (
                        SELECT * FROM unnest($1::{schema}.{query_item_type}[]) WITH ORDINALITY
                    ),
                    initial_matches AS (
                        SELECT
                            t.main_id,
                            qi.ordinality,
                            t.tag,
                            qi.tags AS required_tags,
                            qi.types AS allowed_types
                        FROM query_items qi
                        JOIN {schema}.{tags_table} t
                          ON t.tag = ANY(qi.tags)
                       WHERE t.main_id > $2
                    ),
                    matched_groups AS (
                        SELECT
                            main_id,
                            ordinality,
                            COUNT(DISTINCT tag) AS matched_tag_count,
                            array_length(required_tags, 1) AS required_tag_count,
                            allowed_types
                        FROM initial_matches
                        GROUP BY main_id, ordinality, required_tag_count, allowed_types
                    ),
                    qualified_ids AS (
                        SELECT main_id, allowed_types
                        FROM matched_groups
                        WHERE matched_tag_count = required_tag_count
                    ),
                    filtered_ids AS (
                        SELECT m.id
                        FROM {schema}.{events_table} m
                        JOIN qualified_ids q ON q.main_id = m.id
                        WHERE
                            m.id > $2
                            AND (
                                array_length(q.allowed_types, 1) IS NULL
                                OR array_length(q.allowed_types, 1) = 0
                                OR m.type = ANY(q.allowed_types)
                            )
                        ORDER BY m.id ASC
                        LIMIT $3
                    )
                    SELECT *
                    FROM {schema}.{events_table} m
                    WHERE m.id IN (SELECT id FROM filtered_ids)
                    ORDER BY m.id ASC;
                "#, schema=self.schema, query_item_type=self.query_item_type_name, tags_table=self.tags_table, events_table=self.events_table);
                
                self.client.query(&sql, &[&pg_query_items, &after_val, &limit_val]).await?
            } else if q.items.len() == 1 && q.items[0].types.len() == 1 && q.items[0].tags.is_empty() {
                let sql = format!("SELECT * FROM {}.{} WHERE type = $1 AND id > $2 ORDER BY id ASC LIMIT $3", self.schema, self.events_table);
                self.client.query(&sql, &[&q.items[0].types[0], &after_val, &limit_val]).await?
            } else {
                return Err(anyhow!("Unsupported query"));
            }
        } else {
            let sql = format!("SELECT * FROM {}.{} WHERE id > $1 ORDER BY id ASC LIMIT $2", self.schema, self.events_table);
            self.client.query(&sql, &[&after_val, &limit_val]).await?
        };

        let mut events = Vec::new();
        for row in rows {
            events.push(DcbSequencedEvent {
                event: DcbEvent {
                    type_name: row.get("type"),
                    data: row.get("data"),
                    tags: row.get("tags"),
                },
                position: row.get("id"),
            });
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    async fn setup() -> Result<PostgresDCBRecorderTT> {
        let config = "host=localhost user=eventsourcing password=eventsourcing dbname=eventsourcing";
        let recorder = PostgresDCBRecorderTT::connect(config, "public").await?;
        let _ = recorder.drop_tables().await;
        recorder.create_tables().await?;
        Ok(recorder)
    }

    #[tokio::test]
    #[serial]
    async fn test_unconditional_append_and_read() -> Result<()> {
        let recorder = setup().await?;
        
        let events = vec![
            DcbEvent {
                type_name: "Type1".to_string(),
                data: vec![1, 2, 3],
                tags: vec!["tag1".to_string(), "tag2".to_string()],
            },
            DcbEvent {
                type_name: "Type2".to_string(),
                data: vec![4, 5, 6],
                tags: vec!["tag2".to_string()],
            },
        ];

        let last_pos = recorder.append(events, None).await?;
        assert!(last_pos > 0);

        let read_events = recorder.read(None, None, None).await?;
        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].event.type_name, "Type1");
        assert_eq!(read_events[1].event.type_name, "Type2");
        assert_eq!(read_events[1].position, last_pos);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_conditional_append() -> Result<()> {
        let recorder = setup().await?;

        recorder.append(vec![
            DcbEvent {
                type_name: "Type1".to_string(),
                data: vec![],
                tags: vec!["tag1".to_string()],
            }
        ], None).await?;

        let cond1 = DcbAppendCondition {
            fail_if_events_match: DcbQuery {
                items: vec![DcbQueryItem {
                    types: vec![],
                    tags: vec!["tag2".to_string()],
                }]
            },
            after: Some(0),
        };
        recorder.append(vec![
            DcbEvent {
                type_name: "Type2".to_string(),
                data: vec![],
                tags: vec!["tag2".to_string()],
            }
        ], Some(cond1)).await?;

        let cond2 = DcbAppendCondition {
            fail_if_events_match: DcbQuery {
                items: vec![DcbQueryItem {
                    types: vec![],
                    tags: vec!["tag1".to_string()],
                }]
            },
            after: Some(0),
        };
        let res = recorder.append(vec![
            DcbEvent {
                type_name: "Type3".to_string(),
                data: vec![],
                tags: vec!["tag3".to_string()],
            }
        ], Some(cond2)).await;

        assert!(res.is_err());

        let all = recorder.read(None, None, None).await?;
        assert_eq!(all.len(), 2);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_read_by_tags() -> Result<()> {
        let recorder = setup().await?;

        recorder.append(vec![
            DcbEvent { type_name: "T1".to_string(), data: vec![], tags: vec!["A".to_string()] },
            DcbEvent { type_name: "T2".to_string(), data: vec![], tags: vec!["B".to_string()] },
            DcbEvent { type_name: "T3".to_string(), data: vec![], tags: vec!["A".to_string(), "B".to_string()] },
        ], None).await?;

        let query = DcbQuery {
            items: vec![DcbQueryItem {
                types: vec![],
                tags: vec!["A".to_string()],
            }]
        };
        let res = recorder.read(Some(query), None, None).await?;
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].event.type_name, "T1");
        assert_eq!(res[1].event.type_name, "T3");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_throughput_unconditional_append() -> Result<()> {
        let recorder = setup().await?;
        let num_iterations = 1000;
        let events_per_append = 1;

        let start = std::time::Instant::now();
        for i in 0..num_iterations {
            let events = vec![DcbEvent {
                type_name: format!("Type{}", i),
                data: vec![0; 100],
                tags: vec![format!("tag{}", i)],
            }; events_per_append];
            recorder.append(events, None).await?;
        }
        let duration = start.elapsed();
        let total_events = num_iterations * events_per_append;
        println!("\nUnconditional append throughput: {} events in {:?}, {:.2} events/sec", 
            total_events, duration, total_events as f64 / duration.as_secs_f64());

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_throughput_conditional_append() -> Result<()> {
        let recorder = setup().await?;
        let num_iterations = 1000;
        let events_per_append = 1;

        recorder.append(vec![DcbEvent {
            type_name: "Initial".to_string(),
            data: vec![],
            tags: vec!["initial".to_string()],
        }], None).await?;

        let start = std::time::Instant::now();
        for i in 0..num_iterations {
            let events = vec![DcbEvent {
                type_name: format!("Type{}", i),
                data: vec![0; 100],
                tags: vec![format!("tag_new_{}", i)],
            }; events_per_append];

            let condition = DcbAppendCondition {
                fail_if_events_match: DcbQuery {
                    items: vec![DcbQueryItem {
                        types: vec![],
                        tags: vec![format!("nonexistent_{}", i)],
                    }]
                },
                after: Some(0),
            };

            recorder.append(events, Some(condition)).await?;
        }
        let duration = start.elapsed();
        let total_events = num_iterations * events_per_append;
        println!("\nConditional append throughput: {} events in {:?}, {:.2} events/sec", 
            total_events, duration, total_events as f64 / duration.as_secs_f64());

        Ok(())
    }
}
