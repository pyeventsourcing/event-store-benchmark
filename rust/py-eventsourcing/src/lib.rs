use tokio_postgres::{Client, NoTls};
use postgres_types::{ToSql, FromSql};
use anyhow::{Result, anyhow};

#[derive(Debug, ToSql, FromSql)]
#[postgres(name = "dcb_event_tt")]
pub struct DcbEventTt {
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

pub struct DcbEvent {
    pub type_name: String,
    pub data: Option<Vec<u8>>,
    pub tags: Vec<String>,
}

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
    schema: String,
    events_table: String,
    tags_table: String,
    event_type_name: String,
    query_item_type_name: String,
    unconditional_append_fn: String,
    conditional_append_fn: String,
}

impl PostgresDCBRecorderTT {
    pub async fn connect(config: &str, schema: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(config, NoTls).await?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let events_table = "dcb_events_tt_main".to_string();
        let tags_table = "dcb_events_tt_tag".to_string();

        Ok(Self {
            client,
            schema: schema.to_string(),
            events_table,
            tags_table,
            event_type_name: "dcb_event_tt".to_string(),
            query_item_type_name: "dcb_query_item_tt".to_string(),
            unconditional_append_fn: "dcb_unconditional_append_tt".to_string(),
            conditional_append_fn: "dcb_conditional_append_tt".to_string(),
        })
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
            CREATE TYPE {schema}.{event_type} AS (
                type text,
                data bytea,
                tags text[]
            );
            CREATE TYPE {schema}.{query_item_type} AS (
                types text[],
                tags text[]
            );
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
        let pg_events: Vec<DcbEventTt> = events.into_iter().map(|e| DcbEventTt {
            type_name: e.type_name,
            data: e.data,
            tags: e.tags,
        }).collect();

        if let Some(cond) = condition {
            let pg_query_items: Vec<DcbQueryItemTt> = cond.fail_if_events_match.items.into_iter().map(|i| DcbQueryItemTt {
                types: i.types,
                tags: i.tags,
            }).collect();

            let after = cond.after.unwrap_or(0);
            
            let row = self.client.query_one(
                &format!("SELECT * FROM {}.{}($1, $2, $3)", self.schema, self.conditional_append_fn),
                &[&pg_query_items, &after, &pg_events]
            ).await?;

            let res: Option<i64> = row.get(0);
            res.ok_or_else(|| anyhow!("IntegrityError: Append condition failed"))
        } else {
            let row = self.client.query_one(
                &format!("SELECT * FROM {}.{}($1)", self.schema, self.unconditional_append_fn),
                &[&pg_events]
            ).await?;
            let res: i64 = row.get(0);
            Ok(res)
        }
    }

    pub async fn read(&self, query: Option<DcbQuery>, after: Option<i64>, limit: Option<i64>) -> Result<Vec<DcbSequencedEvent>> {
        let after_val = after.unwrap_or(0);
        let limit_val = limit.unwrap_or(i64::MAX);

        let rows = if let Some(q) = query {
            if q.items.iter().all(|i| !i.tags.is_empty()) && !q.items.is_empty() {
                // Select by tags
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
                // Select by type
                let sql = format!("SELECT * FROM {}.{} WHERE type = $1 AND id > $2 ORDER BY id ASC LIMIT $3", self.schema, self.events_table);
                self.client.query(&sql, &[&q.items[0].types[0], &after_val, &limit_val]).await?
            } else {
                return Err(anyhow!("Unsupported query"));
            }
        } else {
            // Select all
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

    async fn setup() -> Result<PostgresDCBRecorderTT> {
        let config = "host=localhost user=postgres password=postgres dbname=postgres";
        let recorder = PostgresDCBRecorderTT::connect(config, "public").await?;
        let _ = recorder.drop_tables().await;
        recorder.create_tables().await?;
        Ok(recorder)
    }

    #[tokio::test]
    async fn test_unconditional_append_and_read() -> Result<()> {
        let recorder = setup().await?;
        
        let events = vec![
            DcbEvent {
                type_name: "Type1".to_string(),
                data: Some(vec![1, 2, 3]),
                tags: vec!["tag1".to_string(), "tag2".to_string()],
            },
            DcbEvent {
                type_name: "Type2".to_string(),
                data: Some(vec![4, 5, 6]),
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
    async fn test_conditional_append() -> Result<()> {
        let recorder = setup().await?;

        // 1. Initial append
        recorder.append(vec![
            DcbEvent {
                type_name: "Type1".to_string(),
                data: None,
                tags: vec!["tag1".to_string()],
            }
        ], None).await?;

        // 2. Successful conditional append
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
                data: None,
                tags: vec!["tag2".to_string()],
            }
        ], Some(cond1)).await?;

        // 3. Failed conditional append
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
                data: None,
                tags: vec!["tag3".to_string()],
            }
        ], Some(cond2)).await;

        assert!(res.is_err());

        let all = recorder.read(None, None, None).await?;
        assert_eq!(all.len(), 2); // Type1 and Type2

        Ok(())
    }

    #[tokio::test]
    async fn test_read_by_tags() -> Result<()> {
        let recorder = setup().await?;

        recorder.append(vec![
            DcbEvent { type_name: "T1".to_string(), data: None, tags: vec!["A".to_string()] },
            DcbEvent { type_name: "T2".to_string(), data: None, tags: vec!["B".to_string()] },
            DcbEvent { type_name: "T3".to_string(), data: None, tags: vec!["A".to_string(), "B".to_string()] },
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
}
