pub struct DcbCondition<'a> {
    pub tag_type: &'a str,
    pub tag_value: &'a str,
    pub event_type: Option<&'a str>,
}

pub struct DcbQuery<'a> {
    pub conditions: Vec<DcbCondition<'a>>,
    pub last_seen_sequence: i64,
}

pub fn generate_dcb_exists_sql(query: &DcbQuery) -> String {
    let mut sql = String::from("SELECT EXISTS (SELECT 1 FROM ");

    let mut distinct_tag_types: Vec<&str> = Vec::new();
    for condition in &query.conditions {
        if !distinct_tag_types.contains(&condition.tag_type) {
            distinct_tag_types.push(condition.tag_type);
        }
    }

    // Start with the first tag table
    for (i, tag_type) in distinct_tag_types.iter().enumerate() {
        let alias = format!("t{}", i);
        if i == 0 {
            sql.push_str(&format!("mt_event_tag_{} {}", tag_type, alias));
        } else {
            sql.push_str(&format!(" INNER JOIN mt_event_tag_{} {} ON t0.seq_id = {}.seq_id", tag_type, alias, alias));
        }
    }

    // Join to mt_events only if we need event type filtering
    let has_event_type_filter = query.conditions.iter().any(|c| c.event_type.is_some());
    if has_event_type_filter {
        sql.push_str(" INNER JOIN mt_events e ON t0.seq_id = e.seq_id");
    }

    sql.push_str(&format!(" WHERE t0.seq_id > {}", query.last_seen_sequence));

    // Build OR conditions
    sql.push_str(" AND (");
    for (i, condition) in query.conditions.iter().enumerate() {
        if i > 0 {
            sql.push_str(" OR ");
        }

        let tag_index = distinct_tag_types.iter().position(|&t| t == condition.tag_type).unwrap();
        let alias = format!("t{}", tag_index);

        sql.push_str(&format!("({}.value = '{}'", alias, condition.tag_value));

        if let Some(event_type) = condition.event_type {
            sql.push_str(&format!(" AND e.type = '{}'", event_type));
        }

        sql.push_str(")");
    }

    sql.push_str(") LIMIT 1)");
    sql
}
