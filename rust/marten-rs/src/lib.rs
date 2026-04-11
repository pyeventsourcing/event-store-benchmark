pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {
    use tokio_postgres::NoTls;

    #[tokio::test]
    async fn test_postgres_integration() -> Result<(), tokio_postgres::Error> {
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

        client.batch_execute("DROP TABLE IF EXISTS example").await?;
        client.batch_execute("CREATE TABLE example (id SERIAL PRIMARY KEY, data TEXT)").await?;
        
        client.execute("INSERT INTO example (data) VALUES ($1)", &[&"hello world"]).await?;

        let rows = client.query("SELECT data FROM example", &[]).await?;
        assert_eq!(rows.len(), 1);
        let data: &str = rows[0].get(0);
        assert_eq!(data, "hello world");

        Ok(())
    }
}
