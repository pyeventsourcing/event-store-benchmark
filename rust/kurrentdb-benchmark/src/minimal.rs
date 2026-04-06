use anyhow::Result;
use clap::Parser;
use hdrhistogram::Histogram;
use kurrentdb::{AppendToStreamOptions, KurrentDbClient};
use std::time::Instant;
use uuid::Uuid;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// KurrentDB connection string
    #[arg(long, default_value = "esdb://127.0.0.1:2113?tls=false")]
    conn: String,

    /// Stream name to append to
    #[arg(long, default_value = "benchmark-stream")]
    stream: String,

    /// Number of events to append
    #[arg(long, default_value_t = 1000)]
    events: usize,

    /// Size of each event payload in bytes
    #[arg(long, default_value_t = 1024)]
    size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    // Use KurrentDbClient::new directly as shown in the test
    let client = KurrentDbClient::new(args.conn.clone()).await.map_err(|e| anyhow::anyhow!(e))?;

    println!("Starting Minimal Rust benchmark: {} events, {} bytes each", args.events, args.size);
    println!("Target stream: {}", args.stream);
    println!("Connection string: {}", args.conn);

    let payload: Vec<u8> = vec![b'x'; args.size];
    let mut histogram = Histogram::<u64>::new_with_bounds(1, 10_000_000, 3)?; // 1us to 10s

    let options = AppendToStreamOptions::default();

    let start_time = Instant::now();

    for i in 0..args.events {
        let event = kurrentdb::EventData::binary("BenchmarkEvent", payload.clone().into()).id(Uuid::new_v4());

        let step_start = Instant::now();
        client
            .append_to_stream(args.stream.clone(), &options, vec![event])
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        let step_duration = step_start.elapsed();
        
        histogram.record(step_duration.as_micros() as u64)?;

        if (i + 1) % 100 == 0 {
            println!("Appended {}/{} events...", i + 1, args.events);
        }
    }

    let total_duration = start_time.elapsed();
    let throughput = args.events as f64 / total_duration.as_secs_f64();

    println!("\nBenchmark Results (Minimal Client):");
    println!("Total events: {}", args.events);
    println!("Total time: {:.2} s", total_duration.as_secs_f64());
    println!("Throughput: {:.2} events/s", throughput);

    println!("Latency:");
    println!("  Mean:   {:.3} ms", histogram.mean() / 1000.0);
    println!("  Min:    {:.3} ms", histogram.min() as f64 / 1000.0);
    println!("  Max:    {:.3} ms", histogram.max() as f64 / 1000.0);
    println!("  P50:    {:.3} ms", histogram.value_at_quantile(0.5) as f64 / 1000.0);
    println!("  P95:    {:.3} ms", histogram.value_at_quantile(0.95) as f64 / 1000.0);
    println!("  P99:    {:.3} ms", histogram.value_at_quantile(0.99) as f64 / 1000.0);

    Ok(())
}
