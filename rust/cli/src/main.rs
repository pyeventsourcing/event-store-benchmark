use anyhow::Result;
use bench_core::adapter::ConnectionParams;
use bench_core::{run_workload, AdapterFactory, RunOptions, WorkflowFactory, WorkloadFile};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "esbs", version, about = "Event Store Benchmark Suite CLI")]
struct Cli {
    #[arg(long, default_value = "info")]
    log: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a workload against a store
    Run {
        /// Store adapter name (e.g., umadb)
        #[arg(long)]
        store: String,
        /// Workflow name (e.g., concurrent_writers)
        #[arg(long, default_value = "concurrent_writers")]
        workflow: String,
        /// Path to workload YAML
        #[arg(long)]
        workload: PathBuf,
        /// Output directory base (raw results will be placed under an adapter-workload folder)
        #[arg(long, default_value = "results/raw")]
        output: PathBuf,
        /// Connection URI for the store (defaults per adapter)
        #[arg(long)]
        uri: Option<String>,
        /// Optional key=value options (repeatable)
        #[arg(long, num_args=0.., value_parser = parse_key_val::<String, String>)]
        option: Vec<(String, String)>,
        /// Random seed
        #[arg(long, default_value_t = 42)]
        seed: u64,
    },
    /// List available workloads in the repo
    ListWorkloads {
        #[arg(long, default_value = "workloads")]
        path: PathBuf,
    },
    /// List available store adapters
    ListStores,
    /// List available workflow types
    ListWorkflows,
}

fn parse_key_val<K, V>(s: &str) -> std::result::Result<(K, V), String>
where
    K: std::str::FromStr,
    V: std::str::FromStr,
{
    let pos = s.find('=');
    match pos {
        Some(pos) => {
            let key = s[..pos]
                .parse()
                .map_err(|_| format!("invalid key: {}", &s[..pos]))?;
            let value = s[pos + 1..]
                .parse()
                .map_err(|_| format!("invalid value: {}", &s[pos + 1..]))?;
            Ok((key, value))
        }
        None => Err(format!("invalid KEY=VALUE: no `=` in `{}`", s)),
    }
}

fn adapter_factories() -> Vec<Box<dyn AdapterFactory>> {
    vec![
        Box::new(dummy_adapter::DummyFactory),
        Box::new(umadb_adapter::UmaDbFactory),
        Box::new(kurrentdb_adapter::KurrentDbFactory),
        Box::new(axonserver_adapter::AxonServerFactory),
        Box::new(eventsourcingdb_adapter::EventsourcingDbFactory),
    ]
}

fn workflow_factories() -> Vec<Box<dyn WorkflowFactory>> {
    vec![
        Box::new(bench_core::workflows::ConcurrentWritersFactory),
        Box::new(bench_core::workflows::ConcurrentReadersFactory),
    ]
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Supress the noise from the KurrentDB Rust client.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::new(&cli.log).add_directive("kurrentdb::grpc=off".parse().unwrap()),
        )
        .init();

    let factories = adapter_factories();

    match cli.command {
        Commands::ListStores => {
            for f in &factories {
                println!("{}", f.name());
            }
            Ok(())
        }
        Commands::ListWorkflows => {
            let workflow_factories_vec = workflow_factories();
            for f in &workflow_factories_vec {
                println!("{}", f.name());
            }
            Ok(())
        }
        Commands::ListWorkloads { path } => {
            for entry in fs::read_dir(path)? {
                let entry = entry?;
                let p = entry.path();
                if p.extension().and_then(|e| e.to_str()) == Some("yaml") {
                    println!("{}", p.display());
                }
            }
            Ok(())
        }
        Commands::Run {
            store,
            workflow,
            workload,
            output,
            uri,
            option,
            seed,
        } => {
            // Load workload
            let wl = WorkloadFile::load(&workload)?;
            let adapter_name = store.to_lowercase();
            let workflow_name = workflow.to_lowercase();

            // Create nested structure: workflow/adapter-r000-w000
            let workload_dir = output.join(workflow_name.as_str());
            fs::create_dir_all(&workload_dir)?;

            // Format directory name based on workload type, zero-padded for sorting
            let run_dir_name = format!("{}-r{:03}-w{:03}", adapter_name, wl.readers, wl.writers);
            let run_dir = workload_dir.join(run_dir_name);
            fs::create_dir_all(&run_dir)?;

            let default_uri = match adapter_name.as_str() {
                "umadb" => "http://localhost:50051".to_string(),
                "kurrentdb" => "esdb://localhost:2113?tls=false".to_string(),
                "axonserver" => "http://localhost:8124".to_string(),
                "eventsourcingdb" => "http://localhost:4000".to_string(),
                _ => String::new(),
            };
            let uri = uri.unwrap_or(default_uri);
            let conn = ConnectionParams {
                uri,
                options: option.into_iter().collect(),
            };

            // Find and move the factory out of the vector
            let factory_box = factories
                .into_iter()
                .find(|f| f.name() == adapter_name)
                .ok_or_else(|| anyhow::anyhow!("unknown adapter: {}", adapter_name))?;

            // Convert Box to Arc directly
            let factory_arc: Arc<dyn AdapterFactory> = factory_box.into();

            // Find workflow factory
            let workflow_factories_vec = workflow_factories();
            let workflow_factory = workflow_factories_vec
                .into_iter()
                .find(|f| f.name() == workflow_name)
                .ok_or_else(|| anyhow::anyhow!("unknown workflow: {}", workflow_name))?;

            // Create workflow strategy instance
            let workflow_strategy = workflow_factory.create(&wl, seed)?;

            let rt = Runtime::new()?;
            let adapter_name_for_run = adapter_name.clone();
            let result = rt.block_on(async move {
                run_workload(
                    factory_arc,
                    workflow_strategy,
                    wl,
                    RunOptions {
                        adapter_name: adapter_name_for_run,
                        conn,
                        seed,
                    },
                )
                .await
            })?;

            // Write JSON summary and samples
            let summary_path = run_dir.join("summary.json");
            let samples_path = run_dir.join("samples.jsonl");
            fs::write(
                &summary_path,
                serde_json::to_string_pretty(&result.summary)?,
            )?;
            // JSON Lines for samples
            let mut lines = String::new();
            for s in result.samples {
                lines.push_str(&serde_json::to_string(&s)?);
                lines.push('\n');
            }
            fs::write(&samples_path, lines)?;

            // Minimal Criterion-compatible marker file (for Python to find runs)
            let meta_path = run_dir.join("run.meta.json");
            fs::write(
                &meta_path,
                json!({
                    "adapter": adapter_name,
                    "workload": workload.to_string_lossy(),
                })
                .to_string(),
            )?;

            println!("Run complete. Outputs written to {}", run_dir.display());
            Ok(())
        }
    }
}
