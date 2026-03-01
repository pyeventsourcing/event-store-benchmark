use anyhow::Result;
use bench_core::{run_workload, StoreManagerFactory, WorkloadFactory};
use clap::{Parser, Subcommand};
use serde_json::json;
use std::fs;
use std::path::PathBuf;
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
        /// Workload name (e.g., concurrent_writers)
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
    /// List available workload types
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

fn store_manager_factories() -> Vec<Box<dyn StoreManagerFactory>> {
    vec![
        Box::new(dummy_adapter::DummyFactory),
        Box::new(umadb_adapter::UmaDbFactory),
        Box::new(kurrentdb_adapter::KurrentDbFactory),
        Box::new(axonserver_adapter::AxonServerFactory),
        Box::new(eventsourcingdb_adapter::EventsourcingDbFactory),
    ]
}

fn workload_factories() -> Vec<Box<dyn WorkloadFactory>> {
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

    match cli.command {
        Commands::ListStores => {
            for f in store_manager_factories() {
                println!("{}", f.name());
            }
            Ok(())
        }
        Commands::ListWorkflows => {
            for f in workload_factories() {
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
            let adapter_name = store.to_lowercase();
            let workload_type_name = workflow.to_lowercase();

            // Load workload YAML
            let yaml_config = fs::read_to_string(&workload)?;

            // Find workload factory and create workload
            let workload_factory = workload_factories()
                .into_iter()
                .find(|f| f.name() == workload_type_name)
                .ok_or_else(|| anyhow::anyhow!("unknown workload type: {}", workload_type_name))?;

            let workload_instance = workload_factory.create(&yaml_config, seed)?;

            // Find store factory and create store manager
            let store_factory = store_manager_factories()
                .into_iter()
                .find(|f| f.name() == adapter_name)
                .ok_or_else(|| anyhow::anyhow!("unknown store: {}", adapter_name))?;

            let store_manager = store_factory.create_store_manager(uri, option.into_iter().collect())?;

            // Create output directory
            let workload_dir = output.join(workload_type_name.as_str());
            fs::create_dir_all(&workload_dir)?;

            let run_dir_name = format!("{}-r{:03}-w{:03}", adapter_name, workload_instance.readers(), workload_instance.writers());
            let run_dir = workload_dir.join(run_dir_name);
            fs::create_dir_all(&run_dir)?;

            // Run workload
            let rt = Runtime::new()?;
            let duration_seconds = workload_instance.duration_seconds();
            let result = rt.block_on(async {
                run_workload(
                    store_manager,
                    workload_instance,
                    duration_seconds,
                )
                .await
            })?;

            // Write outputs
            let summary_path = run_dir.join("summary.json");
            let samples_path = run_dir.join("samples.jsonl");
            fs::write(
                &summary_path,
                serde_json::to_string_pretty(&result.summary)?,
            )?;
            let mut lines = String::new();
            for s in result.samples {
                lines.push_str(&serde_json::to_string(&s)?);
                lines.push('\n');
            }
            fs::write(&samples_path, lines)?;

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
