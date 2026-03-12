use anyhow::Result;
use bench_core::{execute_run, StoreManager, StoreManagerFactory, WorkloadFactory};
use clap::{Parser, Subcommand};
use rand::Rng;
use serde_json::json;
use std::fs;
use std::path::PathBuf;
use tokio::runtime::Runtime;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "es-bench", version, about = "Event Store Benchmark Suite CLI")]
struct Cli {
    #[arg(long, default_value = "info")]
    log: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(serde::Deserialize)]
struct WorkloadConfig {
    workload: String,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a workload against a store
    Run {
        /// Store name (e.g., umadb)
        #[arg(long, default_value = "all")]
        store: String,
        /// Path to YAML file
        #[arg(long)]
        config: PathBuf,
        /// Output directory base
        #[arg(long, default_value = "results/raw")]
        output: PathBuf,
        /// Random seed (default is a random value)
        #[arg(long)]
        seed: Option<u64>,
    },
    /// List available store adapters
    ListStores,
    /// List available workload types
    ListWorkloads,
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
        Box::new(bench_core::workloads::ConcurrentWritersFactory),
        Box::new(bench_core::workloads::ConcurrentReadersFactory),
    ]
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Supress the noise from the KurrentDB Rust client.
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::new(&cli.log).add_directive("kurrentdb::grpc=off".parse()?),
        )
        .init();

    match cli.command {
        Commands::ListStores => {
            for f in store_manager_factories() {
                println!("{}", f.name());
            }
            Ok(())
        }
        Commands::ListWorkloads => {
            for f in workload_factories() {
                println!("{}", f.name());
            }
            Ok(())
        }
        Commands::Run {
            store,
            config,
            output,
            seed,
        } => {
            let actual_seed = seed.unwrap_or_else(|| rand::thread_rng().gen());
            let store_name = store.to_lowercase();

            // Parse workload name from YAML config
            let config_content = fs::read_to_string(&config)?;
            let workload_config: WorkloadConfig = serde_yaml::from_str(&config_content)?;
            let workload = workload_config.workload;

            if store_name == "all" {
                for store_manager_factory in store_manager_factories() {
                    let factory_store_name = store_manager_factory.name().to_string();
                    let store_manager = store_manager_factory.create_store_manager()?;
                    run_workload_and_write_output(factory_store_name, store_manager, workload.clone(), &config, output.clone(), actual_seed)?;
                }
            } else {
                // Find a store factory and create a store manager
                let store_manager_factory = store_manager_factories()
                    .into_iter()
                    .find(|f| f.name() == store_name)
                    .ok_or_else(|| anyhow::anyhow!("unknown store: {}", store_name))?;

                let store_manager = store_manager_factory.create_store_manager()?;
                run_workload_and_write_output(store_name, store_manager, workload, &config, output, actual_seed)?;

            }
            Ok(())
        }
    }
}

fn run_workload_and_write_output(store_name: String, store_manager: Box<dyn StoreManager>, workload: String, config: &PathBuf, output: PathBuf, seed: u64) -> Result<()> {
    let workload_name = workload.to_lowercase();
    let workload_config_yaml = fs::read_to_string(&config)?;
    // Find a workload factory and create a workload
    let workload_factory = workload_factories()
        .into_iter()
        .find(|f| f.name() == workload_name)
        .ok_or_else(|| anyhow::anyhow!("unknown workload type: {}", workload_name))?;

    let workload_instance = workload_factory.create(&workload_config_yaml, seed)?;

    // Create an output directory
    let workload_dir = output.join(workload_name.as_str());
    fs::create_dir_all(&workload_dir)?;

    let run_dir_name = format!("{}-r{:03}-w{:03}", store_name, workload_instance.readers(), workload_instance.writers());
    let run_dir = workload_dir.join(run_dir_name);
    fs::create_dir_all(&run_dir)?;

    // Execute run
    let rt = Runtime::new()?;
    let result = rt.block_on(async {
        execute_run(
            store_manager,
            workload_instance,
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
                    "store": store_name,
                    "workload": workload_name,
                    "config": config.to_string_lossy(),
                    "seed": seed,
                })
            .to_string(),
    )?;

    println!("Run complete. Outputs written to {}", run_dir.display());
    Ok(())
}
