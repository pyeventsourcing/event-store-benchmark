use anyhow::Result;
use bench_core::{
    collect_environment_info, execute_run, get_git_commit_hash, SessionMetadata,
    StoreManagerFactory, WorkloadFactory,
};
use chrono::Utc;
use clap::{Parser, Subcommand};
use rand::Rng;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "es-bench", version, about = "Event Store Benchmark Suite CLI")]
struct Cli {
    #[arg(long, default_value = "info")]
    log: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run a workload against store(s)
    Run {
        /// Path to workload YAML config file
        #[arg(long)]
        config: PathBuf,
        /// Random seed (defaults to random value)
        #[arg(long)]
        seed: Option<u64>,
        /// Optional directory to store benchmark data (enables bind mounts)
        #[arg(long)]
        data_dir: Option<String>,
    },
    /// List available store adapters
    ListStores,
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

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Suppress the noise from the KurrentDB Rust client
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::new(&cli.log).add_directive("kurrentdb::grpc=off".parse()?),
        )
        .init();

    let rt = Runtime::new()?;
    let cancel_token = CancellationToken::new();
    let ct = cancel_token.clone();

    // Spawn Ctrl+C handler
    rt.spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to listen for ctrl_c");
        println!("\nInterrupt received, shutting down...");
        ct.cancel();
    });

    match cli.command {
        Commands::ListStores => {
            for f in store_manager_factories() {
                println!("{}", f.name());
            }
            Ok(())
        }
        Commands::Run { config, seed, data_dir } => {
            rt.block_on(async { run_benchmark(&config, seed, data_dir, cancel_token).await })?;
            Ok(())
        }
    }
}

async fn run_benchmark(config_path: &PathBuf, seed: Option<u64>, data_dir: Option<String>, cancel_token: CancellationToken) -> Result<()> {
    // Resolve data_dir to an absolute path if provided
    let data_dir = if let Some(path) = data_dir {
        let abs_path = fs::canonicalize(&path)
            .or_else(|_| {
                // If it doesn't exist yet, create it and then canonicalize
                fs::create_dir_all(&path)?;
                fs::canonicalize(&path)
            })?;
        Some(abs_path.to_string_lossy().to_string())
    } else {
        None
    };

    // Read config file
    let config_yaml = fs::read_to_string(config_path)?;

    // Extract workload name from config
    let workload_name = WorkloadFactory::extract_workload_name(&config_yaml)?;
    println!("Running workload: {}", workload_name);

    // Determine which stores to run
    let stores_to_run = if let Some(stores) = WorkloadFactory::extract_stores(&config_yaml)? {
        stores
    } else {
        // Default to all stores
        store_manager_factories()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect()
    };
    println!("Stores: {}", stores_to_run.join(", "));

    // Decide random seed.
    let actual_seed = seed.unwrap_or_else(|| rand::thread_rng().gen());
    println!("Seed: {}", actual_seed);

    // Collect environment info
    let data_dir_path = data_dir.as_ref().map(Path::new);
    let environment_info = collect_environment_info(data_dir_path).await?;

    // Get benchmark version (git commit)
    let benchmark_version = get_git_commit_hash().unwrap_or_else(|_| "unknown".to_string());

    // Detect if this is a sweep and expand if needed
    let is_sweep = WorkloadFactory::is_sweep(&config_yaml)?;
    let workloads = if is_sweep {
        WorkloadFactory::expand_sweep(&config_yaml, actual_seed)?
    } else {
        vec![WorkloadFactory::create_from_yaml(&config_yaml, actual_seed)?]
    };

    println!("Sweep mode: {}", if is_sweep { "enabled" } else { "disabled" });
    if is_sweep {
        println!("Running {} workload variants", workloads.len());
    }

    // Generate session ID (ISO timestamp)
    let session_id = Utc::now().format("%Y-%m-%dT%H-%M-%S").to_string();
    println!("Session ID: {}", session_id);

    // Create session results directory
    let session_results_path = PathBuf::from("results/raw/sessions").join(&session_id);
    fs::create_dir_all(&session_results_path)?;

    // Write session metadata
    let session_metadata = SessionMetadata {
        session_id: session_id.clone(),
        benchmark_version,
        workload_name: workload_name.clone(),
        workload_type: "performance".to_string(), // TODO: Extract from workload
        config_file: config_path.to_string_lossy().to_string(),
        seed: actual_seed,
        stores_run: stores_to_run.clone(),
        is_sweep,
    };

    let session_json = serde_json::to_string_pretty(&session_metadata)?;
    fs::write(session_results_path.join("session.json"), session_json)?;

    // Write environment info
    let environment_json = serde_json::to_string_pretty(&environment_info)?;
    fs::write(session_results_path.join("environment.json"), environment_json)?;

    // Copy config file to session directory
    fs::copy(config_path, session_results_path.join("config.yaml"))?;

    // Run each workload variant
    for workload in workloads {
        let workload_name = &workload.name().expect("workload name");

        // Create workload directory
        let workload_results_path = session_results_path.join(workload_name);
        fs::create_dir_all(&workload_results_path)?;

        // Run workload for each store
        for store_name in &stores_to_run {
            if cancel_token.is_cancelled() {
                break;
            }
            println!("\n=== Running {} on {} ===", workload_name, store_name);

            // Find store factory
            let store_factory = store_manager_factories()
                .into_iter()
                .find(|f| f.name() == store_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown store: {}", store_name))?;

            // Create store manager
            let store_manager = store_factory.create_store_manager(data_dir.clone())?;

            // Execute the run
            let (
                container_metrics,
                workload_results,
            ) = match execute_run(store_manager, &workload, cancel_token.clone()).await {
                Ok(res) => res,
                Err(e) => {
                    if cancel_token.is_cancelled() {
                        println!("Run interrupted, skipping results for {}", store_name);
                        continue;
                    }
                    return Err(e);
                }
            };

            // Create store results directory
            let store_results_path = workload_results_path.join(store_name);
            fs::create_dir_all(&store_results_path)?;

            // Write metadata
            let metadata = serde_json::json!({
                "type": &workload.type_str()?,
            });
            let metadata_json = serde_json::to_string_pretty(&metadata)?;
            fs::write(store_results_path.join("run.meta.json"), metadata_json)?;

            // Write container metrics
            let container_json = serde_json::to_string_pretty(&container_metrics)?;
            fs::write(store_results_path.join("container.json"), container_json)?;

            // Write workload results (throughput and latency)
            workload_results.write_to_dir(&store_results_path)?;

            println!(
                "✓ {} completed",
                store_name
            );
        }
    }

    println!("\n✓ Session complete: {}", session_results_path.display());
    Ok(())
}

