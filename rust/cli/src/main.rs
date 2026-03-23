use anyhow::Result;
use bench_core::{
    collect_environment_info, execute_run, get_git_commit_hash, SessionMetadata,
    StoreManagerFactory, WorkloadFactory,
};
use chrono::Utc;
use clap::{Parser, Subcommand};
use rand::Rng;
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
    },
    /// List available store adapters
    ListStores,
    /// Generate analytics report from session data
    Report {
        /// Path to sessions directory (default: results/raw/sessions)
        #[arg(long, default_value = "results/raw/sessions")]
        sessions: PathBuf,
        /// Output directory for report (default: results/published)
        #[arg(long, default_value = "results/published")]
        output: PathBuf,
    },
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

    match cli.command {
        Commands::ListStores => {
            for f in store_manager_factories() {
                println!("{}", f.name());
            }
            Ok(())
        }
        Commands::Run { config, seed } => {
            run_benchmark(&config, seed)?;
            Ok(())
        }
        Commands::Report { sessions, output } => {
            generate_report(&sessions, &output)?;
            Ok(())
        }
    }
}

fn run_benchmark(config_path: &PathBuf, seed: Option<u64>) -> Result<()> {
    let actual_seed = seed.unwrap_or_else(|| rand::thread_rng().gen());

    // Read config file
    let config_yaml = fs::read_to_string(config_path)?;

    // Extract workload name and stores from config
    let workload_name = WorkloadFactory::extract_workload_name(&config_yaml)?;
    let stores_from_config = WorkloadFactory::extract_stores(&config_yaml)?;

    // Determine which stores to run
    let stores_to_run = if let Some(stores) = stores_from_config {
        stores
    } else {
        // Default to all stores
        store_manager_factories()
            .into_iter()
            .map(|f| f.name().to_string())
            .collect()
    };

    println!("Running workload: {}", workload_name);
    println!("Stores: {}", stores_to_run.join(", "));
    println!("Seed: {}", actual_seed);

    // Generate session ID (ISO timestamp)
    let session_id = Utc::now().format("%Y-%m-%dT%H-%M-%S").to_string();
    println!("Session ID: {}", session_id);

    // Collect environment info
    let environment_info = collect_environment_info()?;

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

    // Create session directory
    let session_dir = PathBuf::from("results/raw/sessions").join(&session_id);
    fs::create_dir_all(&session_dir)?;

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
    fs::write(session_dir.join("session.json"), session_json)?;

    // Write environment info
    let environment_json = serde_json::to_string_pretty(&environment_info)?;
    fs::write(session_dir.join("environment.json"), environment_json)?;

    // Copy config file to session directory
    fs::copy(config_path, session_dir.join("config.yaml"))?;

    // Run each workload variant
    for workload in workloads {
        let workload_name = match &workload {
            bench_core::Workload::Performance(w) => w.name(),
            _ => "unknown",
        };

        // Create workload directory
        let workload_dir = session_dir.join(workload_name);
        fs::create_dir_all(&workload_dir)?;

        // Run workload for each store
        for store_name in &stores_to_run {
            println!("\n=== Running {} on {} ===", workload_name, store_name);

            // Find store factory
            let store_factory = store_manager_factories()
                .into_iter()
                .find(|f| f.name() == store_name)
                .ok_or_else(|| anyhow::anyhow!("Unknown store: {}", store_name))?;

            // Create store manager
            let store_manager = store_factory.create_store_manager()?;

            // Create store directory
            let store_dir = workload_dir.join(store_name);
            fs::create_dir_all(&store_dir)?;

            // Execute the run
            let rt = Runtime::new()?;
            let result = rt.block_on(async { execute_run(store_manager, &workload).await })?;

            // Write summary
            let summary_json = serde_json::to_string_pretty(&result.summary)?;
            fs::write(store_dir.join("summary.json"), summary_json)?;

            // Write samples
            let mut samples_lines = String::new();
            for sample in result.samples {
                samples_lines.push_str(&serde_json::to_string(&sample)?);
                samples_lines.push('\n');
            }
            fs::write(store_dir.join("samples.jsonl"), samples_lines)?;

            // Write metadata with sample rate
            let metadata = serde_json::json!({
                "sample_rate": result.sample_rate,
            });
            let metadata_json = serde_json::to_string_pretty(&metadata)?;
            fs::write(store_dir.join("run.meta.json"), metadata_json)?;

            println!(
                "✓ {} completed: {:.2} events/sec",
                store_name, result.summary.throughput_eps
            );
        }
    }

    println!("\n✓ Session complete: {}", session_dir.display());
    Ok(())
}

fn generate_report(sessions_path: &PathBuf, output_path: &PathBuf) -> Result<()> {
    let generator = analytics::ReportGenerator::new(sessions_path, output_path);
    generator.generate()?;
    Ok(())
}
