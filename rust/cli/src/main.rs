use anyhow::Result;
use bench_core::{collect_environment_info, execute_run, get_git_commit_hash, PerformanceWorkload, SessionMetadata, StoreManagerFactory, Workload};
use chrono::Utc;
use clap::{Parser, Subcommand};
use rand::random;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::runtime::Runtime;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;
use bench_core::workloads::performance::WorkloadConfig;
use serde::Deserialize;

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
    /// Create tables for py-eventsourcing adapter
    CreatePyEventsourcingTables,
    /// Drop tables for py-eventsourcing adapter
    DropPyEventsourcingTables,
    /// Create tables for marten adapter
    CreateMartenTables,
    /// Drop tables for marten adapter
    DropMartenTables,
}

fn store_manager_factories() -> Vec<Box<dyn StoreManagerFactory>> {
    vec![
        Box::new(dummy_adapter::DummyFactory),
        Box::new(umadb_adapter::UmaDbFactory),
        Box::new(kurrentdb_adapter::KurrentDbFactory),
        Box::new(axonserver_adapter::AxonServerFactory),
        Box::new(eventsourcingdb_adapter::EventsourcingDbFactory),
        Box::new(fact_adapter::FactFactory),
        Box::new(marten_adapter::MartenFactory),
        Box::new(py_eventsourcing_adapter::PyEventsourcingFactory),
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
        Commands::CreatePyEventsourcingTables => {
            rt.block_on(async {
                let factory = py_eventsourcing_adapter::PyEventsourcingFactory {};
                let manager = factory.create_store_manager(None, true)?;
                let adapter = manager.create_adapter().await?;
                // We know it's a PyEventsourcingAdapter
                let adapter = adapter.as_any().downcast_ref::<py_eventsourcing_adapter::PyEventsourcingAdapter>()
                    .expect("Adapter should be PyEventsourcingAdapter");
                adapter.recorder().create_tables().await?;
                println!("py-eventsourcing tables created successfully");
                Ok::<(), anyhow::Error>(())
            })?;
            Ok(())
        }
        Commands::DropPyEventsourcingTables => {
            rt.block_on(async {
                let factory = py_eventsourcing_adapter::PyEventsourcingFactory {};
                let manager = factory.create_store_manager(None, true)?;
                let adapter = manager.create_adapter().await?;
                // We know it's a PyEventsourcingAdapter
                let adapter = adapter.as_any().downcast_ref::<py_eventsourcing_adapter::PyEventsourcingAdapter>()
                    .expect("Adapter should be PyEventsourcingAdapter");
                adapter.recorder().drop_tables().await?;
                println!("py-eventsourcing tables dropped successfully");
                Ok::<(), anyhow::Error>(())
            })?;
            Ok(())
        }
        Commands::CreateMartenTables => {
            rt.block_on(async {
                let factory = marten_adapter::MartenFactory {};
                let manager = factory.create_store_manager(None, true)?;
                let adapter = manager.create_adapter().await?;
                // We know it's a MartenAdapter
                let adapter = adapter.as_any().downcast_ref::<marten_adapter::MartenAdapter>()
                    .expect("Adapter should be MartenAdapter");
                // We need a way to use the client. Marten is not Clone easily if it has a client.
                // Actually, let's just use the connect if we can.
                adapter.client().create_tables().await?;

                println!("marten tables created successfully");
                Ok::<(), anyhow::Error>(())
            })?;
            Ok(())
        }
        Commands::DropMartenTables => {
            rt.block_on(async {
                let factory = marten_adapter::MartenFactory {};
                let manager = factory.create_store_manager(None, true)?;
                let adapter = manager.create_adapter().await?;
                // We know it's a MartenAdapter
                let adapter = adapter.as_any().downcast_ref::<marten_adapter::MartenAdapter>()
                    .expect("Adapter should be MartenAdapter");
                // We need a way to use the client. Marten is not Clone easily if it has a client.
                // Actually, let's just use the connect if we can.
                adapter.client().drop_tables().await?;

                println!("marten tables dropped successfully");
                Ok::<(), anyhow::Error>(())
            })?;
            Ok(())
        }
        Commands::Run { config, seed, data_dir } => {
            rt.block_on(async { run_benchmark(&config, seed, data_dir, cancel_token).await })?;
            Ok(())
        }
    }
}

async fn run_benchmark(session_config_path: &PathBuf, seed: Option<u64>, data_dir: Option<String>, cancel_token: CancellationToken) -> Result<()> {
    // Generate session ID (ISO timestamp or from environment variable)
    let session_id = std::env::var("ESB_SESSION_ID")
        .unwrap_or_else(|_| Utc::now().format("%Y-%m-%dT%H-%M-%S").to_string());
    println!("Session ID: {}", session_id);

    if let Ok(stores) = std::env::var("ESB_WORKLOAD_STORES") {
        println!("Overriding stores with: {}", stores);
    }

    // Decide random seed.
    let actual_seed = seed.unwrap_or_else(random);
    println!("Seed: {}", actual_seed);

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
    println!("Data path: {:?}", data_dir.clone().unwrap_or("".to_string()));

    // Read config file
    let session_config_yaml = fs::read_to_string(session_config_path)?;

    // Create raw results directory
    let session_results_path = PathBuf::from("results/raw").join(&session_id);
    fs::create_dir_all(&session_results_path)?;

    // Record session config
    fs::write(session_results_path.join("config.yaml"), session_config_yaml.clone())?;

    // 1. Collect all expanded workloads and ensure unique base names across the whole session
    let mut all_expanded_runs = Vec::new();
    let mut seen_base_names = std::collections::HashSet::new();

    for document in serde_yaml::Deserializer::from_str(&session_config_yaml) {
        let value = WorkloadConfig::deserialize(document)?;
        if let Some(mut unexpanded) = value.performance {
            if let Ok(stores_override) = std::env::var("ESB_WORKLOAD_STORES") {
                unexpanded.stores = stores_override.into();
            }
            if seen_base_names.contains(&unexpanded.name) {
                anyhow::bail!("Duplicate base workload name detected: {}. Please ensure all workload names in the session config are unique.", unexpanded.name);
            }
            seen_base_names.insert(unexpanded.name.clone());

            for config in unexpanded.expand() {
                all_expanded_runs.push((unexpanded.name.clone(), config));
            }
        }
    }

    if all_expanded_runs.is_empty() {
        return Ok(());
    }

    println!("Total runs to execute: {}", all_expanded_runs.len());

    // 2. Write session metadata and environment info (once per session)
    let benchmark_version = get_git_commit_hash().unwrap_or_else(|_| "unknown".to_string());
    let session_metadata = SessionMetadata {
        session_id: session_id.clone(),
        benchmark_version,
        config_file: session_config_path.to_string_lossy().to_string(),
        seed: actual_seed,
    };
    fs::write(
        session_results_path.join("session.json"),
        serde_json::to_string_pretty(&session_metadata)?
    )?;

    let data_dir_path = data_dir.as_ref().map(Path::new);
    let environment_info = collect_environment_info(data_dir_path).await?;
    fs::write(
        session_results_path.join("environment.json"),
        serde_json::to_string_pretty(&environment_info)?
    )?;

    // 3. Execute all runs
    for (base_name, config) in all_expanded_runs {
        if cancel_token.is_cancelled() {
            break;
        }

        let use_docker = config.use_docker;
        let workload = Workload::Performance(PerformanceWorkload::from_config(config, actual_seed)?);
        let workload_name = workload.name()?.to_string();

        println!("\n=== Running {} (Workload: {}) ===", workload_name, base_name);

        // Create workload results directory (results/raw/<session_id>/<base_name>/<run_name>)
        let workload_results_path = session_results_path.join(&base_name).join(&workload_name);
        fs::create_dir_all(&workload_results_path)?;

        // Find store factory
        let store_name = workload.store_name()?;
        let store_factory = store_manager_factories()
            .into_iter()
            .find(|f| f.name() == store_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown store: {}", store_name))?;

        // Create store manager
        let store_manager = store_factory.create_store_manager(data_dir.clone(), use_docker)?;

        // Execute the run
        let (
            run_metrics,
            workload_results,
            throughput_samples,
            store_latency_percentiles,
            benchmark_latency_percentiles,
            cpu_samples,
            memory_samples,
            benchmark_cpu_samples,
            benchmark_memory_samples,
            container_logs,
        ) = match execute_run(store_manager, &workload, cancel_token.clone()).await {
            Ok(res) => res,
            Err(e) => {
                if cancel_token.is_cancelled() {
                    println!("Run interrupted, skipping results for {}", store_name);
                    continue;
                }
                println!("Error executing run for {}: {}", store_name, e);
                continue;
            }
        };

        // Write individual run results
        workload_results.write_to_dir(
            &workload_results_path,
            &throughput_samples,
            &store_latency_percentiles,
            &benchmark_latency_percentiles,
            cpu_samples.as_deref(),
            memory_samples.as_deref(),
            benchmark_cpu_samples.as_deref(),
            benchmark_memory_samples.as_deref(),
        )?;
        if let Some(container) = run_metrics.container {
            fs::write(
                workload_results_path.join("container_stats.json"),
                serde_json::to_string_pretty(&container)?,
            )?;
        }
        if !container_logs.is_empty() {
            fs::write(workload_results_path.join("logs.txt"), container_logs)?;
        }

        // Include the actual run config
        let run_config_yaml = serde_yaml::to_string(&workload.performance_config()?)?;
        fs::write(workload_results_path.join("config.yaml"), run_config_yaml)?;

        println!("✓ {} on {} completed", workload_name, store_name);
    }

    println!("\n✓ Session complete: {}", session_results_path.display());
    Ok(())
}

