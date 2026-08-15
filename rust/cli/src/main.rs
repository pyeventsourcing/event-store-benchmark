use anyhow::Result;
use bench_core::workloads::performance::WorkloadConfig;
use bench_core::{
    collect_environment_info, HarnessProvenance, PerformanceWorkload, SessionInfo,
    StoreManagerFactory, WorkloadRunner,
};
use bench_testcontainers::detect_docker_host;
use chrono::Utc;
use clap::{Parser, Subcommand};
use rand::random;
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};
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
    /// Create tables for postgres-dcb-ttcte adapter
    CreatePostgresDcbTtcteTables,
    /// Drop tables for postgres-dcb-ttcte adapter
    DropPostgresDcbTtcteTables,
    /// Create tables for marten adapter
    CreateMartenTables,
    /// Drop tables for marten adapter
    DropMartenTables,
    /// Read all events and find the maximum timestamp in metadata
    ReadMaxTimestamp {
        /// Store name
        store: String,
    },
}

fn store_manager_factories() -> Vec<Box<dyn StoreManagerFactory>> {
    let mut factories: Vec<Box<dyn StoreManagerFactory>> = vec![];
    factories.push(Box::new(dummy_adapter::DummyFactory));

    #[cfg(feature = "umadb")]
    {
        factories.push(Box::new(umadb_adapter::UmaDbFactory));
    }

    #[cfg(feature = "tephra")]
    {
        factories.push(Box::new(tephra_adapter::TephraFactory));
    }

    #[cfg(feature = "kurrentdb")]
    {
        factories.push(Box::new(kurrentdb_adapter::KurrentDbFactory));
    }

    #[cfg(feature = "axonserver")]
    {
        factories.push(Box::new(axonserver_adapter::AxonServerFactory));
    }

    #[cfg(feature = "eventsourcingdb")]
    {
        factories.push(Box::new(eventsourcingdb_adapter::EventsourcingDbFactory));
    }

    #[cfg(feature = "fact")]
    {
        factories.push(Box::new(fact_adapter::FactFactory));
    }

    #[cfg(feature = "marten")]
    {
        factories.push(Box::new(marten_adapter::MartenFactory));
    }

    #[cfg(feature = "postgres-dcb-ttcte")]
    {
        factories.push(Box::new(
            postgres_dcb_ttcte_adapter::PostgresDcbTtcteFactory,
        ));
    }

    #[cfg(feature = "foundationdb-dcb")]
    {
        factories.push(Box::new(foundationdb_dcb_adapter::FoundationDbDcbFactory));
    }

    factories
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Suppress the noise from the KurrentDB Rust client
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new(&cli.log).add_directive("kurrentdb::grpc=off".parse()?))
        .init();

    // Must run before Runtime::new() — set_var is unsound under concurrent env reads.
    detect_docker_host();

    // Raise the blocking-pool ceiling (default 512): the tephra adapter runs its synchronous
    // client on `spawn_blocking`, so one blocking thread is live per concurrent worker. The
    // default would cap tephra at 512 concurrent ops and skew high-concurrency comparisons.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .max_blocking_threads(4096)
        .build()?;
    let cancel_token = CancellationToken::new();
    let ct = cancel_token.clone();

    // Spawn Ctrl+C handler
    rt.spawn(async move {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl_c");
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
        Commands::CreatePostgresDcbTtcteTables => {
            #[cfg(feature = "postgres-dcb-ttcte")]
            {
                rt.block_on(async {
                    let factory = postgres_dcb_ttcte_adapter::PostgresDcbTtcteFactory {};
                    let mut manager = factory.create_store_manager(None, false)?;
                    let adapter = manager.create_adapter().await?;
                    // We know it's a PostgresDcbTtcteAdapter
                    let adapter = adapter
                        .as_any()
                        .downcast_ref::<postgres_dcb_ttcte_adapter::PostgresDcbTtcteAdapter>()
                        .expect("Adapter should be PostgresDcbTtcteAdapter");
                    adapter.recorder().create_tables().await?;
                    println!("postgres-dcb-ttcte tables created successfully");
                    Ok::<(), anyhow::Error>(())
                })?;
            }
            Ok(())
        }
        Commands::DropPostgresDcbTtcteTables => {
            #[cfg(feature = "postgres-dcb-ttcte")]
            {
                rt.block_on(async {
                    let factory = postgres_dcb_ttcte_adapter::PostgresDcbTtcteFactory {};
                    let mut manager = factory.create_store_manager(None, false)?;
                    let adapter = manager.create_adapter().await?;
                    // We know it's a PostgresDcbTtcteAdapter
                    let adapter = adapter
                        .as_any()
                        .downcast_ref::<postgres_dcb_ttcte_adapter::PostgresDcbTtcteAdapter>()
                        .expect("Adapter should be PostgresDcbTtcteAdapter");
                    adapter.recorder().drop_tables().await?;
                    println!("postgres-dcb-ttcte tables dropped successfully");
                    Ok::<(), anyhow::Error>(())
                })?;
            }
            Ok(())
        }
        Commands::CreateMartenTables => {
            #[cfg(feature = "marten")]
            {
                rt.block_on(async {
                    let factory = marten_adapter::MartenFactory {};
                    let mut manager = factory.create_store_manager(None, false)?;
                    let adapter = manager.create_adapter().await?;
                    // We know it's a MartenAdapter
                    let adapter = adapter
                        .as_any()
                        .downcast_ref::<marten_adapter::MartenAdapter>()
                        .expect("Adapter should be MartenAdapter");
                    // We need a way to use the client. Marten is not Clone easily if it has a client.
                    // Actually, let's just use the connect if we can.
                    adapter.client().create_tables().await?;

                    println!("marten tables created successfully");
                    Ok::<(), anyhow::Error>(())
                })?;
            }
            Ok(())
        }
        Commands::DropMartenTables => {
            #[cfg(feature = "marten")]
            {
                rt.block_on(async {
                    let factory = marten_adapter::MartenFactory {};
                    let mut manager = factory.create_store_manager(None, false)?;
                    let adapter = manager.create_adapter().await?;
                    // We know it's a MartenAdapter
                    let adapter = adapter
                        .as_any()
                        .downcast_ref::<marten_adapter::MartenAdapter>()
                        .expect("Adapter should be MartenAdapter");
                    // We need a way to use the client. Marten is not Clone easily if it has a client.
                    // Actually, let's just use the connect if we can.
                    adapter.client().drop_tables().await?;

                    println!("marten tables dropped successfully");
                    Ok::<(), anyhow::Error>(())
                })?;
            }
            Ok(())
        }
        Commands::ReadMaxTimestamp { store } => {
            rt.block_on(async {
                let store_factory = store_manager_factories()
                    .into_iter()
                    .find(|f| f.name() == store)
                    .ok_or_else(|| anyhow::anyhow!("Unknown store: {}", store))?;

                let mut manager = store_factory.create_store_manager(None, false)?;

                let max_retries = 30;
                let mut attempts = 0;

                let (min_timestamp, max_timestamp, count) = loop {
                    attempts += 1;

                    // Especially to copy with Axon Server's...
                    //
                    //  'Client specified an invalid argument', message:
                    //  "[AXONIQ-2308] Operation not supported on non-DCB context"
                    //
                    // ...we wrap the actual connection and read logic in a sub-async block
                    // so we can easily catch any anyhow::Error and retry.
                    let result = async {
                        let adapter = manager.create_adapter().await?;
                        let mut read_response = adapter.read_all().await?;

                        let mut current_min: Option<u128> = None;
                        let mut current_max: Option<u128> = None;
                        let mut timestamp_count = 0u64;

                        while let Some(event) = read_response.next_event().await? {
                            for (key, value) in &event.metadata {
                                if key == "timestamp" {
                                    timestamp_count += 1;
                                    if let Ok(ts) = value.parse::<u128>() {
                                        current_max = match current_max {
                                            Some(max) if ts > max => Some(ts),
                                            Some(max) => Some(max),
                                            None => Some(ts),
                                        };
                                        current_min = match current_min {
                                            Some(min) if ts < min => Some(ts),
                                            Some(min) => Some(min),
                                            None => Some(ts),
                                        };
                                    }
                                    break
                                }
                            }
                        }

                        Ok::<(Option<u128>, Option<u128>, u64), anyhow::Error>((current_min, current_max, timestamp_count))
                    }.await;

                    match result {
                        Ok(result) => break result,
                        Err(e) => {
                            if attempts >= max_retries {
                                return Err(anyhow::anyhow!(
                            "Gave up after {} attempts. Last error: {}",
                            max_retries, e
                        ));
                            }

                            // Print to stderr so it doesn't pollute stdout if you are parsing the final number
                            eprintln!("⚠️ [Attempt {}/{}] Store not ready: {}. Retrying in 2s...", attempts, max_retries, e);

                            // Wait 2 seconds before trying again, unless interrupted
                            tokio::select! {
                                _ = tokio::time::sleep(std::time::Duration::from_secs(2)) => {}
                                _ = cancel_token.cancelled() => {
                                    return Err(anyhow::anyhow!("Interrupted while waiting to retry"));
                                }
                            }
                        }
                    }
                };

                println!("Total received timestamps: {}", count);
                match min_timestamp {
                    Some(ts) => println!("Min received timestamp: {} ns", ts),
                    None => {},
                }
                match max_timestamp {
                    Some(ts) => println!("Max received timestamp: {} ns", ts),
                    None => {},
                }

                Ok::<(), anyhow::Error>(())
            })?;
            Ok(())
        }
        Commands::Run {
            config,
            seed,
            data_dir,
        } => {
            rt.block_on(async { run_benchmark(&config, seed, data_dir, cancel_token).await })?;
            Ok(())
        }
    }
}

async fn run_benchmark(
    session_config_path: &PathBuf,
    seed: Option<u64>,
    data_dir: Option<String>,
    cancel_token: CancellationToken,
) -> Result<()> {
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
        let abs_path = fs::canonicalize(&path).or_else(|_| {
            // If it doesn't exist yet, create it and then canonicalize
            fs::create_dir_all(&path)?;
            fs::canonicalize(&path)
        })?;
        Some(abs_path.to_string_lossy().to_string())
    } else {
        None
    };
    println!(
        "Data path: {:?}",
        data_dir.clone().unwrap_or("".to_string())
    );

    // Read config file
    let session_config_yaml = fs::read_to_string(session_config_path)?;

    // Create session results directory
    let session_results_path = PathBuf::from("results").join(format!("esb-{}", session_id));
    fs::create_dir_all(&session_results_path)?;

    // Record session config
    fs::write(
        session_results_path.join("config.yaml"),
        session_config_yaml.clone(),
    )?;

    // 1. Collect all expanded workloads and ensure unique base names across the whole session
    let mut all_workload_runs = Vec::new();
    let mut original_workload_names = std::collections::HashSet::new();

    for document in serde_yaml::Deserializer::from_str(&session_config_yaml) {
        let value = WorkloadConfig::deserialize(document)?;
        if let Some(mut original_workload_config) = value.performance {
            if let Ok(stores_override) = std::env::var("ESB_WORKLOAD_STORES") {
                original_workload_config.stores = stores_override.into();
            }
            if original_workload_names.contains(&original_workload_config.name) {
                anyhow::bail!("Duplicate base workload name detected: {}. Please ensure all workload names in the session config are unique.", original_workload_config.name);
            }
            original_workload_names.insert(original_workload_config.name.clone());

            for workload_run_config in original_workload_config.expand() {
                all_workload_runs
                    .push((original_workload_config.name.clone(), workload_run_config));
            }
        }
    }

    if all_workload_runs.is_empty() {
        return Ok(());
    }

    let total_runs = all_workload_runs.len();
    println!("Total runs to execute: {}", total_runs);
    let runs_started = std::time::Instant::now();

    // 2. Write session metadata and environment info (once per session).
    // Provenance is baked into the binary at build time (see `rust/cli/build.rs`), so it is
    // always present regardless of whether git is reachable at run time. A runtime
    // `ESB_GIT_VERSION` still overrides the short sha (e.g. for release-pipeline stamping).
    let provenance = HarnessProvenance {
        es_bench_version: env!("CARGO_PKG_VERSION").to_string(),
        git_sha: env!("ESB_GIT_SHA").to_string(),
        git_sha_short: std::env::var("ESB_GIT_VERSION")
            .unwrap_or_else(|_| env!("ESB_GIT_SHA_SHORT").to_string()),
        git_dirty: env!("ESB_GIT_DIRTY") == "true",
        build_timestamp: env!("ESB_BUILD_TIMESTAMP").to_string(),
        rustc_version: env!("ESB_RUSTC_VERSION").to_string(),
    };
    let session_metadata = SessionInfo {
        session_id: session_id.clone(),
        // Kept for backward compatibility with existing tooling; mirrors the short sha.
        tool_version: provenance.git_sha_short.clone(),
        workload_name: session_config_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
            .to_string(),
        config_file: session_config_path.to_string_lossy().to_string(),
        seed: actual_seed,
        provenance,
    };
    fs::write(
        session_results_path.join("session.json"),
        serde_json::to_string_pretty(&session_metadata)?,
    )?;

    let data_dir_path = data_dir.as_ref().map(Path::new);
    let environment_info = collect_environment_info(data_dir_path).await?;
    fs::write(
        session_results_path.join("environment.json"),
        serde_json::to_string_pretty(&environment_info)?,
    )?;

    // 3. Execute all runs
    for (run_index, (original_workload_name, workload_run_config)) in
        all_workload_runs.into_iter().enumerate()
    {
        if cancel_token.is_cancelled() {
            break;
        }

        let use_docker = std::env::var("ESB_USE_DOCKER")
            .map(|val| matches!(val.to_lowercase().as_str(), "true" | "1"))
            .unwrap_or(workload_run_config.use_docker);
        let workload_runner = WorkloadRunner::Performance(PerformanceWorkload::from_config(
            workload_run_config,
            actual_seed,
        )?);
        let workload_run_name = workload_runner.name()?.to_string();

        // Progress line: which run this is, elapsed session time, and a rough ETA from the
        // average run duration so far (so `tail -f` shows how much is left).
        let elapsed_s = runs_started.elapsed().as_secs();
        let eta_note = if run_index > 0 {
            let per = runs_started.elapsed().as_secs_f64() / run_index as f64;
            let remaining = (per * (total_runs - run_index) as f64) as u64;
            format!(", ~{}m{:02}s left", remaining / 60, remaining % 60)
        } else {
            String::new()
        };
        println!(
            "\n=== [{}/{}] Running {} / {} (elapsed {}m{:02}s{}) ===",
            run_index + 1,
            total_runs,
            original_workload_name,
            workload_run_name,
            elapsed_s / 60,
            elapsed_s % 60,
            eta_note
        );

        // Create workload run results directory (results/esb-<session_id>/<original_workload_name>/<workload_run_name>)
        let run_results_path = session_results_path
            .join(&original_workload_name)
            .join(&workload_run_name);
        fs::create_dir_all(&run_results_path)?;

        // Find store factory
        let store_name = workload_runner.store_name()?;
        let store_factory = store_manager_factories()
            .into_iter()
            .find(|f| f.name() == store_name)
            .ok_or_else(|| anyhow::anyhow!("Unknown store: {}", store_name))?;

        // Create store manager
        let store_manager = store_factory.create_store_manager(data_dir.clone(), use_docker)?;

        // Run the workload
        let run_results = match workload_runner
            .execute(store_manager, cancel_token.clone())
            .await
        {
            Ok(run_results) => run_results,
            Err(e) => {
                if cancel_token.is_cancelled() {
                    println!("Run interrupted, skipping results for {}", store_name);
                    continue;
                }
                println!("Error executing run for {}: {}", store_name, e);
                continue;
            }
        };

        // Write run results
        run_results.write_to_dir(&run_results_path)?;

        println!("✓ {} on {} completed", workload_run_name, store_name);
    }

    println!("\n✓ Session complete: {}", session_results_path.display());
    Ok(())
}
