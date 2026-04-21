import argparse
from collections import defaultdict
from pathlib import Path

from .data_loader import load_session_workloads, load_session_metadata
from .reporting import plotting, html


def main():
    parser = argparse.ArgumentParser(description="Generate ES-BENCH benchmark report from raw results")
    parser.add_argument("--raw", type=str, default="results/raw", help="Path to raw results dir")
    parser.add_argument("--out", type=str, default="results/published", help="Output reports dir")
    parser.add_argument("--force", action="store_true", help="Force regeneration of already published sessions")
    args = parser.parse_args()

    raw_base = Path(args.raw)
    published_base = Path(args.out)
    published_base.mkdir(parents=True, exist_ok=True)

    if not raw_base.exists() or not raw_base.is_dir():
        print(f"No sessions found in {raw_base}")
        return

    all_session_ids = sorted([d.name for d in raw_base.iterdir() if d.is_dir()])
    if not all_session_ids:
        print(f"No sessions found in {raw_base}")
        return

    sessions_to_process = []
    if args.force:
        sessions_to_process = all_session_ids
    else:
        for session_id in all_session_ids:
            if not (published_base / session_id).exists():
                sessions_to_process.append(session_id)

    if not sessions_to_process:
        print(f"No new sessions to process in {raw_base}")

    for session_id in sessions_to_process:
        print(f"Processing session: {session_id}")
        raw_session_dir = raw_base / session_id
        published_session_dir = published_base / session_id
        published_session_dir.mkdir(parents=True, exist_ok=True)

        # Load all session metadata
        metadata = load_session_metadata(raw_session_dir)
        session_info = metadata["session_info"]
        env_info_obj = metadata["env_info"]

        # Load all workload runs for the session
        workloads = load_session_workloads(raw_session_dir)
        if not workloads:
            print(f"No valid workloads found for session {session_id}. Skipping.")
            continue

        workload_summaries = {}

        for workload_name, data in workloads.items():
            print(f"  Processing workload: {workload_name}")
            workload_config = data["config"]
            runs = data["runs"]
            if not runs:
                continue

            workload_dir = published_session_dir / workload_name
            workload_dir.mkdir(parents=True, exist_ok=True)

            # --- Generate individual run reports ---
            for run in runs:
                report_dir_name = f"report-{run.adapter}-r{run.readers:03d}-w{run.writers:03d}"
                report_dir = workload_dir / report_dir_name
                report_dir.mkdir(parents=True, exist_ok=True)

                plotting.plot_latency_cdf(run, str(report_dir / "latency_cdf.png"))
                plotting.plot_throughput_timeseries(run, str(report_dir / "throughput_over_time.png"))
                plotting.plot_cpu_timeseries(run, str(report_dir / "cpu_over_time.png"))
                plotting.plot_memory_timeseries(run, str(report_dir / "memory_over_time.png"))
                plotting.plot_benchmark_latency_cdf(run, str(report_dir / "benchmark_latency_cdf.png"))
                plotting.plot_benchmark_cpu_timeseries(run, str(report_dir / "benchmark_cpu_over_time.png"))
                plotting.plot_benchmark_memory_timeseries(run, str(report_dir / "benchmark_memory_over_time.png"))
                html.generate_run_html(report_dir, run)

            # --- Generate consolidated workload reports ---
            store_order = workload_config.get("stores", [])
            store_order_map = {name: i for i, name in enumerate(store_order)}
            get_store_rank = lambda name: store_order_map.get(name, 999)

            worker_groups = defaultdict(list)
            for run in runs:
                worker_groups[run.worker_count].append(run)

            # Generate comparison plots for each worker count
            first_run = runs[0]
            worker_label = "Readers" if first_run.is_read_workload else "Writers"
            worker_suffix = "r" if first_run.is_read_workload else "w"

            for wc, group_runs in sorted(worker_groups.items()):
                plotting.plot_comparison_latency_cdf(
                    group_runs, f"Latency CDF — {wc} {worker_label}(s)",
                    str(workload_dir / f"comparison_{worker_suffix}{wc}_latency_cdf.png"),
                    get_store_rank)

                plotting.plot_comparison_throughput(
                    group_runs, f"Throughput — {wc} {worker_label}(s)",
                    str(workload_dir / f"comparison_{worker_suffix}{wc}_throughput_over_time.png"),
                    get_store_rank)

                plotting.plot_comparison_cpu(
                    group_runs, f"CPU Usage — {wc} {worker_label}(s)",
                    str(workload_dir / f"comparison_{worker_suffix}{wc}_cpu_over_time.png"),
                    get_store_rank)

                plotting.plot_comparison_memory(
                    group_runs, f"Memory Usage — {wc} {worker_label}(s)",
                    str(workload_dir / f"comparison_{worker_suffix}{wc}_memory_over_time.png"),
                    get_store_rank)

                plotting.plot_comparison_benchmark_latency_cdf(
                    group_runs, f"Benchmark Latency CDF — {wc} {worker_label}(s)",
                    str(workload_dir / f"comparison_{worker_suffix}{wc}_benchmark_latency_cdf.png"),
                    get_store_rank)

                plotting.plot_comparison_benchmark_cpu(
                    group_runs, f"Benchmark CPU Usage — {wc} {worker_label}(s)",
                    str(workload_dir / f"comparison_{worker_suffix}{wc}_benchmark_cpu_over_time.png"),
                    get_store_rank)

                plotting.plot_comparison_benchmark_memory(
                    group_runs, f"Benchmark Memory Usage — {wc} {worker_label}(s)",
                    str(workload_dir / f"comparison_{worker_suffix}{wc}_benchmark_memory_over_time.png"),
                    get_store_rank)

            # Always generate scaling plots
            plotting.plot_throughput_scaling(runs, str(workload_dir / "scaling_throughput.png"),
                                             get_store_rank)
            plotting.plot_latency_scaling(runs, str(workload_dir / "scaling_latency.png"),
                                          get_store_rank)
            plotting.plot_cpu_scaling(runs, str(workload_dir / "scaling_cpu.png"),
                                      get_store_rank)
            plotting.plot_memory_scaling(runs, str(workload_dir / "scaling_memory.png"),
                                         get_store_rank)
            plotting.plot_benchmark_latency_scaling(runs, str(workload_dir / "scaling_benchmark_latency.png"),
                                                   get_store_rank)
            plotting.plot_benchmark_cpu_scaling(runs, str(workload_dir / "scaling_benchmark_cpu.png"),
                                               get_store_rank)
            plotting.plot_benchmark_memory_scaling(runs, str(workload_dir / "scaling_benchmark_memory.png"),
                                                  get_store_rank)

            # Generate container stats plots, and main workload HTML
            plotting.plot_container_stats(runs, str(workload_dir / "container_stats.png"),
                                            get_store_rank)
            html.generate_workload_html(published_session_dir, workload_name, runs, worker_groups, workload_config,
                                        get_store_rank)

            workload_summaries[workload_name] = {
                'worker_counts': set(worker_groups.keys())
            }

        # Generate the main index page for the session
        html.generate_session_index(published_session_dir, session_id, workload_summaries, env_info_obj, session_info)

    # Finally, update the top-level index of all sessions
    html.generate_top_level_index(raw_base, published_base)
    print(f"\nTop-level index written to {published_base}/index.html")


if __name__ == "__main__":
    main()