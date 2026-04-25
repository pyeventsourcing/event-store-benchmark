import argparse
from collections import defaultdict
from pathlib import Path

from .data_loader import load_session_workloads, load_session_metadata
from .reporting import plotting, html
from .workloads.performance import PerformanceWorkloadRun


def main() -> None:
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

        # Load session metadata
        session_metadata = load_session_metadata(raw_session_dir)
        if session_metadata is None:
            continue
        # Load all workload runs for the session
        session_workloads = load_session_workloads(raw_session_dir)
        if not session_workloads:
            print(f"No valid workloads found for session {session_id}. Skipping.")
            continue

        workload_summaries = {}
        all_runs = []

        for orig_yaml_config, workload_config, workload_runs in session_workloads:
            print(f"  Processing workload: {workload_config.name}")
            if not workload_runs:
                continue
            
            all_runs.extend(workload_runs)

            workload_dir = published_session_dir / workload_config.name
            workload_dir.mkdir(parents=True, exist_ok=True)

            # --- Generate individual run reports ---
            for run in workload_runs:
                report_dir_name = run.adapter
                if run.readers > 0:
                    report_dir_name += f"-r{run.readers}"
                if run.writers > 0:
                    report_dir_name += f"-w{run.writers}"
                report_dir = workload_dir / report_dir_name
                report_dir.mkdir(parents=True, exist_ok=True)

                plotting.plot_latency_cdf(run, str(report_dir / "latency_cdf.png"))
                plotting.plot_throughput_timeseries(run, str(report_dir / "throughput_timeseries.png"))
                plotting.plot_cpu_timeseries(run, str(report_dir / "cpu_timeseries.png"))
                plotting.plot_memory_timeseries(run, str(report_dir / "memory_timeseries.png"))
                plotting.plot_tool_latency_cdf(run, str(report_dir / "tool_latency_cdf.png"))
                plotting.plot_tool_cpu_timeseries(run, str(report_dir / "tool_cpu_timeseries.png"))
                plotting.plot_tool_memory_timeseries(run, str(report_dir / "tool_memory_timeseries.png"))
                html.generate_run_html(report_dir, run)

            # --- Generate consolidated workload reports ---
            store_order = workload_config.stores
            store_order_map = {name: i for i, name in enumerate(store_order)}
            get_store_rank = lambda name: store_order_map.get(name, 999)

            worker_groups = defaultdict[int, list[PerformanceWorkloadRun]](list)
            for run in workload_runs:
                worker_groups[run.worker_count].append(run)

            # Generate comparison plots for each worker count
            first_run = workload_runs[0]
            # Base label for pluralization in scaling plots
            worker_label = "Readers" if first_run.is_read_workload else "Writers"
            worker_suffix = "r" if first_run.is_read_workload else "w"

            for wc, group_runs in sorted(worker_groups.items()):
                # Pluralize based on worker count
                curr_label = "Reader" if first_run.is_read_workload else "Writer"
                if wc != 1:
                    curr_label += "s"

                plotting.plot_worker_slice_latency_cdf(
                    group_runs, f"Latency CDF — {wc} {curr_label}",
                    str(workload_dir / f"worker_slice_{worker_suffix}{wc}_latency_cdf.png"),
                    get_store_rank)

                plotting.plot_worker_slice_throughput(
                    group_runs, f"Throughput — {wc} {curr_label}",
                    str(workload_dir / f"worker_slice_{worker_suffix}{wc}_throughput.png"),
                    get_store_rank)

                plotting.plot_worker_slice_cpu(
                    group_runs, f"CPU Usage — {wc} {curr_label}",
                    str(workload_dir / f"worker_slice_{worker_suffix}{wc}_cpu_timeseries.png"),
                    get_store_rank)

                plotting.plot_worker_slice_memory(
                    group_runs, f"Memory Usage — {wc} {curr_label}",
                    str(workload_dir / f"worker_slice_{worker_suffix}{wc}_memory_timeseries.png"),
                    get_store_rank)

                plotting.plot_worker_slice_tool_latency_cdf(
                    group_runs, f"Tool Latency CDF — {wc} {curr_label}",
                    str(workload_dir / f"worker_slice_{worker_suffix}{wc}_tool_latency_cdf.png"),
                    get_store_rank)

                plotting.plot_worker_slice_tool_cpu(
                    group_runs, f"Tool CPU Usage — {wc} {curr_label}",
                    str(workload_dir / f"worker_slice_{worker_suffix}{wc}_tool_cpu_timeseries.png"),
                    get_store_rank)

                plotting.plot_worker_slice_tool_memory(
                    group_runs, f"Tool Memory Usage — {wc} {curr_label}",
                    str(workload_dir / f"worker_slice_{worker_suffix}{wc}_tool_memory_timeseries.png"),
                    get_store_rank)

            # Always generate scaling plots
            plotting.plot_throughput_by_workers(workload_runs, str(workload_dir / "by_workers_throughput.png"),
                                             get_store_rank)
            plotting.plot_latency_by_workers(workload_runs, str(workload_dir / "by_workers_latency.png"),
                                          get_store_rank)
            plotting.plot_cpu_by_workers(workload_runs, str(workload_dir / "by_workers_cpu.png"),
                                      get_store_rank)
            plotting.plot_memory_by_workers(workload_runs, str(workload_dir / "by_workers_memory.png"),
                                         get_store_rank)
            plotting.plot_tool_latency_by_workers(workload_runs, str(workload_dir / "by_workers_tool_latency.png"),
                                                   get_store_rank)
            plotting.plot_tool_cpu_by_workers(workload_runs, str(workload_dir / "by_workers_tool_cpu.png"),
                                               get_store_rank)
            plotting.plot_tool_memory_by_workers(workload_runs, str(workload_dir / "by_workers_tool_memory.png"),
                                                  get_store_rank)

            # Generate main workload HTML
            html.generate_workload_html(
                published_session_dir,
                workload_config.name,
                workload_runs,
                worker_groups,
                orig_yaml_config,
                get_store_rank,
            )

            workload_summaries[workload_config.name] = {
                'worker_counts': set(worker_groups.keys())
            }

        # Generate container stats plots for the whole session
        if all_runs:
            # We use the store order from the last processed workload, or default if none
            # In a session, store order should ideally be consistent.
            plotting.plot_image_size(all_runs, str(published_session_dir / "image_size.png"),
                                     get_store_rank)
            plotting.plot_startup_time(all_runs, str(published_session_dir / "startup_time.png"),
                                       get_store_rank)

        # Generate the main index page for the session
        html.generate_session_index(published_session_dir, workload_summaries, session_metadata)

    # Finally, update the top-level index of all sessions
    html.generate_top_level_index(raw_base, published_base)
    print(f"\nTop-level index written to {published_base}/index.html")


if __name__ == "__main__":
    main()