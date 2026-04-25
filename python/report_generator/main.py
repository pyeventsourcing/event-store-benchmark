import argparse
from pathlib import Path

from .data_loader import load_session_workloads, load_session_metadata
from .reporting import html
from .reporting.performance_pipeline import generate_performance_session_reports


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

        generate_performance_session_reports(
            session_id=session_id,
            raw_session_dir=raw_session_dir,
            published_session_dir=published_session_dir,
            session_metadata=session_metadata,
            session_workloads=session_workloads,
        )

    # Finally, update the top-level index of all sessions
    html.generate_top_level_index(raw_base, published_base)
    print(f"\nTop-level index written to {published_base}/index.html")


if __name__ == "__main__":
    main()