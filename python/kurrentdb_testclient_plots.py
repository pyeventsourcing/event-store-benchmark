import re
import matplotlib.pyplot as plt
import numpy as np
import os
from typing import Any, Dict, List, Optional
from matplotlib.lines import Line2D
from matplotlib.ticker import LogLocator, NullFormatter, FormatStrFormatter, ScalarFormatter

def parse_log_file(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Parses the KurrentDB.TestClient output log.
    Extracts number of clients, throughput (reqs per sec), and latency percentiles.
    """
    with open(file_path, 'r') as f:
        content = f.read()

    # Extract number of clients from DataName or command info
    # Example: clientsCnt:16;
    clients_match = re.search(r'clientsCnt:(\d+);', content)
    if not clients_match:
        # Fallback to command line or INF logs if needed
        # [ 4084, 5,13:32:05.612,INF] Client                         Processing command: WRFLGRPC 16 ...
        clients_match = re.search(r'Processing command: WRFLGRPC (\d+)', content)
    
    if not clients_match:
        return None

    clients = int(clients_match.group(1))

    # Extract throughput
    # Example: 100000 requests completed in 7461ms (13403.03 reqs per sec).
    throughput_match = re.search(r'\((\d+\.?\d*) reqs per sec\)', content)
    throughput = float(throughput_match.group(1)) if throughput_match else 0.0

    # Extract latency percentiles
    # Example:
    # 50% : ... (not directly shown in quintiles but we can infer p50 from 40-60% or look for it)
    # The example output shows quintiles:
    # 20% : 218-6346
    # 40% : 6346-9055
    # 60% : 9055-13432
    # 80% : 13432-22807
    # 100% : 22807-76066
    # And then specific percentiles:
    # 90% : 30460
    # 95% : 36878
    # 98% : 42908
    # 99% : 46543
    # 99.5% : 51392
    # 99.9% : 58381
    
    # Let's approximate p50 as the average of 40% and 60% if 50% is not found
    # Actually, looking at the output:
    # 99% : 46543
    # 99.9% : 58381
    
    p50_match = re.search(r'50% : (\d+)', content)
    p99_match = re.search(r'99% : (\d+)', content)
    p999_match = re.search(r'99\.9% : (\d+)', content)
    
    # If p50 is not explicitly there, check quintiles
    if not p50_match:
        # 40% : 6346-9055
        # 60% : 9055-13432
        # The first number after 40% : is the 40th percentile, the second is 60th?
        # No, "40% : 6346-9055" likely means from 20% to 40% it's 6346 to 9055.
        # So 40th percentile is 9055. 60th is 13432.
        # Let's take the middle of 40% and 60% as p50.
        m40 = re.search(r'40% : \d+-(\d+)', content)
        m60 = re.search(r'60% : \d+-(\d+)', content)
        if m40 and m60:
            p50 = (float(m40.group(1)) + float(m60.group(1))) / 2.0
        else:
            p50 = 0.0
    else:
        p50 = float(p50_match.group(1))

    p99 = float(p99_match.group(1)) if p99_match else 0.0
    p999 = float(p999_match.group(1)) if p999_match else 0.0

    # Convert latencies from microseconds (presumably, based on values like 46543 for 13k req/sec) to milliseconds
    # The output says "fastest: 218", and "99.9% : 58381".
    # If it's 13403 req/sec, that's ~0.07ms per req on average. 218 might be microseconds (0.218ms).
    # 58381 microseconds is 58.3ms. This makes sense.
    p50 /= 1000.0
    p99 /= 1000.0
    p999 /= 1000.0

    print(f"Parsed {file_path}: clients={clients}, throughput={throughput}, p50={p50}, p99={p99}, p999={p999}")
    return {
        'clients': clients,
        'throughput': throughput,
        'p50': p50,
        'p99': p99,
        'p999': p999
    }

def generate_plots(results: List[Dict[str, Any]], output_dir: str) -> None:
    if not results:
        print("No results to plot.")
        return

    # Sort by number of clients
    results.sort(key=lambda x: x['clients'])
    
    clients = [r['clients'] for r in results]
    throughput = [r['throughput'] for r in results]
    p50 = [r['p50'] for r in results]
    p99 = [r['p99'] for r in results]
    p999 = [r['p999'] for r in results]

    x = np.arange(len(clients))
    color = '#1f77b4'

    # Create one figure with two subplots: Throughput on top, Latency on bottom
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12), sharex=True)
    fig.suptitle('KurrentDB WRFLGRPC', fontsize=16)

    # --- Throughput Plot (Top) ---
    ax1.bar(x, throughput, color=color, alpha=0.9)
    ax1.set_ylim(bottom=0)
    ax1.set_ylabel('Throughput (reqs/sec)')
    # ax1.set_title('Throughput vs Number of Clients')
    ax1.grid(True, axis='y', ls=':', alpha=0.6)

    # --- Latency Plot (Bottom) ---
    print(f"Generating latency subplot for clients={clients}")
    print(f"  p50:   {p50}")
    print(f"  p99:   {p99}")
    print(f"  p99.9: {p999}")
    
    # Linear plotting with zero-based y-axis (OVERLAYING)
    ax2.bar(x, p999, label='p99.9', color=color, alpha=0.3)
    ax2.bar(x, p99, label='p99', color=color, alpha=0.6)
    ax2.bar(x, p50, label='p50', color=color, alpha=1.0)

    ax2.set_ylim(bottom=0)
    ax2.set_xlabel('Number of Clients')
    ax2.set_ylabel('Latency (ms)')
    # ax2.set_title('Latency vs Number of Clients')
    ax2.set_xticks(x)
    ax2.set_xticklabels(clients)
    
    ax2.legend(loc='upper center', bbox_to_anchor=(0.5, -0.12), ncol=3, frameon=False)
    ax2.grid(True, axis='y', ls=':', alpha=0.6)

    plt.tight_layout()
    output_path = os.path.join(output_dir, 'kurrentdb_performance_scaling.png')
    plt.savefig(output_path, dpi=150)
    plt.close()

    print(f"Combined performance plot saved to: {output_path}")

if __name__ == "__main__":
    import sys
    log_files = sys.argv[1:]
    if not log_files:
        print("Usage: python kurrentdb_testclient_plots.py <log_file1> <log_file2> ...")
        sys.exit(1)

    results = []
    for log_file in log_files:
        res = parse_log_file(log_file)
        if res:
            results.append(res)
    
    output_dir = "plots"
    os.makedirs(output_dir, exist_ok=True)
    generate_plots(results, output_dir)
    print(f"Plots generated in {output_dir}/")
