import time
import argparse
import statistics
from uuid import uuid4

from kurrentdbclient import KurrentDBClient, NewEvent, StreamState

def run_benchmark(connection_string, stream_name, num_events, payload_size):
    client = KurrentDBClient(connection_string)
    
    print(f"Starting benchmark: {num_events} events, {payload_size} bytes each")
    print(f"Target stream: {stream_name}")
    print(f"Connection string: {connection_string}")
    
    payload = b"x" * payload_size
    latencies = []
    
    start_time = time.perf_counter()
    
    try:
        for i in range(num_events):
            event = NewEvent(
                type="BenchmarkEvent",
                data=payload,
            )
            
            step_start = time.perf_counter()
            client.append_to_stream(
                stream_name=stream_name + str(uuid4()) + str(uuid4()),
                events=[event],
                current_version=StreamState.ANY,
            )
            step_end = time.perf_counter()
            
            latencies.append((step_end - step_start) * 1000) # in ms
            
            if (i + 1) % 100 == 0:
                print(f"Appended {i + 1}/{num_events} events...")
                
    except Exception as e:
        print(f"Error during benchmark: {e}")
        return

    end_time = time.perf_counter()
    total_time = end_time - start_time
    
    throughput = num_events / total_time
    
    print("\nBenchmark Results:")
    print(f"Total events: {num_events}")
    print(f"Total time: {total_time:.2f} s")
    print(f"Throughput: {throughput:.2f} events/s")
    
    if latencies:
        print(f"Latency (ms):")
        print(f"  Mean:   {statistics.mean(latencies):.3f}")
        print(f"  Median: {statistics.median(latencies):.3f}")
        print(f"  Min:    {min(latencies):.3f}")
        print(f"  Max:    {max(latencies):.3f}")
        if len(latencies) > 1:
            quantiles = statistics.quantiles(latencies, n=100)
            print(f"  P95:    {quantiles[94]:.3f}")
            print(f"  P99:    {quantiles[98]:.3f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="KurrentDB Python Client Benchmark")
    parser.add_argument("--conn", default="kurrentdb://127.0.0.1:2113?tls=false", help="KurrentDB connection string")
    parser.add_argument("--stream", default="benchmark-stream", help="Stream name to append to")
    parser.add_argument("--events", type=int, default=1000, help="Number of events to append")
    parser.add_argument("--size", type=int, default=1024, help="Size of each event payload in bytes")
    
    args = parser.parse_args()
    
    run_benchmark(args.conn, args.stream, args.events, args.size)
