#!/usr/bin/env python
"""Demo script to visualize TDigest quantiles for different distributions."""

import numpy as np
import apache_beam as beam
from apache_beam.metrics import Metrics


class RecordDistribution(beam.DoFn):
    """DoFn that records each element to a distribution metric."""

    def __init__(self, dist_name):
        self.dist_name = dist_name
        self.distribution = None

    def setup(self):
        self.distribution = Metrics.distribution('demo', self.dist_name)

    def process(self, element):
        self.distribution.update(int(element))
        yield element


def generate_normal(n=10000, mean=500, std=100):
    """Generate normally distributed data."""
    return np.random.normal(mean, std, n).clip(0, 1000)


def generate_bimodal(n=10000):
    """Generate bimodal distribution (two peaks)."""
    half = n // 2
    peak1 = np.random.normal(200, 50, half)
    peak2 = np.random.normal(800, 50, n - half)
    return np.concatenate([peak1, peak2]).clip(0, 1000)


def generate_longtail(n=10000):
    """Generate long-tail (exponential) distribution."""
    return np.random.exponential(100, n).clip(0, 1000)


def run_pipeline(data, dist_name):
    """Run a Beam pipeline and return the distribution result."""
    # Use BundleBasedDirectRunner to avoid portable runner which loses tdigest
    with beam.Pipeline(runner='BundleBasedDirectRunner') as p:
        _ = (
            p
            | f'Create_{dist_name}' >> beam.Create(data.tolist())
            | f'Record_{dist_name}' >> beam.ParDo(RecordDistribution(dist_name))
        )
        result = p.run()
        result.wait_until_finish()

        # Get metrics
        metrics = result.metrics().query()
        for dist in metrics['distributions']:
            if dist.key.metric.name == dist_name:
                committed = dist.committed
                # Debug: check if tdigest is present
                if committed:
                    print(f"    {dist_name}: count={committed.count}, "
                          f"tdigest={'present' if committed.data.tdigest else 'MISSING'}")
                return committed

    return None


def plot_quantiles(results):
    """Plot quantiles for all distributions."""
    quantiles = np.arange(0, 101, 1) / 100.0

    print("\n" + "=" * 80)
    print("TDIGEST QUANTILE COMPARISON")
    print("=" * 80)

    # Print header
    print(f"\n{'Percentile':<12}", end="")
    for name in results:
        print(f"{name:<15}", end="")
    print()
    print("-" * (12 + 15 * len(results)))

    # Print key percentiles
    key_percentiles = [0, 10, 25, 50, 75, 90, 95, 99, 100]
    for pct in key_percentiles:
        q = pct / 100.0
        print(f"p{pct:<10}", end="")
        for name, result in results.items():
            if result and result.data.tdigest:
                val = result.quantile(q)
                print(f"{val:>14.1f}", end=" ")
            else:
                print(f"{'N/A':>14}", end=" ")
        print()

    print("\n" + "=" * 80)
    print("ASCII VISUALIZATION (p0 to p100)")
    print("=" * 80)

    for name, result in results.items():
        if not result or not result.data.tdigest:
            print(f"\n{name}: No TDigest data available")
            continue

        print(f"\n{name}:")
        print(f"  Count: {result.count}, Sum: {result.sum}, "
              f"Min: {result.min}, Max: {result.max}, Mean: {result.mean:.1f}")

        # Get all quantile values
        q_values = [result.quantile(q) for q in quantiles]
        min_val, max_val = min(q_values), max(q_values)
        range_val = max_val - min_val if max_val > min_val else 1

        # ASCII plot
        width = 60
        print(f"\n  {'p0':<5}{min_val:>8.1f} |", end="")
        print("-" * width, end="")
        print(f"| {max_val:<8.1f} p100")

        # Plot key percentiles as markers
        markers = {10: 'p10', 25: 'p25', 50: 'p50', 75: 'p75', 90: 'p90', 99: 'p99'}
        for pct, label in markers.items():
            q = pct / 100.0
            val = result.quantile(q)
            pos = int((val - min_val) / range_val * width)
            pos = max(0, min(width - 1, pos))
            print(f"  {label:<5}{val:>8.1f} |", end="")
            print(" " * pos + "*" + " " * (width - pos - 1), end="")
            print("|")

    # Detailed quantile table
    print("\n" + "=" * 80)
    print("FULL QUANTILE TABLE (every 5%)")
    print("=" * 80)

    print(f"\n{'%':<6}", end="")
    for name in results:
        print(f"{name:<15}", end="")
    print()
    print("-" * (6 + 15 * len(results)))

    for pct in range(0, 101, 5):
        q = pct / 100.0
        print(f"{pct:<6}", end="")
        for name, result in results.items():
            if result and result.data.tdigest:
                val = result.quantile(q)
                print(f"{val:>14.1f}", end=" ")
            else:
                print(f"{'N/A':>14}", end=" ")
        print()


def main():
    np.random.seed(42)

    print("Generating data...")
    data = {
        'normal': generate_normal(),
        'bimodal': generate_bimodal(),
        'longtail': generate_longtail(),
    }

    print("Running pipelines...")
    results = {}
    for name, values in data.items():
        print(f"  Processing {name}...")
        results[name] = run_pipeline(values, name)

    plot_quantiles(results)

    # Also print actual data statistics for comparison
    print("\n" + "=" * 80)
    print("ACTUAL DATA STATISTICS (numpy)")
    print("=" * 80)
    for name, values in data.items():
        print(f"\n{name}:")
        print(f"  Count: {len(values)}, Mean: {np.mean(values):.1f}, "
              f"Std: {np.std(values):.1f}")
        print(f"  Min: {np.min(values):.1f}, Max: {np.max(values):.1f}")
        print(f"  Percentiles: p25={np.percentile(values, 25):.1f}, "
              f"p50={np.percentile(values, 50):.1f}, "
              f"p75={np.percentile(values, 75):.1f}, "
              f"p95={np.percentile(values, 95):.1f}, "
              f"p99={np.percentile(values, 99):.1f}")


if __name__ == '__main__':
    main()
