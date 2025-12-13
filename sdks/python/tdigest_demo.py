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
    print("PDF VISUALIZATION (vertical, density estimate from TDigest)")
    print("=" * 80)

    # Create vertical PDF visualization for all distributions side by side
    num_bins = 20
    height = 25  # Chart height in lines

    for name, result in results.items():
        if not result or not result.data.tdigest:
            print(f"\n{name}: No TDigest data available")
            continue

        print(f"\n{name}:")
        print(f"  Count: {result.count}, Min: {result.min}, Max: {result.max}, "
              f"Mean: {result.mean:.1f}")

        # Estimate PDF by computing density at each bin
        # Density is proportional to 1 / (dValue/dQuantile)
        min_val = float(result.min)
        max_val = float(result.max)
        bin_width = (max_val - min_val) / num_bins

        densities = []
        bin_centers = []
        for i in range(num_bins):
            bin_start = min_val + i * bin_width
            bin_end = bin_start + bin_width
            bin_center = (bin_start + bin_end) / 2
            bin_centers.append(bin_center)

            # Use TDigest's cdf to estimate density
            # Density = (cdf(bin_end) - cdf(bin_start)) / bin_width
            cdf_start = result.data.tdigest.cdf(bin_start)
            cdf_end = result.data.tdigest.cdf(bin_end)
            density = (cdf_end - cdf_start) / bin_width if bin_width > 0 else 0
            densities.append(density)

        # Normalize densities for display
        max_density = max(densities) if densities else 1
        normalized = [d / max_density for d in densities]

        # Print vertical histogram (rotated 90 degrees)
        chart_width = 50
        print()
        for level in range(height, 0, -1):
            threshold = level / height
            line = "  "
            for norm_d in normalized:
                if norm_d >= threshold:
                    line += "██"
                else:
                    line += "  "
            # Add density scale on right side at a few levels
            if level == height:
                line += f"  {max_density:.4f}"
            elif level == height // 2:
                line += f"  {max_density/2:.4f}"
            elif level == 1:
                line += "  0"
            print(line)

        # Print x-axis
        print("  " + "──" * num_bins)
        # Print x-axis labels
        print(f"  {min_val:<{num_bins}}{max_val:>{num_bins}}")
        print(f"  {'Value range':-^{num_bins * 2}}")

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
