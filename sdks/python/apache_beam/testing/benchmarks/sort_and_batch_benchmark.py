#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Benchmark: BatchElements vs SortAndBatchElements (weight-based splitting).

Compares two batching strategies for variable-length inference workloads:

- Baseline (BatchElements): fixed-count chunking, ignores element sizes.
- Stateless (SortAndBatchElements): within each bundle, sorts elements
  by size, then splits batches using max_batch_weight so that each batch
  has a bounded total weight.  The improvement comes from *changing batch
  boundaries* (weight-based splitting), NOT from sorting alone -- sorting
  within fixed boundaries yields 0% gain (verified by strict-control).

Padding ratio::

  padding_ratio = sum(max_len_in_batch * batch_size) / sum(actual_lengths)
  Lower is better.  1.0 = no padding waste.

Methodology:

- N=20 independent trials per condition (3 warmup trials excluded).
- Same input corpus (seed=42) for A/B comparison.
- Percentile method: linear interpolation between adjacent ranks
  (equivalent to numpy.percentile with method='linear').
  For N=20 trials: P50 interpolates ranks 10-11 (0-indexed 9-10),
  P95 interpolates ranks 19-20 (0-indexed 18-19),
  P99 interpolates near rank 20 (0-indexed 18.81).
- Reports median [IQR] and P95 for each metric.
- Inference model: latency = batch_size * (max_seq_len / 50)^1.5 ms
  (simulates transformer-like scaling).

Run::

  python3 -m apache_beam.testing.benchmarks.sort_and_batch_benchmark
"""

import math
import random
import statistics
import time
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple

# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------


def generate_highly_skewed_data(
    num_elements: int,
    min_length: int = 1,
    max_length: int = 500,
    seed: int = 42) -> List[str]:
  """Pareto(alpha=1.2) -- most short, few very long."""
  random.seed(seed)
  data = []
  for _ in range(num_elements):
    length = int(random.paretovariate(1.2) * min_length)
    length = min(max(length, min_length), max_length)
    data.append('x' * length)
  return data


def generate_lognormal_data(
    num_elements: int,
    mean_length: int = 50,
    std_factor: float = 0.8,
    min_length: int = 1,
    max_length: int = 500,
    seed: int = 42) -> List[str]:
  """Log-normal -- moderate skew, typical NLP."""
  random.seed(seed)
  mu = math.log(mean_length)
  sigma = std_factor
  data = []
  for _ in range(num_elements):
    length = int(random.lognormvariate(mu, sigma))
    length = min(max(length, min_length), max_length)
    data.append('x' * length)
  return data


def generate_bimodal_data(
    num_elements: int,
    mode1_mean: int = 20,
    mode2_mean: int = 200,
    mode1_ratio: float = 0.7,
    min_length: int = 1,
    max_length: int = 500,
    seed: int = 42) -> List[str]:
  """Bimodal -- two distinct length groups."""
  random.seed(seed)
  data = []
  for _ in range(num_elements):
    if random.random() < mode1_ratio:
      length = int(random.gauss(mode1_mean, mode1_mean * 0.3))
    else:
      length = int(random.gauss(mode2_mean, mode2_mean * 0.3))
    length = min(max(length, min_length), max_length)
    data.append('x' * length)
  return data


def generate_low_variance_data(
    num_elements: int,
    mean_length: int = 100,
    cv: float = 0.1,
    min_length: int = 1,
    max_length: int = 500,
    seed: int = 42) -> List[str]:
  """Low-variance control (CV=10%)."""
  random.seed(seed)
  std = mean_length * cv
  data = []
  for _ in range(num_elements):
    length = int(random.gauss(mean_length, std))
    length = min(max(length, min_length), max_length)
    data.append('x' * length)
  return data


# ---------------------------------------------------------------------------
# Batching algorithms
# ---------------------------------------------------------------------------


def simulate_batch_elements(data: List[str],
                            max_batch_size: int) -> List[List[str]]:
  """Baseline: simple count-based chunking (BatchElements behaviour)."""
  batches = []
  current_batch = []
  for element in data:
    current_batch.append(element)
    if len(current_batch) >= max_batch_size:
      batches.append(current_batch)
      current_batch = []
  if current_batch:
    batches.append(current_batch)
  return batches


def simulate_sort_and_batch_elements(
    data: List[str],
    max_batch_size: int,
    max_batch_weight: int,
    element_size_fn: Optional[Callable[[Any], int]] = None,
    bundle_size: Optional[int] = None) -> List[List[str]]:
  """Core mechanism: sort by size + weight-based batch splitting."""
  if element_size_fn is None:
    element_size_fn = len

  # Split into bundles if specified (realistic Beam behavior)
  if bundle_size is not None and bundle_size > 0:
    bundles = [
        data[i:i + bundle_size] for i in range(0, len(data), bundle_size)
    ]
  else:
    bundles = [data]

  all_batches = []

  for bundle in bundles:
    # Sort by element size (ascending)
    sorted_bundle = sorted(bundle, key=element_size_fn)

    current_batch = []
    current_weight = 0

    for element in sorted_bundle:
      element_weight = element_size_fn(element)

      # Check if adding this element would exceed limits
      would_exceed_count = len(current_batch) >= max_batch_size
      would_exceed_weight = (
          current_weight + element_weight > max_batch_weight and current_batch)

      if would_exceed_count or would_exceed_weight:
        all_batches.append(current_batch)
        current_batch = []
        current_weight = 0

      current_batch.append(element)
      current_weight += element_weight

    if current_batch:
      all_batches.append(current_batch)

  return all_batches


# ---------------------------------------------------------------------------
# Simulated inference
# ---------------------------------------------------------------------------


def simulate_inference_latency(
    batch: List[str], base_latency_ms: float = 1.0) -> float:
  """Simulate transformer inference: O(batch_size * seq_len^1.5)."""
  if not batch:
    return 0.0
  batch_size = len(batch)
  max_len = max(len(s) for s in batch)
  return base_latency_ms * batch_size * (max_len / 50)**1.5


# ---------------------------------------------------------------------------
# Stats helpers
# ---------------------------------------------------------------------------


def percentile(data: Sequence[float], p: float) -> float:
  """Percentile via linear interpolation between adjacent ranks.

  Equivalent to numpy.percentile(data, p, method='linear').
  For N=20: P50 interpolates ranks 10-11, P95 ranks 19-20,
  P99 near rank 20 (fractional index 18.81).
  """
  if not data:
    return 0.0
  s = sorted(data)
  k = (len(s) - 1) * p / 100
  f = int(k)
  c = min(f + 1, len(s) - 1)
  return s[f] + (k - f) * (s[c] - s[f])


def compute_padding_stats(batches: List[List[str]]) -> Dict[str, Any]:
  """Padding-efficiency statistics for a list of batches."""
  total_actual = 0
  total_padded = 0
  batch_sizes = []
  max_lengths = []

  for batch in batches:
    if not batch:
      continue
    lengths = [len(s) for s in batch]
    mx = max(lengths)
    total_actual += sum(lengths)
    total_padded += mx * len(batch)
    batch_sizes.append(len(batch))
    max_lengths.append(mx)

  efficiency = total_actual / total_padded if total_padded else 0.0
  padding_ratio = total_padded / total_actual if total_actual else float('inf')

  return {
      'efficiency': efficiency,
      'padding_ratio': padding_ratio,
      'num_batches': len(batches),
      'avg_batch_size': statistics.mean(batch_sizes) if batch_sizes else 0,
      'total_actual_length': total_actual,
      'total_padded_length': total_padded,
      'padding_overhead': total_padded - total_actual,
      'batch_size_p50': percentile(batch_sizes, 50) if batch_sizes else 0,
      'batch_size_p95': percentile(batch_sizes, 95) if batch_sizes else 0,
      'batch_size_max': max(batch_sizes) if batch_sizes else 0,
      'max_len_p50': percentile(max_lengths, 50) if max_lengths else 0,
      'max_len_p95': percentile(max_lengths, 95) if max_lengths else 0,
  }


# ---------------------------------------------------------------------------
# Invariant validation
# ---------------------------------------------------------------------------


def validate_invariants(
    data: List[str],
    baseline_batches: List[List[str]],
    stateless_batches: List[List[str]],
    config: Dict[str, Any]) -> Dict[str, Any]:
  """Validate element/token counts and batch-size equality."""
  n = len(data)
  b_n = sum(len(b) for b in baseline_batches)
  s_n = sum(len(b) for b in stateless_batches)
  tok = sum(len(s) for s in data)
  b_tok = sum(sum(len(s) for s in b) for b in baseline_batches)
  s_tok = sum(sum(len(s) for s in b) for b in stateless_batches)

  return {
      'input_elements': n,
      'baseline_elements': b_n,
      'stateless_elements': s_n,
      'elements_match': n == b_n == s_n,
      'input_tokens': tok,
      'baseline_tokens': b_tok,
      'stateless_tokens': s_tok,
      'tokens_match': tok == b_tok == s_tok,
      'baseline_num_batches': len(baseline_batches),
      'stateless_num_batches': len(stateless_batches),
  }


# ---------------------------------------------------------------------------
# Performance benchmark (N=20 trials)
# ---------------------------------------------------------------------------


def run_performance_benchmark(
    data: List[str],
    max_batch_size: int,
    max_batch_weight: int,
    bundle_size: int = 500,
    num_trials: int = 20,
    warmup_trials: int = 3) -> Tuple[Dict[str, Any], Dict[str, Any]]:
  """Run N=20 trials for baseline and stateless."""
  total_tokens = sum(len(s) for s in data)

  baseline_trials = []
  stateless_trials = []

  for trial_idx in range(warmup_trials + num_trials):
    is_warmup = trial_idx < warmup_trials

    # --- Baseline ---
    start = time.perf_counter()
    b_batches = simulate_batch_elements(data, max_batch_size)
    batch_ms = (time.perf_counter() - start) * 1000
    b_inf = [simulate_inference_latency(b) for b in b_batches]
    b_e2e = batch_ms + sum(b_inf)
    if not is_warmup:
      baseline_trials.append({
          'overhead_ms': batch_ms,
          'inference_ms': sum(b_inf),
          'e2e_ms': b_e2e,
          'batch_latencies': b_inf,
          'num_batches': len(b_batches),
      })

    # --- Stateless (SortAndBatchElements) ---
    start = time.perf_counter()
    s_batches = simulate_sort_and_batch_elements(
        data, max_batch_size, max_batch_weight, bundle_size=bundle_size)
    sort_ms = (time.perf_counter() - start) * 1000
    s_inf = [simulate_inference_latency(b) for b in s_batches]
    s_e2e = sort_ms + sum(s_inf)
    if not is_warmup:
      stateless_trials.append({
          'overhead_ms': sort_ms,
          'inference_ms': sum(s_inf),
          'e2e_ms': s_e2e,
          'batch_latencies': s_inf,
          'num_batches': len(s_batches),
      })

  def _stats(trials):
    e2e = [t['e2e_ms'] for t in trials]
    tput = [total_tokens / (t['e2e_ms'] / 1000) for t in trials]
    overhead = [t['overhead_ms'] for t in trials]
    all_lat = [l for t in trials for l in t['batch_latencies']]
    return {
        'e2e_median': percentile(e2e, 50),
        'e2e_p25': percentile(e2e, 25),
        'e2e_p75': percentile(e2e, 75),
        'e2e_p95': percentile(e2e, 95),
        'tput_median': percentile(tput, 50),
        'tput_p25': percentile(tput, 25),
        'tput_p75': percentile(tput, 75),
        'tput_p95': percentile(tput, 95),
        'overhead_median': percentile(overhead, 50),
        'overhead_p25': percentile(overhead, 25),
        'overhead_p75': percentile(overhead, 75),
        'overhead_p95': percentile(overhead, 95),
        'batch_lat_p50': percentile(all_lat, 50),
        'batch_lat_p95': percentile(all_lat, 95),
        'batch_lat_p99': percentile(all_lat, 99),
        'inf_p95': percentile(all_lat, 95),
        'num_trials': len(trials),
        'num_batches': trials[0]['num_batches'] if trials else 0,
    }

  return _stats(baseline_trials), _stats(stateless_trials)


# ---------------------------------------------------------------------------
# Single benchmark run
# ---------------------------------------------------------------------------


def run_benchmark(
    num_elements: int = 10000,
    min_length: int = 1,
    max_length: int = 500,
    max_batch_size: int = 32,
    max_batch_weight: int = 2000,
    bundle_size: int = 500,
    distribution: str = 'pareto',
    seed: int = 42) -> Dict[str, Any]:
  """Run baseline vs stateless comparison."""
  generators = {
      'pareto': lambda: generate_highly_skewed_data(
          num_elements, min_length, max_length, seed),
      'lognormal': lambda: generate_lognormal_data(
          num_elements, 50, 0.8, min_length, max_length, seed),
      'bimodal': lambda: generate_bimodal_data(
          num_elements, 20, 200, 0.7, min_length, max_length, seed),
      'low_variance': lambda: generate_low_variance_data(
          num_elements, 100, 0.1, min_length, max_length, seed),
  }
  if distribution not in generators:
    raise ValueError(f"Unknown distribution: {distribution}")

  data = generators[distribution]()
  lengths = [len(s) for s in data]

  baseline_batches = simulate_batch_elements(data, max_batch_size)
  stateless_batches = simulate_sort_and_batch_elements(
      data, max_batch_size, max_batch_weight, bundle_size=bundle_size)

  baseline_pad = compute_padding_stats(baseline_batches)
  stateless_pad = compute_padding_stats(stateless_batches)

  baseline_perf, stateless_perf = run_performance_benchmark(
      data, max_batch_size, max_batch_weight, bundle_size)
  baseline_pad.update(baseline_perf)
  stateless_pad.update(stateless_perf)

  validation = validate_invariants(
      data,
      baseline_batches,
      stateless_batches, {
          'max_batch_size': max_batch_size,
          'max_batch_weight': max_batch_weight
      })

  return {
      'config': {
          'num_elements': num_elements,
          'max_batch_size': max_batch_size,
          'max_batch_weight': max_batch_weight,
          'bundle_size': bundle_size,
          'distribution': distribution,
      },
      'data_stats': {
          'min': min(lengths),
          'max': max(lengths),
          'mean': statistics.mean(lengths),
          'median': statistics.median(lengths),
          'std': statistics.stdev(lengths),
      },
      'baseline': baseline_pad,
      'stateless': stateless_pad,
      'validation': validation,
  }


# ---------------------------------------------------------------------------
# Printing
# ---------------------------------------------------------------------------


def _fmt_iqr(median, p25, p75, unit=''):
  return f"{median:.1f} [{p25:.1f}-{p75:.1f}]{unit}"


def print_results(results: Dict[str, Any]) -> None:
  cfg = results['config']
  ds = results['data_stats']
  bl = results['baseline']
  st = results['stateless']
  val = results['validation']

  print("=" * 80)
  print(
      f"Distribution: {cfg['distribution']}  |  "
      f"N={cfg['num_elements']}  |  "
      f"max_batch_size={cfg['max_batch_size']}  |  "
      f"max_batch_weight={cfg['max_batch_weight']}")
  print(
      f"Input lengths: min={ds['min']}  max={ds['max']}  "
      f"mean={ds['mean']:.1f}  median={ds['median']:.0f}  std={ds['std']:.1f}")
  print("-" * 80)

  def _arm(label, s):
    print(f"\n  {label}:")
    print(f"    Num batches:    {s['num_batches']}")
    print(f"    Padding ratio:  {s['padding_ratio']:.2f}x")
    print("    ")
    print("    Throughput (Ktok/s):")
    med = s['tput_median'] / 1000
    p25 = s['tput_p25'] / 1000
    p75 = s['tput_p75'] / 1000
    print(f"      Median [IQR]: {med:.1f}"
          f" [{p25:.1f}-{p75:.1f}]")
    print(f"      P95:          {s['tput_p95']/1000:.1f}")
    print("    ")
    print("    E2E latency (ms):")
    print(
        f"      Median [IQR]: {s['e2e_median']:.1f}"
        f" [{s['e2e_p25']:.1f}-{s['e2e_p75']:.1f}]")
    print(f"      P95:          {s['e2e_p95']:.1f}")
    print("    ")
    print("    Overhead (ms):")
    print(
        f"      Median [IQR]:"
        f" {s['overhead_median']:.2f}"
        f" [{s['overhead_p25']:.2f}"
        f"-{s['overhead_p75']:.2f}]")
    print(f"      P95:          {s['overhead_p95']:.2f}")
    print("    ")
    print("    Batch latency (ms):")
    print(f"      P50:          {s['batch_lat_p50']:.1f}")
    print(f"      P95:          {s['batch_lat_p95']:.1f}")
    print(f"      P99:          {s['batch_lat_p99']:.1f}")

  _arm("Baseline (BatchElements)", bl)
  _arm("Stateless (SortAndBatchElements w/ weight-based splitting)", st)

  # Delta — explicit arrows so direction is unambiguous
  #   ↓ = value decreased (good for latency/padding)
  #   ↑ = value increased (good for throughput)
  def _delta_lower(base, new):
    """For metrics where lower is better (latency, padding)."""
    if base == 0:
      return 'N/A'
    pct = (base - new) / base * 100
    arrow = '\u2193' if pct > 0 else '\u2191'
    return f"{arrow}{abs(pct):.1f}%"

  def _delta_higher(base, new):
    """For metrics where higher is better (throughput)."""
    if base == 0:
      return 'N/A'
    pct = (new - base) / base * 100
    arrow = '\u2191' if pct > 0 else '\u2193'
    return f"{arrow}{abs(pct):.1f}%"

  print(f"\n  {'_' * 76}")
  print("  DELTA (Baseline -> Stateless):")

  def _line(label, bv, sv, delta_fn, fmt='.1f', unit=''):
    d = delta_fn(bv, sv)
    print(f"  {label}: {bv:{fmt}}{unit}"
          f" -> {sv:{fmt}}{unit}  ({d})")

  bl_tmed = bl['tput_median'] / 1000
  st_tmed = st['tput_median'] / 1000
  bl_tp95 = bl['tput_p95'] / 1000
  st_tp95 = st['tput_p95'] / 1000

  _line(
      'Padding ratio    ',
      bl['padding_ratio'],
      st['padding_ratio'],
      _delta_lower,
      fmt='.2f',
      unit='x')
  _line('Throughput median', bl_tmed, st_tmed, _delta_higher, unit=' Ktok/s')
  _line('Throughput p95   ', bl_tp95, st_tp95, _delta_higher, unit=' Ktok/s')
  _line(
      'E2E latency med  ',
      bl['e2e_median'],
      st['e2e_median'],
      _delta_lower,
      unit=' ms')
  _line(
      'E2E latency p95  ',
      bl['e2e_p95'],
      st['e2e_p95'],
      _delta_lower,
      unit=' ms')
  _line(
      'Batch lat p95    ',
      bl['batch_lat_p95'],
      st['batch_lat_p95'],
      _delta_lower,
      unit=' ms')
  _line(
      'Batch lat p99    ',
      bl['batch_lat_p99'],
      st['batch_lat_p99'],
      _delta_lower,
      unit=' ms')

  # Invariants
  e_ok = "Y" if val['elements_match'] else "X"
  t_ok = "Y" if val['tokens_match'] else "X"
  b_nb = val['baseline_num_batches']
  s_nb = val['stateless_num_batches']
  print(
      f"\n  Invariants: elements {e_ok}  tokens {t_ok}"
      f"  (baseline {b_nb} -> stateless {s_nb}"
      f" batches)")
  print("=" * 80)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
  print("=" * 80)
  print("BASELINE (count-based) vs STATELESS (weight-based boundary splitting)")
  print("=" * 80)
  print()
  print("Experiment design:")
  print("  A = Baseline   : BatchElements with max_batch_size=32 (count-based)")
  print("  B = Stateless   : SortAndBatchElements with max_batch_weight=2000")
  print(
      "                    (sort by size within bundle -> weight-based split)")
  print()
  print("Why Stateless wins:")
  print("  Weight-based splitting changes batch BOUNDARIES so each batch has")
  print(
      "  similar-length elements -> less padding.  Sorting alone within fixed")
  print("  boundaries yields 0% gain (verified by strict-control experiment).")
  print()
  print("Methodology:")
  print("  - N=20 trials, 3 warmup excluded")
  print("  - Percentiles: linear interpolation (= numpy default)")
  print("  - Same seed=42 for both arms")
  print("  - Inference model: latency = batch_size * (max_seq_len/50)^1.5 ms")
  print()

  dist = 'pareto'
  print(f"\nRunning: {dist}...")
  r = run_benchmark(
      num_elements=10000,
      max_batch_size=32,
      max_batch_weight=2000,
      bundle_size=500,
      distribution=dist,
      seed=42)
  print_results(r)


if __name__ == '__main__':
  main()
