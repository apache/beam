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

"""Benchmark: BatchElements vs SortAndBatchElements on real Beam pipelines.

Compares two batching strategies for variable-length inference workloads by
running the actual Beam transforms under DirectRunner:

- Baseline (BatchElements): fixed-count batching by setting
  ``min_batch_size == max_batch_size``.
- Stateless (SortAndBatchElements): sorts elements by size within each runner
  bundle, then splits batches using ``max_batch_weight``.

The benchmark materializes per-batch summaries through a temporary Beam sink and
analyzes them after the pipeline completes. This keeps the benchmark on the
normal Beam execution path rather than relying on InteractiveRunner-specific
result materialization or local side effects.

Bundle boundaries are runner-defined. As a result, these measurements are meant
to compare the actual DirectRunner behavior of the two transforms rather than a
synthetic, user-configurable bundle model.

Padding ratio::

  padding_ratio = sum(max_len_in_batch * batch_size) / sum(actual_lengths)
  Lower is better.  1.0 = no padding waste.

Methodology:

- N=20 independent trials per condition (3 warmup trials excluded).
- Same input corpus (seed=42) for A/B comparison.
- DirectRunner with in-memory execution and one worker for reproducibility.
- Percentile method: linear interpolation between adjacent ranks
  (equivalent to numpy.percentile with method='linear').
  For N=20 trials: P50 interpolates ranks 10-11 (0-indexed 9-10),
  P95 interpolates ranks 19-20 (0-indexed 18-19),
  P99 interpolates near rank 20 (0-indexed 18.81).
- Reports median [IQR] and P95 for each metric.
- Inference model: latency = batch_size * (max_seq_len / 50)^1.5 ms
  (simulates downstream transformer-like scaling).

Run::

  python3 -m apache_beam.testing.benchmarks.sort_and_batch_benchmark
"""

import glob
import json
import math
import os
import random
import statistics
import tempfile
import time
from collections.abc import Sequence
from typing import Any

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms import util

# ---------------------------------------------------------------------------
# Data generators
# ---------------------------------------------------------------------------


def generate_highly_skewed_data(
    num_elements: int,
    min_length: int = 1,
    max_length: int = 500,
    seed: int = 42) -> list[str]:
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
    seed: int = 42) -> list[str]:
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
    seed: int = 42) -> list[str]:
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
    seed: int = 42) -> list[str]:
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
# Real Beam batching
# ---------------------------------------------------------------------------


def _direct_runner_options() -> PipelineOptions:
  return PipelineOptions([
      '--runner=DirectRunner',
      '--direct_running_mode=in_memory',
      '--direct_num_workers=1',
  ])


def _batch_to_json(batch: list[str]) -> str:
  lengths = [len(element) for element in batch]
  return json.dumps({
      'batch_size': len(batch),
      'actual_total_length': sum(lengths),
      'max_len': max(lengths) if lengths else 0,
  })


def _read_batch_summaries(output_prefix: str) -> list[dict[str, int]]:
  summaries = []
  for path in sorted(glob.glob(f'{output_prefix}*')):
    if path.endswith('.crc'):
      continue
    with open(path, encoding='utf-8') as handle:
      for line in handle:
        line = line.strip()
        if line:
          summaries.append(json.loads(line))
  return summaries


def _run_batching_pipeline(
    strategy: str, data: list[str], max_batch_size: int,
    max_batch_weight: int) -> tuple[list[dict[str, int]], float]:
  """Runs one Beam pipeline and returns batch summaries plus runtime."""
  with tempfile.TemporaryDirectory(prefix='beam_batch_benchmark_') as temp_dir:
    output_prefix = os.path.join(temp_dir, strategy)
    pipeline = beam.Pipeline(options=_direct_runner_options())
    batched = pipeline | 'CreateInput' >> beam.Create(data, reshuffle=False)

    if strategy == 'baseline':
      batched = batched | 'BatchElements' >> util.BatchElements(
          min_batch_size=max_batch_size, max_batch_size=max_batch_size)
    elif strategy == 'stateless':
      batched = batched | 'SortAndBatchElements' >> util.SortAndBatchElements(
          min_batch_size=1,
          max_batch_size=max_batch_size,
          max_batch_weight=max_batch_weight)
    else:
      raise ValueError(f'Unknown strategy: {strategy}')

    _ = (
        batched
        | 'SerializeBatchSummary' >> beam.Map(_batch_to_json)
        | 'WriteBatchSummary' >> beam.io.WriteToText(output_prefix))

    start = time.perf_counter()
    result = pipeline.run()
    result.wait_until_finish()
    runtime_ms = (time.perf_counter() - start) * 1000

    return _read_batch_summaries(output_prefix), runtime_ms


# ---------------------------------------------------------------------------
# Simulated inference
# ---------------------------------------------------------------------------


def simulate_inference_latency(
    batch_size: int, max_len: int, base_latency_ms: float = 1.0) -> float:
  """Simulate downstream inference: O(batch_size * seq_len^1.5)."""
  if not batch_size or not max_len:
    return 0.0
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


def compute_padding_stats(
    batch_summaries: list[dict[str, int]]) -> dict[str, Any]:
  """Padding-efficiency statistics for materialized batch summaries."""
  total_actual = sum(s['actual_total_length'] for s in batch_summaries)
  total_padded = sum(s['max_len'] * s['batch_size'] for s in batch_summaries)
  batch_sizes = [s['batch_size'] for s in batch_summaries if s['batch_size']]
  max_lengths = [s['max_len'] for s in batch_summaries if s['batch_size']]

  efficiency = total_actual / total_padded if total_padded else 0.0
  padding_ratio = total_padded / total_actual if total_actual else float('inf')

  return {
      'efficiency': efficiency,
      'padding_ratio': padding_ratio,
      'num_batches': len(batch_summaries),
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
    data: list[str],
    baseline_summaries: list[dict[str, int]],
    stateless_summaries: list[dict[str, int]]) -> dict[str, Any]:
  """Validate element/token counts and batch-size equality."""
  n = len(data)
  b_n = sum(s['batch_size'] for s in baseline_summaries)
  s_n = sum(s['batch_size'] for s in stateless_summaries)
  tok = sum(len(s) for s in data)
  b_tok = sum(s['actual_total_length'] for s in baseline_summaries)
  s_tok = sum(s['actual_total_length'] for s in stateless_summaries)

  return {
      'input_elements': n,
      'baseline_elements': b_n,
      'stateless_elements': s_n,
      'elements_match': n == b_n == s_n,
      'input_tokens': tok,
      'baseline_tokens': b_tok,
      'stateless_tokens': s_tok,
      'tokens_match': tok == b_tok == s_tok,
      'baseline_num_batches': len(baseline_summaries),
      'stateless_num_batches': len(stateless_summaries),
  }


# ---------------------------------------------------------------------------
# Performance benchmark (N=20 trials)
# ---------------------------------------------------------------------------


def run_performance_benchmark(
    data: list[str],
    max_batch_size: int,
    max_batch_weight: int,
    num_trials: int = 20,
    warmup_trials: int = 3
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[dict[str, int]],
    list[dict[str, int]],
]:
  """Run N=20 trials for baseline and stateless."""
  total_tokens = sum(len(s) for s in data)

  baseline_trials = []
  stateless_trials = []
  baseline_sample_summaries = []
  stateless_sample_summaries = []

  for trial_idx in range(warmup_trials + num_trials):
    is_warmup = trial_idx < warmup_trials
    trial_results = {}

    if trial_idx % 2 == 0:
      trial_order = ('baseline', 'stateless')
    else:
      trial_order = ('stateless', 'baseline')

    for strategy in trial_order:
      summaries, runtime_ms = _run_batching_pipeline(
          strategy, data, max_batch_size, max_batch_weight)
      batch_latencies = [
          simulate_inference_latency(s['batch_size'], s['max_len'])
          for s in summaries
      ]
      trial_results[strategy] = {
          'runtime_ms': runtime_ms,
          'inference_ms': sum(batch_latencies),
          'e2e_ms': runtime_ms + sum(batch_latencies),
          'batch_latencies': batch_latencies,
          'num_batches': len(summaries),
          'summaries': summaries,
      }

    if not is_warmup:
      baseline_trials.append(trial_results['baseline'])
      stateless_trials.append(trial_results['stateless'])
      if not baseline_sample_summaries:
        baseline_sample_summaries = trial_results['baseline']['summaries']
      if not stateless_sample_summaries:
        stateless_sample_summaries = trial_results['stateless']['summaries']

  def _stats(trials):
    e2e = [t['e2e_ms'] for t in trials]
    tput = [total_tokens / (t['e2e_ms'] / 1000) for t in trials]
    runtime = [t['runtime_ms'] for t in trials]
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
        'runtime_median': percentile(runtime, 50),
        'runtime_p25': percentile(runtime, 25),
        'runtime_p75': percentile(runtime, 75),
        'runtime_p95': percentile(runtime, 95),
        'batch_lat_p50': percentile(all_lat, 50),
        'batch_lat_p95': percentile(all_lat, 95),
        'batch_lat_p99': percentile(all_lat, 99),
        'inf_p95': percentile(all_lat, 95),
        'num_trials': len(trials),
        'num_batches': trials[0]['num_batches'] if trials else 0,
    }

  return (
      _stats(baseline_trials),
      _stats(stateless_trials),
      baseline_sample_summaries,
      stateless_sample_summaries,
  )


# ---------------------------------------------------------------------------
# Single benchmark run
# ---------------------------------------------------------------------------


def run_benchmark(
    num_elements: int = 10000,
    min_length: int = 1,
    max_length: int = 500,
    max_batch_size: int = 32,
    max_batch_weight: int = 2000,
    distribution: str = 'pareto',
    seed: int = 42) -> dict[str, Any]:
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

  baseline_perf, stateless_perf, baseline_summaries, stateless_summaries = (
      run_performance_benchmark(data, max_batch_size, max_batch_weight))
  baseline_pad = compute_padding_stats(baseline_summaries)
  stateless_pad = compute_padding_stats(stateless_summaries)
  baseline_pad.update(baseline_perf)
  stateless_pad.update(stateless_perf)

  validation = validate_invariants(
      data, baseline_summaries, stateless_summaries)

  return {
      'config': {
          'num_elements': num_elements,
          'max_batch_size': max_batch_size,
          'max_batch_weight': max_batch_weight,
          'distribution': distribution,
          'runner': 'DirectRunner',
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


def print_results(results: dict[str, Any]) -> None:
  cfg = results['config']
  ds = results['data_stats']
  bl = results['baseline']
  st = results['stateless']
  val = results['validation']

  print("=" * 80)
  print(
      f"Distribution: {cfg['distribution']}  |  "
      f"N={cfg['num_elements']}  |  "
      f"runner={cfg['runner']}  |  "
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
    print("    Pipeline runtime (ms):")
    print(
        f"      Median [IQR]:"
        f" {s['runtime_median']:.2f}"
        f" [{s['runtime_p25']:.2f}"
        f"-{s['runtime_p75']:.2f}]")
    print(f"      P95:          {s['runtime_p95']:.2f}")
    print("    ")
    print("    Batch latency (ms):")
    print(f"      P50:          {s['batch_lat_p50']:.1f}")
    print(f"      P95:          {s['batch_lat_p95']:.1f}")
    print(f"      P99:          {s['batch_lat_p99']:.1f}")

  _arm("Baseline (BatchElements)", bl)
  _arm("Stateless (SortAndBatchElements w/ weight-based splitting)", st)

  # Explicit arrows so direction is unambiguous.
  #   down arrow = value decreased (good for latency/padding)
  #   up arrow = value increased (good for throughput)
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
      'Pipeline runtime ',
      bl['runtime_median'],
      st['runtime_median'],
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
  print("BASELINE (BatchElements) vs STATELESS (SortAndBatchElements)")
  print("=" * 80)
  print()
  print("Experiment design:")
  print("  A = Baseline  : BatchElements with min=max=32")
  print("  B = Stateless : SortAndBatchElements with max_batch_weight=2000")
  print("                  (sort within runner bundle, then split by weight)")
  print()
  print("Implementation notes:")
  print("  - Runs beam.Create(...) pipelines on DirectRunner")
  print("  - Materializes per-batch summaries through a temporary text sink")
  print("  - Uses runner-defined bundle boundaries rather than a synthetic")
  print("    bundle_size knob")
  print()
  print("Methodology:")
  print("  - N=20 trials, 3 warmup excluded")
  print("  - DirectRunner, in_memory mode, single worker")
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
      distribution=dist,
      seed=42)
  print_results(r)


if __name__ == '__main__':
  main()
