#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
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

"""Utility functions for all microbenchmarks."""

# pytype: skip-file

import collections
import cProfile
import gc
import importlib
import os
import time
from typing import Callable
from typing import NamedTuple

import numpy

BenchmarkFn = Callable[[], None]
BenchmarkFactoryFn = Callable[[int], BenchmarkFn]


def check_compiled(module):
  """Check whether given module has been compiled.
  Args:
    module: string, module name
  """
  check_module = importlib.import_module(module)
  ext = os.path.splitext(check_module.__file__)[-1]
  if ext in ('.py', '.pyc'):
    raise RuntimeError(
        "Profiling uncompiled code.\n"
        "To compile beam, run "
        "'pip install Cython; python setup.py build_ext --inplace'")


class BenchmarkConfig(NamedTuple):
  """
  Attributes:
    benchmark: a callable that takes an int argument - benchmark size,
      and returns a callable. A returned callable must run the code being
      benchmarked on an input of specified size.

      For example, one can implement a benchmark as:

      class MyBenchmark(object):
        def __init__(self, size):
          [do necessary initialization]
        def __call__(self):
          [run the code in question]

    size: int, a size of the input. Aggregated per-element metrics
      are counted based on the size of the input.
    num_runs: int, number of times to run each benchmark.
  """
  benchmark: BenchmarkFactoryFn
  size: int
  num_runs: int

  def name(self):
    return getattr(self.benchmark, '__name__', str(self.benchmark))

  def __str__(self):
    return "%s, %s element(s)" % (self.name(), str(self.size))


class LinearRegressionBenchmarkConfig(NamedTuple):
  """
  Attributes:
    benchmark: a callable that takes an int argument - benchmark size,
      and returns a callable. A returned callable must run the code being
      benchmarked on an input of specified size.

      For example, one can implement a benchmark as:

      class MyBenchmark(object):
        def __init__(self, size):
          [do necessary initialization]
        def __call__(self):
          [run the code in question]

    starting_point: int, an initial size of the input. Regression results are
      calculated based on the input.
    increment: int, the rate of growth of the input for each run of the
      benchmark.
    num_runs: int, number of times to run each benchmark.
  """
  benchmark: Callable[[int], BenchmarkFn]
  starting_point: int
  increment: int
  num_runs: int

  def name(self):
    return getattr(self.benchmark, '__name__', str(self.benchmark))

  def __str__(self):
    return "%s, %s element(s) at start, %s growth per run" % (
        self.name(), str(self.starting_point), str(self.increment))


def run_benchmarks(benchmark_suite, verbose=True, profile_filename_base=None):
  """Runs benchmarks, and collects execution times.

  A simple instrumentation to run a callable several times, collect and print
  its execution times.

  Args:
    benchmark_suite: A list of BenchmarkConfig.
    verbose: bool, whether to print benchmark results to stdout.
    profile_filename_base: str, if present each benchmark will be profiled and
      have stats dumped to per-benchmark files beginning with this prefix.

  Returns:
    A dictionary of the form string -> list of floats. Keys of the dictionary
    are benchmark names, values are execution times in seconds for each run.
  """
  profiler = cProfile.Profile() if profile_filename_base else None

  def run(benchmark: BenchmarkFactoryFn, size: int):
    # Contain each run of a benchmark inside a function so that any temporary
    # objects can be garbage-collected after the run.
    benchmark_instance_callable = benchmark(size)
    start = time.time()
    if profiler:
      profiler.enable()
    _ = benchmark_instance_callable()
    if profiler:
      profiler.disable()
    return time.time() - start

  cost_series = collections.defaultdict(list)
  size_series = collections.defaultdict(list)
  for benchmark_config in benchmark_suite:
    name = str(benchmark_config)
    num_runs = benchmark_config.num_runs

    if isinstance(benchmark_config, LinearRegressionBenchmarkConfig):
      size = benchmark_config.starting_point
      step = benchmark_config.increment
    else:
      assert isinstance(benchmark_config, BenchmarkConfig)
      size = benchmark_config.size
      step = 0

    for run_id in range(num_runs):
      # Do a proactive GC before each run to minimize side-effects of different
      # runs.
      gc.collect()
      time_cost = run(benchmark_config.benchmark, size)
      # Appending size and time cost to perform linear regression
      cost_series[name].append(time_cost)
      size_series[name].append(size)
      if verbose:
        per_element_cost = time_cost / size
        print(
            "%s: run %d of %d, per element time cost: %g sec" %
            (name, run_id + 1, num_runs, per_element_cost))

      # Incrementing the size of the benchmark run by the step size
      size += step

    if profiler:
      filename = profile_filename_base + benchmark_config.name() + '.prof'
      if verbose:
        print("Dumping profile to " + filename)
      profiler.dump_stats(filename)

    if verbose:
      print("")

  if verbose:
    pad_length = max([len(str(bc)) for bc in benchmark_suite])

    for benchmark_config in benchmark_suite:
      name = str(benchmark_config)

      if isinstance(benchmark_config, LinearRegressionBenchmarkConfig):
        from scipy import stats
        print()
        # pylint: disable=unused-variable
        gradient, intercept, r_value, p_value, std_err = stats.linregress(
            size_series[name], cost_series[name])
        print("Fixed cost  ", intercept)
        print("Per-element ", gradient)
        print("R^2         ", r_value**2)
      else:
        assert isinstance(benchmark_config, BenchmarkConfig)
        per_element_median_cost = (
            numpy.median(cost_series[name]) / benchmark_config.size)
        std = numpy.std(cost_series[name]) / benchmark_config.size

        print(
            "%s: p. element median time cost: %g sec, relative std: %.2f%%" % (
                name.ljust(pad_length, " "),
                per_element_median_cost,
                std * 100 / per_element_median_cost))

  return size_series, cost_series
