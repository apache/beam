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

"""A microbenchmark for measuring changes in overhead for critical code paths.

This runs a sequence of trivial Maps over a variable number of inputs to
estimate the per-element processing time.  It can be useful to run this
benchmark before and after a proposed set of changes.  A typical per-element
cost should be 1-2 microseconds.

This executes the same codepaths that are run on the Fn API (and Dataflow)
workers, but is generally easier to run (locally) and more stable.  It does
not, on the other hand, exercise any non-trivial amount of IO (e.g. shuffle).

Run as

   python -m apache_beam.tools.map_fn_microbenchmark
"""

# pytype: skip-file

import argparse
import logging

import apache_beam as beam
from apache_beam.tools import utils
from apache_beam.transforms.window import FixedWindows


def map_pipeline(num_elements, num_maps=100):
  def _pipeline_runner():
    with beam.Pipeline() as p:
      pc = p | beam.Create(list(range(num_elements)))
      for ix in range(num_maps):
        pc = pc | 'Map%d' % ix >> beam.FlatMap(lambda x: (None, ))

  return _pipeline_runner


def map_with_global_side_input_pipeline(num_elements, num_maps=100):
  def add(element, side_input):
    return element + side_input

  def _pipeline_runner():
    with beam.Pipeline() as p:
      side = p | 'CreateSide' >> beam.Create([1])
      pc = p | 'CreateMain' >> beam.Create(list(range(num_elements)))
      for ix in range(num_maps):
        pc = pc | 'Map%d' % ix >> beam.Map(add, beam.pvalue.AsSingleton(side))

  return _pipeline_runner


def map_with_fixed_window_side_input_pipeline(num_elements, num_maps=100):
  def add(element, side_input):
    return element + side_input

  def _pipeline_runner():
    with beam.Pipeline() as p:
      side = p | 'CreateSide' >> beam.Create(
          [1]) | 'WindowSide' >> beam.WindowInto(FixedWindows(1000))
      pc = p | 'CreateMain' >> beam.Create(list(range(
          num_elements))) | 'WindowMain' >> beam.WindowInto(FixedWindows(1000))
      for ix in range(num_maps):
        pc = pc | 'Map%d' % ix >> beam.Map(add, beam.pvalue.AsSingleton(side))

  return _pipeline_runner


def run_benchmark(
    starting_point=1,
    num_runs=10,
    num_elements_step=100,
    verbose=True,
    profile_filename_base=None,
):
  suite = [
      utils.LinearRegressionBenchmarkConfig(
          map_pipeline, starting_point, num_elements_step, num_runs),
      utils.BenchmarkConfig(
          map_with_global_side_input_pipeline,
          starting_point * 1000,
          num_runs,
      ),
      utils.BenchmarkConfig(
          map_with_fixed_window_side_input_pipeline,
          starting_point * 1000,
          num_runs,
      ),
  ]
  return utils.run_benchmarks(
      suite, verbose=verbose, profile_filename_base=profile_filename_base)


if __name__ == '__main__':
  logging.basicConfig()
  utils.check_compiled('apache_beam.runners.common')

  parser = argparse.ArgumentParser()
  parser.add_argument('--num_runs', default=10, type=int)
  parser.add_argument('--starting_point', default=1, type=int)
  parser.add_argument('--increment', default=100, type=int)
  parser.add_argument('--verbose', default=True, type=bool)
  parser.add_argument('--profile_filename_base', default=None, type=str)
  options = parser.parse_args()

  run_benchmark(
      options.starting_point,
      options.num_runs,
      options.increment,
      options.verbose,
      options.profile_filename_base,
  )
