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

"""A microbenchmark for measuring changes in the performance of TestStream
running locally.
This microbenchmark attempts to measure the overhead of the main data paths
for the TestStream. Specifically new elements, watermark changes and processing
time advances.

This runs a series of N parallel pipelines with M parallel stages each. Each
stage does the following:

1) Put all the PCollection elements in a window
2) Wait until the watermark advances past the end of the window.
3) When the watermark passes, change the key and output all the elements
4) Go back to #1 until all elements in the stream have been consumed.

This executes the same codepaths that are run on the Fn API (and Dataflow)
workers, but is generally easier to run (locally) and more stable.

Run as

   python -m apache_beam.tools.teststream_microbenchmark

"""

# pytype: skip-file

import argparse
import itertools
import logging
import random

import apache_beam as beam
from apache_beam import WindowInto
from apache_beam.runners import DirectRunner
from apache_beam.testing.test_stream import TestStream
from apache_beam.tools import utils
from apache_beam.transforms.window import FixedWindows
from apache_beam.typehints import typehints

NUM_PARALLEL_STAGES = 7

NUM_SERIAL_STAGES = 6


class RekeyElements(beam.DoFn):
  def process(self, element):
    _, values = element
    return [(random.randint(0, 1000), v) for v in values]


def _build_serial_stages(input_pc, num_serial_stages, stage_count):
  pc = (input_pc | ('gbk_start_stage%s' % stage_count) >> beam.GroupByKey())

  for i in range(num_serial_stages):
    pc = (
        pc
        | ('stage%s_map%s' % (stage_count, i)) >> beam.ParDo(
            RekeyElements()).with_output_types(typehints.KV[int, int])
        | ('stage%s_gbk%s' % (stage_count, i)) >> beam.GroupByKey())

  return pc


def run_single_pipeline(size):
  def _pipeline_runner():
    with beam.Pipeline(runner=DirectRunner()) as p:
      ts = TestStream().advance_watermark_to(0)
      all_elements = iter(range(size))
      watermark = 0
      while True:
        next_batch = list(itertools.islice(all_elements, 100))
        if not next_batch:
          break
        ts = ts.add_elements([(i, random.randint(0, 1000)) for i in next_batch])
        watermark = watermark + 100
        ts = ts.advance_watermark_to(watermark)
      ts = ts.advance_watermark_to_infinity()

      input_pc = p | ts | WindowInto(FixedWindows(100))
      for i in range(NUM_PARALLEL_STAGES):
        _build_serial_stages(input_pc, NUM_SERIAL_STAGES, i)

  return _pipeline_runner


def run_benchmark(
    starting_point=1, num_runs=10, num_elements_step=300, verbose=True):
  suite = [
      utils.LinearRegressionBenchmarkConfig(
          run_single_pipeline, starting_point, num_elements_step, num_runs)
  ]
  return utils.run_benchmarks(suite, verbose=verbose)


if __name__ == '__main__':
  logging.basicConfig()
  utils.check_compiled('apache_beam.runners.common')

  parser = argparse.ArgumentParser()
  parser.add_argument('--num_runs', default=10, type=int)
  parser.add_argument('--starting_point', default=1, type=int)
  parser.add_argument('--increment', default=300, type=int)
  parser.add_argument('--verbose', default=True, type=bool)
  options = parser.parse_args()

  run_benchmark(
      options.starting_point,
      options.num_runs,
      options.increment,
      options.verbose)
