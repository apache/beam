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

"""A microbenchmark for measuring changes in the critical path of FnApiRunner.
This microbenchmark attempts to measure the overhead of the main data paths
for the FnApiRunner. Specifically state, timers, and shuffling of data.

This runs a series of N parallel pipelines with M parallel stages each. Each
stage does the following:

1) Put all the PCollection elements in state
2) Set a timer for the future
3) When the timer fires, change the key and output all the elements downstream

This executes the same codepaths that are run on the Fn API (and Dataflow)
workers, but is generally easier to run (locally) and more stable..

Run as

   python -m apache_beam.tools.fn_api_runner_microbenchmark

The main metric to work with for this benchmark is Fixed Cost. This represents
the fixed cost of ovehead for the data path of the FnApiRunner.

Initial results were:

run 1 of 10, per element time cost: 3.6778 sec
run 2 of 10, per element time cost: 0.053498 sec
run 3 of 10, per element time cost: 0.0299434 sec
run 4 of 10, per element time cost: 0.0211154 sec
run 5 of 10, per element time cost: 0.0170031 sec
run 6 of 10, per element time cost: 0.0150809 sec
run 7 of 10, per element time cost: 0.013218 sec
run 8 of 10, per element time cost: 0.0119685 sec
run 9 of 10, per element time cost: 0.0107382 sec
run 10 of 10, per element time cost: 0.0103208 sec


Fixed cost   4.537164939085642
Per-element  0.005474923321695039
R^2          0.95189
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import logging
import random
from builtins import range

import apache_beam as beam
import apache_beam.typehints.typehints as typehints
from apache_beam.coders import VarIntCoder
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
from apache_beam.tools import utils
from apache_beam.transforms.timeutil import TimeDomain
from apache_beam.transforms.userstate import SetStateSpec
from apache_beam.transforms.userstate import TimerSpec
from apache_beam.transforms.userstate import on_timer

NUM_PARALLEL_STAGES = 7

NUM_SERIAL_STAGES = 5


class BagInStateOutputAfterTimer(beam.DoFn):

  SET_STATE = SetStateSpec('buffer', VarIntCoder())
  EMIT_TIMER = TimerSpec('emit_timer', TimeDomain.WATERMARK)

  def process(
      self,
      element,
      set_state=beam.DoFn.StateParam(SET_STATE),
      emit_timer=beam.DoFn.TimerParam(EMIT_TIMER)):
    _, values = element
    for v in values:
      set_state.add(v)
    emit_timer.set(1)

  @on_timer(EMIT_TIMER)
  def emit_values(self, set_state=beam.DoFn.StateParam(SET_STATE)):
    values = set_state.read()
    return [(random.randint(0, 1000), v) for v in values]


def _build_serial_stages(
    pipeline, num_serial_stages, num_elements, stage_count):
  pc = (
      pipeline | ('start_stage%s' % stage_count) >> beam.Create(
          [(random.randint(0, 1000), i) for i in range(num_elements)])
      | ('gbk_start_stage%s' % stage_count) >> beam.GroupByKey())

  for i in range(num_serial_stages):
    pc = (
        pc
        | ('stage%s_map%s' % (stage_count, i)) >> beam.ParDo(
            BagInStateOutputAfterTimer()).with_output_types(
                typehints.KV[int, int])
        | ('stage%s_gbk%s' % (stage_count, i)) >> beam.GroupByKey())

  return pc


def run_single_pipeline(size):
  def _pipeline_runner():
    with beam.Pipeline(runner=FnApiRunner()) as p:
      for i in range(NUM_PARALLEL_STAGES):
        _build_serial_stages(p, NUM_SERIAL_STAGES, size, i)

  return _pipeline_runner


def run_benchmark(starting_point, num_runs, num_elements_step, verbose):
  suite = [
      utils.LinearRegressionBenchmarkConfig(
          run_single_pipeline, starting_point, num_elements_step, num_runs)
  ]
  utils.run_benchmarks(suite, verbose=verbose)


if __name__ == '__main__':
  logging.basicConfig()
  utils.check_compiled('apache_beam.runners.common')

  parser = argparse.ArgumentParser()
  parser.add_argument('--num_runs', default=10, type=int)
  parser.add_argument('--starting_point', default=1, type=int)
  parser.add_argument('--increment', default=100, type=int)
  parser.add_argument('--verbose', default=True, type=bool)
  options = parser.parse_args()

  run_benchmark(
      options.starting_point,
      options.num_runs,
      options.increment,
      options.verbose)
