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

This runs a series of N parallel pipelines with M parallel stages each. Each
stage does the following:

1) Put all the PCollection elements in state
2) Set a timer for the future
3) When the timer fires, change the key and output all the elements downstream

This executes the same codepaths that are run on the Fn API (and Dataflow)
workers, but is generally easier to run (locally) and more stable..

Run as

   python -m apache_beam.tools.fn_api_runner_microbenchmark
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import time
from builtins import range
from builtins import zip

import apache_beam as beam
from apache_beam.coders import VarIntCoder
from apache_beam.runners.portability.fn_api_runner import FnApiRunner
from apache_beam.tools import utils
from apache_beam.transforms.timeutil import TimeDomain
import apache_beam.typehints.typehints as typehints
from apache_beam.transforms.userstate import on_timer
from apache_beam.transforms.userstate import SetStateSpec
from apache_beam.transforms.userstate import TimerSpec
from scipy import stats


class BagInStateOutputAfterTimer(beam.DoFn):

  SET_STATE = SetStateSpec('buffer', VarIntCoder())
  EMIT_TIMER = TimerSpec('emit_timer', TimeDomain.WATERMARK)

  def process(self,
      element,
      set_state=beam.DoFn.StateParam(SET_STATE),
      emit_timer=beam.DoFn.TimerParam(EMIT_TIMER)):
    key, values = element
    for v in values:
      set_state.add(v)
    emit_timer.set(1)

  @on_timer(EMIT_TIMER)
  def emit_values(self, set_state=beam.DoFn.StateParam(SET_STATE)):
    values = set_state.read()
    return [(random.randint(0, 1000), v) for v in values]


def _build_serial_stages(pipeline,
                         num_serial_stages,
                         num_elements,
                         stage_count):
  pc = (pipeline |
        ('start_stage%s' % stage_count) >> beam.Create([
            (random.randint(0, 1000), i) for i in range(num_elements)])
        | ('gbk_start_stage%s' % stage_count) >> beam.GroupByKey())

  for i in range(num_serial_stages):
    pc = (pc
          | ('stage%s_map%s' % (stage_count, i)) >> beam.ParDo(
              BagInStateOutputAfterTimer()).with_output_types(
                  typehints.KV[int, int])
          | ('stage%s_gbk%s' % (stage_count, i)) >> beam.GroupByKey())

  return pc


def run_benchmark(num_parallel_stages=7,
                  num_serial_stages=5,
                  num_runs=10,
                  num_elements_step=100):
  timings = {}
  for run in range(num_runs):
    num_elements = num_elements_step * run + 1
    start = time.time()
    with beam.Pipeline(runner=FnApiRunner()) as p:
      for i in range(num_parallel_stages):
        _build_serial_stages(p, num_serial_stages, num_elements, i)
    timings[num_elements] = time.time() - start
    print("%6d element%s %g sec" % (
        num_elements, " " if num_elements == 1 else "s", timings[num_elements]))

  print()
  # pylint: disable=unused-variable
  gradient, intercept, r_value, p_value, std_err = stats.linregress(
      *list(zip(*list(timings.items()))))
  # Fixed cost is the most important variable, as it represents the fixed cost
  #   of running all the bundles through all the stages.
  print("Fixed cost  ", intercept)
  print("Per-element ", gradient / (num_serial_stages * num_parallel_stages))
  print("R^2         ", r_value**2)


if __name__ == '__main__':
  utils.check_compiled('apache_beam.runners.common')
  run_benchmark()
