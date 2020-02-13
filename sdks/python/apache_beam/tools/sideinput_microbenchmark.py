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

"""A microbenchmark for measuring performance of Side Inputs iterable.

Runs side input iterable that fetches from multiple FakeSources, and
measures the time to fetch all elements from all sources.

Run as
  python -m apache_beam.tools.sideinput_microbenchmark
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import time
from builtins import range

from apache_beam.runners.worker import opcounters
from apache_beam.runners.worker import sideinputs
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.sideinputs_test import FakeSource
from apache_beam.tools import utils
from apache_beam.utils.counters import CounterFactory


def long_generator(value, elements):
  for _ in range(elements):
    yield value


def run_benchmark(num_runs=50, input_per_source=4000, num_sources=4):
  print("Number of runs:", num_runs)
  print("Input size:", num_sources * input_per_source)
  print("Sources:", num_sources)

  times = []
  for _ in range(num_runs):
    counter_factory = CounterFactory()
    state_sampler = statesampler.StateSampler('basic', counter_factory)
    state_sampler.start()
    with state_sampler.scoped_state('step1', 'state'):
      si_counter = opcounters.SideInputReadCounter(
          counter_factory, state_sampler, 'step1', 1)
      si_counter = opcounters.NoOpTransformIOCounter()
      sources = [
          FakeSource(long_generator(i, input_per_source))
          for i in range(num_sources)
      ]
      iterator_fn = sideinputs.get_iterator_fn_for_sources(
          sources, read_counter=si_counter)
      start = time.time()
      list(iterator_fn())
      time_cost = time.time() - start
      times.append(time_cost)
    state_sampler.stop()

  print("Runtimes:", times)

  avg_runtime = sum(times) / len(times)
  print("Average runtime:", avg_runtime)
  print("Time per element:", avg_runtime / (input_per_source * num_sources))


if __name__ == '__main__':
  logging.basicConfig()
  utils.check_compiled('apache_beam.runners.worker.opcounters')
  run_benchmark()
