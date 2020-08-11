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

"""A microbenchmark for measuring the overhead of runtime_type_check vs
performance_runtime_type_check vs a pipeline with no runtime type check.

This runs a sequence of trivial DoFn's over a set of inputs to simulate
a real-world pipeline that processes lots of data.

Run as

   python -m apache_beam.tools.runtime_type_check_microbenchmark
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
from builtins import range
from collections import defaultdict
from time import time

from typing import Iterable
from typing import Tuple
from typing import Union

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.tools import utils


@beam.typehints.with_input_types(Tuple[int, ...])
class SimpleInput(beam.DoFn):
  def process(self, element, *args, **kwargs):
    yield element


@beam.typehints.with_output_types(Tuple[int, ...])
class SimpleOutput(beam.DoFn):
  def process(self, element, *args, **kwargs):
    yield element


@beam.typehints.with_input_types(
    Tuple[int, str, Tuple[float, ...], Iterable[int], Union[str, int]])
class NestedInput(beam.DoFn):
  def process(self, element, *args, **kwargs):
    yield element


@beam.typehints.with_output_types(
    Tuple[int, str, Tuple[float, ...], Iterable[int], Union[str, int]])
class NestedOutput(beam.DoFn):
  def process(self, element, *args, **kwargs):
    yield element


def run_benchmark(num_dofns=100, num_runs=10, num_elements_step=1000):
  options_map = {
      'No Type Check': PipelineOptions(),
      'Runtime Type Check': PipelineOptions(runtime_type_check=True),
      'Performance Runtime Type Check': PipelineOptions(
          performance_runtime_type_check=True)
  }

  for run in range(num_runs):
    num_elements = num_elements_step * run + 1
    simple_elements = [
        tuple(i for i in range(200)) for _ in range(num_elements)
    ]
    nested_elements = [(
        1,
        '2',
        tuple(float(i) for i in range(100)), [i for i in range(100)],
        '5') for _ in range(num_elements)]
    timings = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))

    for option_name, options in options_map.items():
      # Run a Pipeline using DoFn's with simple typehints
      start = time()
      with beam.Pipeline() as p:
        pc = p | beam.Create(simple_elements)
        for ix in range(num_dofns):
          pc = (
              pc | 'SimpleOutput %i' % ix >> beam.ParDo(SimpleOutput())
              | 'SimpleInput %i' % ix >> beam.ParDo(SimpleInput()))
      timings[num_elements]['Simple Types'][option_name] = time() - start

      # Run a pipeline using DoFn's with nested typehints
      start = time()
      with beam.Pipeline(options=options) as p:
        pc = p | beam.Create(nested_elements)
        for ix in range(num_dofns):
          pc = (
              pc | 'NestedOutput %i' % ix >> beam.ParDo(NestedOutput())
              | 'NestedInput %i' % ix >> beam.ParDo(NestedInput()))
      timings[num_elements]['Nested Types'][option_name] = time() - start

    for num_elements, element_type_map in timings.items():
      print("%d Element%s" % (num_elements, " " if num_elements == 1 else "s"))
      for element_type, option_name_map in element_type_map.items():
        print(f"-- %s" % element_type)
        for option_name, time_elapsed in option_name_map.items():
          print(f"---- %.2f sec (%s)" % (time_elapsed, option_name))
    print('\n')


if __name__ == '__main__':
  logging.basicConfig()
  utils.check_compiled('apache_beam.runners.common')
  run_benchmark()
