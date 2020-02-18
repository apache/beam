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

"""A microbenchmark for measuring DistributionAccumulator performance

This runs a sequence of distribution.update for random input value to calculate
average update time per input.
A typical update operation should run into 0.6 microseconds

Run as
  python -m apache_beam.tools.distribution_counter_microbenchmark
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import random
import sys
import time
from builtins import range

from apache_beam.tools import utils


def generate_input_values(num_input, lower_bound, upper_bound):
  values = []
  # pylint: disable=unused-variable
  for i in range(num_input):
    values.append(random.randint(lower_bound, upper_bound))
  return values


def run_benchmark(num_runs=100, num_input=10000, seed=time.time()):
  total_time = 0
  random.seed(seed)
  lower_bound = 0
  upper_bound = sys.maxsize
  inputs = generate_input_values(num_input, lower_bound, upper_bound)
  from apache_beam.transforms import DataflowDistributionCounter
  print("Number of runs:", num_runs)
  print("Input size:", num_input)
  print("Input sequence from %d to %d" % (lower_bound, upper_bound))
  print("Random seed:", seed)
  for i in range(num_runs):
    counter = DataflowDistributionCounter()
    start = time.time()
    counter.add_inputs_for_test(inputs)
    time_cost = time.time() - start
    print("Run %d: Total time cost %g sec" % (i + 1, time_cost))
    total_time += time_cost / num_input
  print("Per element update time cost:", total_time / num_runs)


if __name__ == '__main__':
  logging.basicConfig()
  utils.check_compiled(
      'apache_beam.transforms.cy_dataflow_distribution_counter')
  run_benchmark()
