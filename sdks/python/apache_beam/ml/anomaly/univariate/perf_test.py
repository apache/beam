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

import logging
import random
import statistics
import time
import timeit
import unittest
import warnings

from apache_beam.ml.anomaly.univariate.mean import *
from apache_beam.ml.anomaly.univariate.quantile import *
from apache_beam.ml.anomaly.univariate.stdev import *

seed_value_time = int(time.time())
random.seed(seed_value_time)
print(f"{'Seed value':32s}{seed_value_time}")

numbers = []
for _ in range(50000):
  numbers.append(random.randint(0, 1000))


def run_tracker(tracker, numbers):
  for i in range(len(numbers)):
    tracker.push(numbers[i])
    _ = tracker.get()


def print_result(tracker, number=10, repeat=5):
  runtimes = timeit.repeat(
      lambda: run_tracker(tracker, numbers), number=number, repeat=repeat)
  mean = statistics.mean(runtimes)
  sd = statistics.stdev(runtimes)
  print(f"{tracker.__class__.__name__:32s}{mean:.6f} ± {sd:.6f}")


class PerfTest(unittest.TestCase):
  def test_mean_perf(self):
    print()
    print_result(IncLandmarkMeanTracker())
    print_result(IncSlidingMeanTracker(100))
    # SimpleSlidingMeanTracker (numpy-based batch approach) is an order of
    # magnitude slower than other methods. To prevent excessively long test
    # runs, we reduce the number of repetitions.
    print_result(SimpleSlidingMeanTracker(100), number=1)

  def test_stdev_perf(self):
    print()
    print_result(IncLandmarkStdevTracker())
    print_result(IncSlidingStdevTracker(100))
    # Same as test_mean_perf, we reduce the number of repetitions here.
    print_result(SimpleSlidingStdevTracker(100), number=1)

  def test_quantile_perf(self):
    print()
    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      print_result(BufferedLandmarkQuantileTracker(0.5))
    print_result(BufferedSlidingQuantileTracker(100, 0.5))
    # Same as test_mean_perf, we reduce the number of repetitions here.
    print_result(SimpleSlidingQuantileTracker(100, 0.5), number=1)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
