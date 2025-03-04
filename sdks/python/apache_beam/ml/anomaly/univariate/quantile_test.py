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
import math
import random
import time
import unittest
import warnings

from parameterized import parameterized

from apache_beam.ml.anomaly.univariate.quantile import BufferedLandmarkQuantileTracker  # pylint: disable=line-too-long
from apache_beam.ml.anomaly.univariate.quantile import BufferedSlidingQuantileTracker
from apache_beam.ml.anomaly.univariate.quantile import SimpleSlidingQuantileTracker  # pylint: disable=line-too-long


class LandmarkQuantileTest(unittest.TestCase):
  def test_without_nan(self):
    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      t = BufferedLandmarkQuantileTracker(0.5)

    self.assertTrue(math.isnan(t.get()))
    t.push(1)
    self.assertEqual(t.get(), 1.0)
    t.push(2)
    self.assertEqual(t.get(), 1.5)
    t.push(2)
    self.assertEqual(t.get(), 2.0)
    t.push(0)
    self.assertEqual(t.get(), 1.5)
    t.push(3)
    self.assertEqual(t.get(), 2.0)
    t.push(1)
    self.assertEqual(t.get(), 1.5)

  def test_with_nan(self):
    with warnings.catch_warnings(record=False):
      warnings.simplefilter("ignore")
      t = BufferedLandmarkQuantileTracker(0.2)

    self.assertTrue(math.isnan(t.get()))
    t.push(1)
    self.assertEqual(t.get(), 1.0)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.0)
    t.push(2)
    self.assertEqual(t.get(), 1.2)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.2)
    t.push(0)
    self.assertEqual(t.get(), 0.4)

  def test_accuracy_fuzz(self):
    seed = int(time.time())
    random.seed(seed)
    print("Random seed: %d" % seed)

    def _accuracy_helper():
      numbers = []
      for _ in range(5000):
        numbers.append(random.randint(0, 1000))

      with warnings.catch_warnings(record=False):
        warnings.simplefilter("ignore")
        t1 = BufferedLandmarkQuantileTracker(0.5)
      t2 = SimpleSlidingQuantileTracker(len(numbers), 0.5)
      for v in numbers:
        t1.push(v)
        t2.push(v)
        self.assertTrue(abs(t1.get() - t2.get()) < 1e-9)

    for _ in range(10):
      _accuracy_helper()


class SlidingQuantileTest(unittest.TestCase):
  @parameterized.expand(
      [  #SimpleSlidingQuantileTracker,
          BufferedSlidingQuantileTracker
      ])
  def test_without_nan(self, tracker):
    t = tracker(3, 0.5)
    self.assertTrue(math.isnan(t.get()))
    t.push(1)
    self.assertEqual(t.get(), 1.0)
    t.push(2)
    self.assertEqual(t.get(), 1.5)
    t.push(2)
    self.assertEqual(t.get(), 2.0)
    t.push(0)
    self.assertEqual(t.get(), 2.0)
    t.push(3)
    self.assertEqual(t.get(), 2.0)
    t.push(1)
    self.assertEqual(t.get(), 1.0)

  @parameterized.expand(
      [SimpleSlidingQuantileTracker, BufferedSlidingQuantileTracker])
  def test_with_nan(self, tracker):
    t = tracker(3, 0.8)
    self.assertTrue(math.isnan(t.get()))
    t.push(1)
    self.assertEqual(t.get(), 1.0)
    t.push(2)
    self.assertEqual(t.get(), 1.8)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.8)
    t.push(2)
    self.assertEqual(t.get(), 2.0)
    t.push(0)
    self.assertEqual(t.get(), 1.6)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.6)
    t.push(float('nan'))
    self.assertEqual(t.get(), 0.0)
    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))
    t.push(3)
    self.assertEqual(t.get(), 3.0)
    t.push(1)
    self.assertEqual(t.get(), 2.6)

  def test_accuracy_fuzz(self):
    seed = int(time.time())
    random.seed(seed)
    print("Random seed: %d" % seed)

    def _accuracy_helper():
      numbers = []
      for _ in range(5000):
        numbers.append(random.randint(0, 1000))

      t1 = BufferedSlidingQuantileTracker(100, 0.1)
      t2 = SimpleSlidingQuantileTracker(100, 0.1)
      for v in numbers:
        t1.push(v)
        t2.push(v)
        self.assertTrue(abs(t1.get() - t2.get()) < 1e-9)

    for _ in range(10):
      _accuracy_helper()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
