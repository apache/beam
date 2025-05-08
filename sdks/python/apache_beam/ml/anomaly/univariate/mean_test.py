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

from apache_beam.ml.anomaly.univariate.mean import IncLandmarkMeanTracker
from apache_beam.ml.anomaly.univariate.mean import IncSlidingMeanTracker
from apache_beam.ml.anomaly.univariate.mean import SimpleSlidingMeanTracker

FLOAT64_MAX = 1.79769313486231570814527423731704356798070e+308


class LandmarkMeanTest(unittest.TestCase):
  def test_without_nan(self):
    t = IncLandmarkMeanTracker()
    self.assertTrue(math.isnan(t.get()))  # Returns NaN if tracker is empty

    t.push(1)
    self.assertEqual(t.get(), 1.0)
    t.push(3)
    self.assertEqual(t.get(), 2.0)
    t.push(8)
    self.assertEqual(t.get(), 4.0)
    t.push(16)
    self.assertEqual(t.get(), 7.0)
    t.push(-3)
    self.assertEqual(t.get(), 5.0)

  def test_with_nan(self):
    t = IncLandmarkMeanTracker()

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))  # NaN is ignored
    t.push(1)
    self.assertEqual(t.get(), 1.0)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.0)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.0)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.0)

  def test_with_float64_max(self):
    t = IncLandmarkMeanTracker()
    t.push(FLOAT64_MAX)
    self.assertEqual(t.get(), FLOAT64_MAX)
    t.push(FLOAT64_MAX)
    self.assertEqual(t.get(), FLOAT64_MAX)

  def test_accuracy_fuzz(self):
    seed = int(time.time())
    random.seed(seed)
    print("Random seed: %d" % seed)

    for _ in range(10):
      numbers = []
      for _ in range(5000):
        numbers.append(random.randint(0, 1000))

      with warnings.catch_warnings(record=False):
        warnings.simplefilter("ignore")
        t1 = IncLandmarkMeanTracker()
      t2 = SimpleSlidingMeanTracker(len(numbers))
      for v in numbers:
        t1.push(v)
        t2.push(v)
        self.assertTrue(abs(t1.get() - t2.get()) < 1e-9)


class SlidingMeanTest(unittest.TestCase):
  @parameterized.expand([SimpleSlidingMeanTracker, IncSlidingMeanTracker])
  def test_without_nan(self, tracker):
    t = tracker(3)
    self.assertTrue(math.isnan(t.get()))  # Returns NaN if tracker is empty

    t.push(1)
    self.assertEqual(t.get(), 1.0)
    t.push(3)
    self.assertEqual(t.get(), 2.0)
    t.push(8)
    self.assertEqual(t.get(), 4.0)
    t.push(16)
    self.assertEqual(t.get(), 9.0)
    t.push(-3)
    self.assertEqual(t.get(), 7.0)

  @parameterized.expand([SimpleSlidingMeanTracker, IncSlidingMeanTracker])
  def test_with_nan(self, tracker):
    t = tracker(3)

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))  # NaN is ignored
    t.push(1)
    self.assertEqual(t.get(), 1.0)

    # flush the only number out
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.0)
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.0)
    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))  # All values in the tracker are NaN
    t.push(4)
    self.assertEqual(t.get(), 4.0)

  @parameterized.expand([SimpleSlidingMeanTracker, IncSlidingMeanTracker])
  def test_with_float64_max(self, tracker):
    t = tracker(2)
    t.push(FLOAT64_MAX)
    self.assertEqual(t.get(), FLOAT64_MAX)
    t.push(FLOAT64_MAX)
    if tracker is IncSlidingMeanTracker:
      self.assertEqual(t.get(), FLOAT64_MAX)
      self.assertFalse(math.isinf(t.get()))
    else:
      # SimpleSlidingMean (using Numpy) returns inf when it computes the
      # average of [float64_max, float64_max].
      self.assertTrue(math.isinf(t.get()))

  def test_accuracy_fuzz(self):
    seed = int(time.time())
    random.seed(seed)
    print("Random seed: %d" % seed)

    for _ in range(10):
      numbers = []
      for _ in range(5000):
        numbers.append(random.randint(0, 1000))

      t1 = IncSlidingMeanTracker(100)
      t2 = SimpleSlidingMeanTracker(100)
      for v in numbers:
        t1.push(v)
        t2.push(v)
        self.assertTrue(abs(t1.get() - t2.get()) < 1e-9)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
