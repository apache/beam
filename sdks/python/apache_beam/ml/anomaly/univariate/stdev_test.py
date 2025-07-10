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

from parameterized import parameterized

from apache_beam.ml.anomaly.univariate.stdev import IncLandmarkStdevTracker
from apache_beam.ml.anomaly.univariate.stdev import IncSlidingStdevTracker
from apache_beam.ml.anomaly.univariate.stdev import SimpleSlidingStdevTracker


class LandmarkStdevTest(unittest.TestCase):
  def test_without_nan(self):
    t = IncLandmarkStdevTracker()
    self.assertTrue(math.isnan(t.get()))
    t.push(1)
    self.assertTrue(math.isnan(t.get()))
    t.push(2)
    self.assertEqual(t.get(), 0.7071067811865476)
    t.push(3)
    self.assertEqual(t.get(), 1.0)
    t.push(10)
    self.assertEqual(t.get(), 4.08248290463863)

  def test_with_nan(self):
    t = IncLandmarkStdevTracker()

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))  # NaN is ignored

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))  # NaN is ignored

    t.push(2)
    self.assertTrue(math.isnan(t.get()))
    t.push(1)
    self.assertEqual(t.get(), 0.7071067811865476)
    t.push(3)
    self.assertEqual(t.get(), 1.0)

    # flush the only number out
    t.push(float('nan'))
    self.assertEqual(t.get(), 1.0)

    t.push(10)
    self.assertEqual(t.get(), 4.08248290463863)

  def test_accuracy_fuzz(self):
    seed = int(time.time())
    random.seed(seed)
    print("Random seed: %d" % seed)

    for _ in range(10):
      numbers = []
      for _ in range(5000):
        numbers.append(random.randint(0, 1000))

      t1 = IncLandmarkStdevTracker()
      t2 = SimpleSlidingStdevTracker(len(numbers))
      for v in numbers:
        t1.push(v)
        t2.push(v)
        self.assertTrue((math.isnan(t1.get()) and math.isnan(t2.get())) or
                        abs(t1.get() - t2.get()) < 1e-9)


class SlidingStdevTest(unittest.TestCase):
  @parameterized.expand([SimpleSlidingStdevTracker, IncSlidingStdevTracker])
  def test_without_nan(self, tracker):
    t = tracker(3)
    self.assertTrue(math.isnan(t.get()))
    t.push(2)
    self.assertTrue(math.isnan(t.get()))
    t.push(1)
    self.assertEqual(t.get(), 0.7071067811865476)
    t.push(3)
    self.assertEqual(t.get(), 1.0)
    t.push(10)
    self.assertEqual(t.get(), 4.725815626252609)

  @parameterized.expand([SimpleSlidingStdevTracker, IncSlidingStdevTracker])
  def test_stdev_with_nan(self, tracker):
    t = tracker(3)

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))  # NaN is ignored

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))  # NaN is ignored

    t.push(1)
    self.assertTrue(math.isnan(t.get()))
    t.push(2)
    self.assertEqual(t.get(), 0.7071067811865476)
    t.push(3)
    self.assertEqual(t.get(), 1.0)

    # flush the only number out
    t.push(float('nan'))
    self.assertEqual(t.get(), 0.7071067811865476)

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))

    t.push(float('nan'))
    self.assertTrue(math.isnan(t.get()))

    if tracker is IncSlidingStdevTracker:
      self.assertEqual(t._m2, 0)
      self.assertEqual(t._mean, 0)

    t.push(4)
    self.assertTrue(math.isnan(t.get()))
    t.push(5)
    self.assertEqual(t.get(), 0.7071067811865476)

  def test_accuracy_fuzz(self):
    seed = int(time.time())
    random.seed(seed)
    print("Random seed: %d" % seed)

    for _ in range(10):
      numbers = []
      for _ in range(5000):
        numbers.append(random.randint(0, 1000))

      t1 = IncSlidingStdevTracker(100)
      t2 = SimpleSlidingStdevTracker(100)
      for v in numbers:
        t1.push(v)
        t2.push(v)
        self.assertTrue((math.isnan(t1.get()) and math.isnan(t2.get())) or
                        abs(t1.get() - t2.get()) < 1e-9)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
