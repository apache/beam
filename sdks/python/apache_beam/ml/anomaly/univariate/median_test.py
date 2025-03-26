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
import unittest

from apache_beam.ml.anomaly.univariate.median import MedianTracker
from apache_beam.ml.anomaly.univariate.quantile import SimpleSlidingQuantileTracker  # pylint: disable=line-too-long


class MedianTest(unittest.TestCase):
  def test_default_tracker(self):
    t = MedianTracker()
    self.assertTrue(math.isnan(t.get()))

    t.push(1.0)
    self.assertEqual(t.get(), 1.0)

    t.push(2.0)
    self.assertEqual(t.get(), 1.5)

    t.push(1.0)
    self.assertEqual(t.get(), 1.0)

    t.push(20.0)
    self.assertEqual(t.get(), 1.5)

    t.push(10.0)
    self.assertEqual(t.get(), 2.0)

  def test_custom_tracker(self):
    t = MedianTracker(SimpleSlidingQuantileTracker(3, 0.5))
    self.assertTrue(math.isnan(t.get()))

    t.push(1.0)
    self.assertEqual(t.get(), 1.0)

    t.push(2.0)
    self.assertEqual(t.get(), 1.5)

    t.push(1.0)
    self.assertEqual(t.get(), 1.0)

    t.push(20.0)
    self.assertEqual(t.get(), 2.0)

    t.push(10.0)
    self.assertEqual(t.get(), 10.0)

  def test_wrong_tracker(self):
    t = MedianTracker(SimpleSlidingQuantileTracker(50, 0.1))
    self.assertRaises(AssertionError, lambda: t.run_original_init())


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
