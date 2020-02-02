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

"""Unit tests for core module."""

# pytype: skip-file

from __future__ import absolute_import

import unittest

from apache_beam.transforms.core import WatermarkEstimator
from apache_beam.utils.timestamp import Timestamp


class WatermarkEstimatorTest(unittest.TestCase):

  def test_set_watermark(self):
    watermark_estimator = WatermarkEstimator()
    self.assertEqual(watermark_estimator.current_watermark(), None)
    # set_watermark should only accept timestamp.Timestamp.
    with self.assertRaises(ValueError):
      watermark_estimator.set_watermark(0)

    # watermark_estimator should always keep minimal timestamp.
    watermark_estimator.set_watermark(Timestamp(100))
    self.assertEqual(watermark_estimator.current_watermark(), 100)
    watermark_estimator.set_watermark(Timestamp(150))
    self.assertEqual(watermark_estimator.current_watermark(), 100)
    watermark_estimator.set_watermark(Timestamp(50))
    self.assertEqual(watermark_estimator.current_watermark(), 50)

  def test_reset(self):
    watermark_estimator = WatermarkEstimator()
    watermark_estimator.set_watermark(Timestamp(100))
    self.assertEqual(watermark_estimator.current_watermark(), 100)
    watermark_estimator.reset()
    self.assertEqual(watermark_estimator.current_watermark(), None)


if __name__ == '__main__':
  unittest.main()
