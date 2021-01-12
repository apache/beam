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

"""Unit tests for built-in WatermarkEstimators"""

# pytype: skip-file

from __future__ import absolute_import

import unittest

import mock

from apache_beam.io.iobase import WatermarkEstimator
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.io.watermark_estimators import MonotonicWatermarkEstimator
from apache_beam.io.watermark_estimators import WalltimeWatermarkEstimator
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


class MonotonicWatermarkEstimatorTest(unittest.TestCase):
  def test_initialize_from_state(self):
    timestamp = Timestamp(10)
    watermark_estimator = MonotonicWatermarkEstimator(timestamp)
    self.assertIsInstance(watermark_estimator, WatermarkEstimator)
    self.assertEqual(watermark_estimator.get_estimator_state(), timestamp)

  def test_observe_timestamp(self):
    watermark_estimator = MonotonicWatermarkEstimator(Timestamp(10))
    watermark_estimator.observe_timestamp(Timestamp(15))
    self.assertEqual(watermark_estimator.current_watermark(), Timestamp(15))
    watermark_estimator.observe_timestamp(Timestamp(20))
    self.assertEqual(watermark_estimator.current_watermark(), Timestamp(20))
    watermark_estimator.observe_timestamp(Timestamp(20))
    self.assertEqual(watermark_estimator.current_watermark(), Timestamp(20))
    with self.assertRaises(ValueError):
      watermark_estimator.observe_timestamp(Timestamp(10))

  def test_get_estimator_state(self):
    watermark_estimator = MonotonicWatermarkEstimator(Timestamp(10))
    watermark_estimator.observe_timestamp(Timestamp(15))
    self.assertEqual(watermark_estimator.get_estimator_state(), Timestamp(15))


class WalltimeWatermarkEstimatorTest(unittest.TestCase):
  @mock.patch('apache_beam.utils.timestamp.Timestamp.now')
  def test_initialization(self, mock_timestamp):
    now_time = Timestamp.now() - Duration(10)
    mock_timestamp.side_effect = lambda: now_time
    watermark_estimator = WalltimeWatermarkEstimator()
    self.assertIsInstance(watermark_estimator, WatermarkEstimator)
    self.assertEqual(watermark_estimator.get_estimator_state(), now_time)

  def test_observe_timestamp(self):
    now_time = Timestamp.now() + Duration(10)
    watermark_estimator = WalltimeWatermarkEstimator(now_time)
    watermark_estimator.observe_timestamp(Timestamp(10))
    watermark_estimator.observe_timestamp(Timestamp(10))
    self.assertEqual(watermark_estimator.current_watermark(), now_time)

  def test_advance_watermark_with_incorrect_sys_clock(self):
    initial_timestamp = Timestamp.now() + Duration(100)
    watermark_estimator = WalltimeWatermarkEstimator(initial_timestamp)
    self.assertEqual(watermark_estimator.current_watermark(), initial_timestamp)
    self.assertEqual(
        watermark_estimator.get_estimator_state(), initial_timestamp)


class ManualWatermarkEstimatorTest(unittest.TestCase):
  def test_initialization(self):
    watermark_estimator = ManualWatermarkEstimator(None)
    self.assertIsNone(watermark_estimator.get_estimator_state())
    self.assertIsNone(watermark_estimator.current_watermark())
    watermark_estimator = ManualWatermarkEstimator(Timestamp(10))
    self.assertEqual(watermark_estimator.get_estimator_state(), Timestamp(10))

  def test_set_watermark(self):
    watermark_estimator = ManualWatermarkEstimator(None)
    self.assertIsNone(watermark_estimator.current_watermark())
    watermark_estimator.observe_timestamp(Timestamp(10))
    self.assertIsNone(watermark_estimator.current_watermark())
    watermark_estimator.set_watermark(Timestamp(20))
    self.assertEqual(watermark_estimator.current_watermark(), Timestamp(20))
    watermark_estimator.set_watermark(Timestamp(30))
    self.assertEqual(watermark_estimator.current_watermark(), Timestamp(30))
    with self.assertRaises(ValueError):
      watermark_estimator.set_watermark(Timestamp(25))


if __name__ == '__main__':
  unittest.main()
