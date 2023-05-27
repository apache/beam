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

"""Unit tests for our libraries of combine PTransforms."""
# pytype: skip-file

import time
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import window
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.utils.timestamp import Timestamp


class CombineGloballyTest(unittest.TestCase):
  def test_with_fixed_windows(self):
    with TestPipeline() as p:
      input = (
          p
          | beam.Create([
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707510)),
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707511)),
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707512)),
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707513)),
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707515)),
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707516)),
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707517)),
              window.TimestampedValue(("c", 1), Timestamp(seconds=1666707518))
          ])
          | beam.WindowInto(window.FixedWindows(4))
          | "Print Windows" >> beam.transforms.util.LogElements(
              with_timestamp=True, with_window=True))

      window_sum = input | beam.Values() | beam.CombineGlobally(
          sum).without_defaults()
      _ = window_sum | "Print Window Sum" >> beam.transforms.util.LogElements(
          with_timestamp=True, with_window=True)

  def test_with_periodic_impulse(self):
    with TestPipeline() as p:
      input = (
          p
          | PeriodicImpulse(
              start_timestamp=time.time(),
              stop_timestamp=time.time() + 16,
              fire_interval=1,
              apply_windowing=False,
          )
          | beam.Map(lambda x: ('c', 1))
          | beam.WindowInto(window.FixedWindows(4))
          | "Print Windows" >> beam.transforms.util.LogElements(
              with_timestamp=True, with_window=True))

      window_sum = input | beam.Values() | beam.CombineGlobally(
          sum).without_defaults()
      _ = window_sum | "Print Window Sum" >> beam.transforms.util.LogElements(
          with_timestamp=True, with_window=True)


if __name__ == '__main__':
  unittest.main()
