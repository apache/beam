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
import unittest

import apache_beam as beam
from apache_beam.ml.ts.util import PeriodicStream
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import Timestamp


class PeriodicStreamTest(unittest.TestCase):
  def test_interval(self):
    options = PipelineOptions()
    start = Timestamp.now()
    with beam.Pipeline(options=options) as p:
      ret = (
          p | PeriodicStream([1, 2, 3, 4], interval=0.5)
          | beam.WindowInto(FixedWindows(0.5))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [(0, [1]), (0, [2]), (0, [3]), (0, [4])]
      assert_that(ret, equal_to(expected))
    end = Timestamp.now()
    self.assertGreaterEqual(end - start, 3)
    self.assertLessEqual(end - start, 7)

  def test_repeat(self):
    options = PipelineOptions()
    start = Timestamp.now()
    with beam.Pipeline(options=options) as p:
      ret = (
          p | PeriodicStream(
              [1, 2, 3, 4], interval=0.5, max_duration=3, repeat=True)
          | beam.WindowInto(FixedWindows(0.5))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [(0, [1]), (0, [2]), (0, [3]), (0, [4]), (0, [1]), (0, [2])]
      assert_that(ret, equal_to(expected))
    end = Timestamp.now()
    self.assertGreaterEqual(end - start, 3)
    self.assertLessEqual(end - start, 7)

  def test_timestamped_value(self):
    options = PipelineOptions()
    start = Timestamp.now()
    with beam.Pipeline(options=options) as p:
      ret = (
          p | PeriodicStream([(Timestamp(1), 1), (Timestamp(3), 2),
                              (Timestamp(2), 3), (Timestamp(1), 4)],
                             interval=0.5)
          | beam.WindowInto(FixedWindows(0.5))
          | beam.WithKeys(0)
          | beam.GroupByKey())
      expected = [(0, [1, 4]), (0, [2]), (0, [3])]
      assert_that(ret, equal_to(expected))
    end = Timestamp.now()
    self.assertGreaterEqual(end - start, 3)
    self.assertLessEqual(end - start, 7)

  def test_stable_output(self):
    options = PipelineOptions()
    data = [(Timestamp(1), 1), (Timestamp(2), 2), (Timestamp(3), 3),
            (Timestamp(6), 6), (Timestamp(4), 4), (Timestamp(5), 5),
            (Timestamp(7), 7), (Timestamp(8), 8), (Timestamp(9), 9),
            (Timestamp(10), 10)]
    expected = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    with beam.Pipeline(options=options) as p:
      ret = (p | PeriodicStream(data, interval=0.0001))
      assert_that(ret, equal_to(expected))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.WARNING)
  unittest.main()
