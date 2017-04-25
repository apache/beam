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

"""Unit tests for the test_stream module."""

import unittest

from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils import timestamp
from apache_beam.utils.test_stream import ElementEvent
from apache_beam.utils.test_stream import ProcessingTimeEvent
from apache_beam.utils.test_stream import TestStream
from apache_beam.utils.test_stream import WatermarkEvent
from apache_beam.utils.windowed_value import WindowedValue


class TestStreamTest(unittest.TestCase):

  def test_basic_test_stream(self):
    test_stream = (TestStream()
                   .advance_watermark_to(0)
                   .add_elements([
                       'a',
                       WindowedValue('b', 3, []),
                       TimestampedValue('c', 6)])
                   .advance_processing_time(10)
                   .advance_watermark_to(8)
                   .add_elements(['d'])
                   .advance_watermark_to_infinity())
    self.assertEqual(
        test_stream.events,
        [
            WatermarkEvent(0),
            ElementEvent([
                TimestampedValue('a', 0),
                TimestampedValue('b', 3),
                TimestampedValue('c', 6),
            ]),
            ProcessingTimeEvent(10),
            WatermarkEvent(8),
            ElementEvent([
                TimestampedValue('d', 8),
            ]),
            WatermarkEvent(timestamp.MAX_TIMESTAMP),
        ]
    )

  def test_test_stream_errors(self):
    with self.assertRaises(AssertionError, msg=(
        'Watermark must strictly-monotonically advance.')):
      _ = (TestStream()
           .advance_watermark_to(5)
           .advance_watermark_to(4))

    with self.assertRaises(AssertionError, msg=(
        'Must advance processing time by positive amount.')):
      _ = (TestStream()
           .advance_processing_time(-1))

    with self.assertRaises(AssertionError, msg=(
        'Element timestamp must be before timestamp.MAX_TIMESTAMP.')):
      _ = (TestStream()
           .add_elements([
               TimestampedValue('a', timestamp.MAX_TIMESTAMP)
           ]))


if __name__ == '__main__':
  unittest.main()
