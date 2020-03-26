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

"""Unit tests for the PTransform and descendants."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import tempfile
import time
import unittest
from builtins import range

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.heartbeat import Heartbeat
from apache_beam.transforms.heartbeat import HeartbeatImpulse

# Disable frequent lint warning due to pipe operator for chaining transforms.
# pylint: disable=expression-not-assigned


class HeartbeatTest(unittest.TestCase):
  # Enable nose tests running in parallel

  def test_heartbeat_outputs_valid_sequence(self):
    start_offset = 1
    it = time.time() + start_offset
    duration = 3
    et = it + duration
    interval = 1

    with TestPipeline() as p:
      result = (
          p
          | 'ImpulseElement' >> beam.Create([(it, et, interval)])
          | 'ImpulseSeqGen' >> Heartbeat())

      k = [it + x * interval for x in range(0, int(duration / interval), 1)]
      assert_that(result, equal_to(k))

  def test_heartbeat_windowing_on_si(self):
    start_offset = -15
    it = time.time() + start_offset
    duration = 15
    et = it + duration
    interval = 5

    with TestPipeline() as p:
      si = (
          p
          | 'Heartbeat' >> HeartbeatImpulse(it, et, interval, True)
          | 'AddKey' >> beam.Map(lambda v: ('key', v))
          | 'GBK' >> beam.GroupByKey()
          | 'SortGBK' >> beam.MapTuple(lambda k, vs: (k, sorted(vs))))

      actual = si
      k = [('key', [it + x * interval])
           for x in range(0, int(duration / interval), 1)]
      assert_that(actual, equal_to(k))

  def test_multiple_reads(self):
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    file_pattern = temp_file.name
    temp_file.write(b'a\nb\nc\n')
    temp_file.close()

    start_offset = -15
    it = time.time() + start_offset
    duration = 15
    et = it + duration
    interval = 1

    with TestPipeline() as p:
      si = (
          p
          | 'Heartbeat' >> HeartbeatImpulse(it, et, interval, True)
          | 'MapToFileName' >> beam.Map(lambda x: file_pattern)
          | 'ReadFromFile' >> beam.io.ReadAllFromText()
          | 'AddKey' >> beam.Map(lambda v: ('key', v))
          | 'GBK' >> beam.GroupByKey()
          | 'SortGBK' >> beam.MapTuple(lambda k, vs: (k, sorted(vs))))

      actual = si
      k = [('key', ['a', 'b', 'c'])
           for x in range(0, int(duration / interval), 1)]
      assert_that(actual, equal_to(k))
    os.unlink(file_pattern)

  def test_heartbeat_outputs_valid_sequence_in_past(self):
    start_offset = -1200
    it = time.time() + start_offset
    duration = 5
    et = it + duration
    interval = 1

    with TestPipeline() as p:
      result = (
          p
          | 'ImpulseElement' >> beam.Create([(it, et, interval)])
          | 'ImpulseSeqGen' >> Heartbeat())

      k = [it + x * interval for x in range(0, int(duration / interval), 1)]
      assert_that(result, equal_to(k))


if __name__ == '__main__':
  unittest.main()
