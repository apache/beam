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

import inspect
import time
import unittest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.periodicsequence import PeriodicSequence

# Disable frequent lint warning due to pipe operator for chaining transforms.
# pylint: disable=expression-not-assigned


class PeriodicSequenceTest(unittest.TestCase):
  def test_periodicsequence_outputs_valid_sequence(self):
    start_offset = 1
    start_time = time.time() + start_offset
    duration = 1
    end_time = start_time + duration
    interval = 0.25

    with TestPipeline() as p:
      result = (
          p
          | 'ImpulseElement' >> beam.Create([(start_time, end_time, interval)])
          | 'ImpulseSeqGen' >> PeriodicSequence())

      k = [
          start_time + x * interval
          for x in range(0, int(duration / interval), 1)
      ]

      assert_that(result, equal_to(k))

  def test_periodicimpulse_windowing_on_si(self):
    start_offset = -15
    it = time.time() + start_offset
    duration = 15
    et = it + duration
    interval = 5

    with TestPipeline() as p:
      si = (
          p
          | 'PeriodicImpulse' >> PeriodicImpulse(it, et, interval, True)
          | 'AddKey' >> beam.Map(lambda v: ('key', v))
          | 'GBK' >> beam.GroupByKey()
          | 'SortGBK' >> beam.MapTuple(lambda k, vs: (k, sorted(vs))))

      actual = si
      k = [('key', [it + x * interval])
           for x in range(0, int(duration / interval), 1)]
      assert_that(actual, equal_to(k))

  def test_periodicimpulse_default_start(self):
    default_parameters = inspect.signature(PeriodicImpulse).parameters
    it = default_parameters["start_timestamp"].default
    duration = 1
    et = it + duration
    interval = 0.5

    # Check default `stop_timestamp` is the same type `start_timestamp`
    is_same_type = isinstance(
        it, type(default_parameters["stop_timestamp"].default))
    error = "'start_timestamp' and 'stop_timestamp' have different type"
    assert is_same_type, error

    with TestPipeline() as p:
      result = p | 'PeriodicImpulse' >> PeriodicImpulse(it, et, interval)

      k = [it + x * interval for x in range(0, int(duration / interval))]
      assert_that(result, equal_to(k))

  def test_periodicsequence_outputs_valid_sequence_in_past(self):
    start_offset = -10000
    it = time.time() + start_offset
    duration = 5
    et = it + duration
    interval = 1

    with TestPipeline() as p:
      result = (
          p
          | 'ImpulseElement' >> beam.Create([(it, et, interval)])
          | 'ImpulseSeqGen' >> PeriodicSequence())

      k = [it + x * interval for x in range(0, int(duration / interval), 1)]
      assert_that(result, equal_to(k))


if __name__ == '__main__':
  unittest.main()
