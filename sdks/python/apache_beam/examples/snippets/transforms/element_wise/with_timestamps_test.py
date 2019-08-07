# coding=utf-8
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

from __future__ import absolute_import
from __future__ import print_function

import unittest

import mock

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from . import with_timestamps


def check_plant_timestamps(actual):
  # [START plant_timestamps]
  plant_timestamps = [
      '2020-04-01 00:00:00 - Strawberry',
      '2020-06-01 00:00:00 - Carrot',
      '2020-03-01 00:00:00 - Artichoke',
      '2020-05-01 00:00:00 - Tomato',
      '2020-09-01 00:00:00 - Potato',
  ]
  # [END plant_timestamps]
  assert_that(actual, equal_to(plant_timestamps))


def check_plant_events(actual):
  # [START plant_events]
  plant_events = [
      '1 - Strawberry',
      '4 - Carrot',
      '2 - Artichoke',
      '3 - Tomato',
      '5 - Potato',
  ]
  # [END plant_events]
  assert_that(actual, equal_to(plant_events))


def check_plant_processing_times(actual):
  import apache_beam as beam

  # [START plant_processing_times]
  plant_processing_times = [
      '2020-03-20 20:12:42.145594 - Strawberry',
      '2020-03-20 20:12:42.145827 - Carrot',
      '2020-03-20 20:12:42.145962 - Artichoke',
      '2020-03-20 20:12:42.146093 - Tomato',
      '2020-03-20 20:12:42.146216 - Potato',
  ]
  # [END plant_processing_times]

  # Since `time.time()` will always give something different, we'll
  # simply strip the timestamp information before testing the results.
  actual = actual | beam.Map(lambda row: row.split('-')[-1].strip())
  expected = [row.split('-')[-1].strip() for row in plant_processing_times]
  assert_that(actual, equal_to(expected))


@mock.patch('apache_beam.Pipeline', TestPipeline)
# pylint: disable=line-too-long
@mock.patch('apache_beam.examples.snippets.transforms.element_wise.with_timestamps.print', lambda elem: elem)
# pylint: enable=line-too-long
class WithTimestampsTest(unittest.TestCase):
  def test_event_time(self):
    with_timestamps.event_time(check_plant_timestamps)

  def test_logical_clock(self):
    with_timestamps.logical_clock(check_plant_events)

  def test_processing_time(self):
    with_timestamps.processing_time(check_plant_processing_times)

  def test_time_tuple2unix_time(self):
    unix_time = with_timestamps.time_tuple2unix_time()
    self.assertIsInstance(unix_time, float)

  def test_datetime2unix_time(self):
    unix_time = with_timestamps.datetime2unix_time()
    self.assertIsInstance(unix_time, float)


if __name__ == '__main__':
  unittest.main()
