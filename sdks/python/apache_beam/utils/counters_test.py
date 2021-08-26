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

"""Unit tests for counters and counter names."""

# pytype: skip-file

import unittest

from apache_beam.utils import counters
from apache_beam.utils.counters import CounterName


class CounterNameTest(unittest.TestCase):
  def test_name_string_representation(self):
    counter_name = CounterName('counter_name', 'stage_name', 'step_name')

    # This string representation is utilized by the worker to report progress.
    # Change only if the worker code has also been changed.
    self.assertEqual('stage_name-step_name-counter_name', str(counter_name))
    self.assertIn(
        '<CounterName<stage_name-step_name-counter_name> at 0x',
        repr(counter_name))

  def test_equal_objects(self):
    self.assertEqual(
        CounterName('counter_name', 'stage_name', 'step_name'),
        CounterName('counter_name', 'stage_name', 'step_name'))
    self.assertNotEqual(
        CounterName('counter_name', 'stage_name', 'step_name'),
        CounterName('counter_name', 'stage_name', 'step_nam'))

    # Testing objects with an IOTarget.
    self.assertEqual(
        CounterName(
            'counter_name',
            'stage_name',
            'step_name',
            io_target=counters.side_input_id(1, 's9')),
        CounterName(
            'counter_name',
            'stage_name',
            'step_name',
            io_target=counters.side_input_id(1, 's9')))
    self.assertNotEqual(
        CounterName(
            'counter_name',
            'stage_name',
            'step_name',
            io_target=counters.side_input_id(1, 's')),
        CounterName(
            'counter_name',
            'stage_name',
            'step_name',
            io_target=counters.side_input_id(1, 's9')))

  def test_hash_two_objects(self):
    self.assertEqual(
        hash(CounterName('counter_name', 'stage_name', 'step_name')),
        hash(CounterName('counter_name', 'stage_name', 'step_name')))
    self.assertNotEqual(
        hash(CounterName('counter_name', 'stage_name', 'step_name')),
        hash(CounterName('counter_name', 'stage_name', 'step_nam')))


if __name__ == '__main__':
  unittest.main()
