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

from __future__ import absolute_import

import unittest

from apache_beam.utils.counters import CounterName
from apache_beam.utils.counters import IOTargetName


class CounterNameTest(unittest.TestCase):

  def test_find_counter(self):
    c1 = object()
    cn1 = CounterName('cname1', 'stage1', 'step1')
    c2 = object()
    cn2 = CounterName('cname2', 'stage2', 'step2')

    index = {cn1: c1, cn2: c2}
    self.assertEqual(c1, index[cn1])
    self.assertEqual(c2, index[cn2])

  def test_iotarget_counter_names(self):
    self.assertEqual(CounterName('counter_name',
                                 'stage_name',
                                 'step_name',
                                 io_target=IOTargetName(1, 's9')),
                     CounterName('counter_name',
                                 'stage_name',
                                 'step_name',
                                 io_target=IOTargetName(1, 's9')))
    self.assertNotEqual(CounterName('counter_name',
                                    'stage_name',
                                    'step_name',
                                    io_target=IOTargetName(1, 's')),
                        CounterName('counter_name',
                                    'stage_name',
                                    'step_name',
                                    io_target=IOTargetName(1, 's9')))

  def test_equal_objects(self):
    self.assertEqual(CounterName('counter_name',
                                 'stage_name',
                                 'step_name'),
                     CounterName('counter_name',
                                 'stage_name',
                                 'step_name'))
    self.assertNotEqual(CounterName('counter_name',
                                    'stage_name',
                                    'step_name'),
                        CounterName('counter_name',
                                    'stage_name',
                                    'step_nam'))

  def test_hash_two_objects(self):
    self.assertEqual(hash(CounterName('counter_name',
                                      'stage_name',
                                      'step_name')),
                     hash(CounterName('counter_name',
                                      'stage_name',
                                      'step_name')))
    self.assertNotEqual(hash(CounterName('counter_name',
                                         'stage_name',
                                         'step_name')),
                        hash(CounterName('counter_name',
                                         'stage_name',
                                         'step_nam')))


if __name__ == '__main__':
  unittest.main()
