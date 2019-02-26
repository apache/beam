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
from __future__ import division

import logging
import math
import random
import unittest
from builtins import object
from builtins import range

from apache_beam import coders
from apache_beam.runners.worker import opcounters
from apache_beam.runners.worker import statesampler
from apache_beam.runners.worker.opcounters import OperationCounters
from apache_beam.transforms.window import GlobalWindows
from apache_beam.utils import counters
from apache_beam.utils.counters import CounterFactory

# Classes to test that we can handle a variety of objects.
# These have to be at top level so the pickler can find them.


class OldClassThatDoesNotImplementLen(object):  # pylint: disable=old-style-class

  def __init__(self):
    pass


class ObjectThatDoesNotImplementLen(object):

  def __init__(self):
    pass


class TransformIoCounterTest(unittest.TestCase):

  def test_basic_counters(self):
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler('stage1', counter_factory)
    sampler.start()

    with sampler.scoped_state('step1', 'stateA'):
      counter = opcounters.SideInputReadCounter(counter_factory, sampler,
                                                declaring_step='step1',
                                                input_index=1)
    with sampler.scoped_state('step2', 'stateB'):
      with counter:
        counter.add_bytes_read(10)

      counter.update_current_step()

    sampler.stop()
    sampler.commit_counters()

    actual_counter_names = set([c.name for c in counter_factory.get_counters()])
    expected_counter_names = set([
        # Counter names for STEP 1
        counters.CounterName('read-sideinput-msecs',
                             stage_name='stage1',
                             step_name='step1',
                             io_target=counters.side_input_id('step1', 1)),
        counters.CounterName('read-sideinput-byte-count',
                             step_name='step1',
                             io_target=counters.side_input_id('step1', 1)),

        # Counter names for STEP 2
        counters.CounterName('read-sideinput-msecs',
                             stage_name='stage1',
                             step_name='step1',
                             io_target=counters.side_input_id('step2', 1)),
        counters.CounterName('read-sideinput-byte-count',
                             step_name='step1',
                             io_target=counters.side_input_id('step2', 1)),
    ])
    self.assertTrue(actual_counter_names.issuperset(expected_counter_names))


class OperationCountersTest(unittest.TestCase):

  def verify_counters(self, opcounts, expected_elements, expected_size=None):
    self.assertEqual(expected_elements, opcounts.element_counter.value())
    if expected_size is not None:
      if math.isnan(expected_size):
        self.assertTrue(math.isnan(opcounts.mean_byte_counter.value()))
      else:
        self.assertEqual(expected_size, opcounts.mean_byte_counter.value())

  def test_update_int(self):
    opcounts = OperationCounters(CounterFactory(), 'some-name',
                                 coders.PickleCoder(), 0)
    self.verify_counters(opcounts, 0)
    opcounts.update_from(GlobalWindows.windowed_value(1))
    opcounts.update_collect()
    self.verify_counters(opcounts, 1)

  def test_update_str(self):
    coder = coders.PickleCoder()
    opcounts = OperationCounters(CounterFactory(), 'some-name',
                                 coder, 0)
    self.verify_counters(opcounts, 0, float('nan'))
    value = GlobalWindows.windowed_value('abcde')
    opcounts.update_from(value)
    opcounts.update_collect()
    estimated_size = coder.estimate_size(value)
    self.verify_counters(opcounts, 1, estimated_size)

  def test_update_old_object(self):
    coder = coders.PickleCoder()
    opcounts = OperationCounters(CounterFactory(), 'some-name',
                                 coder, 0)
    self.verify_counters(opcounts, 0, float('nan'))
    obj = OldClassThatDoesNotImplementLen()
    value = GlobalWindows.windowed_value(obj)
    opcounts.update_from(value)
    opcounts.update_collect()
    estimated_size = coder.estimate_size(value)
    self.verify_counters(opcounts, 1, estimated_size)

  def test_update_new_object(self):
    coder = coders.PickleCoder()
    opcounts = OperationCounters(CounterFactory(), 'some-name',
                                 coder, 0)
    self.verify_counters(opcounts, 0, float('nan'))

    obj = ObjectThatDoesNotImplementLen()
    value = GlobalWindows.windowed_value(obj)
    opcounts.update_from(value)
    opcounts.update_collect()
    estimated_size = coder.estimate_size(value)
    self.verify_counters(opcounts, 1, estimated_size)

  def test_update_multiple(self):
    coder = coders.PickleCoder()
    total_size = 0
    opcounts = OperationCounters(CounterFactory(), 'some-name',
                                 coder, 0)
    self.verify_counters(opcounts, 0, float('nan'))
    value = GlobalWindows.windowed_value('abcde')
    opcounts.update_from(value)
    opcounts.update_collect()
    total_size += coder.estimate_size(value)
    value = GlobalWindows.windowed_value('defghij')
    opcounts.update_from(value)
    opcounts.update_collect()
    total_size += coder.estimate_size(value)
    self.verify_counters(opcounts, 2, (float(total_size) / 2))
    value = GlobalWindows.windowed_value('klmnop')
    opcounts.update_from(value)
    opcounts.update_collect()
    total_size += coder.estimate_size(value)
    self.verify_counters(opcounts, 3, (float(total_size) / 3))

  def test_should_sample(self):
    # Order of magnitude more buckets than highest constant in code under test.
    buckets = [0] * 300
    # The seed is arbitrary and exists just to ensure this test is robust.
    # If you don't like this seed, try your own; the test should still pass.
    random.seed(1720)
    # Do enough runs that the expected hits even in the last buckets
    # is big enough to expect some statistical smoothing.
    total_runs = 10 * len(buckets)

    # Fill the buckets.
    for _ in range(total_runs):
      opcounts = OperationCounters(CounterFactory(), 'some-name',
                                   coders.PickleCoder(), 0)
      for i in range(len(buckets)):
        if opcounts.should_sample():
          buckets[i] += 1

    # Look at the buckets to see if they are likely.
    for i in range(10):
      self.assertEqual(total_runs, buckets[i])
    for i in range(10, len(buckets)):
      self.assertTrue(buckets[i] > 7 * total_runs / i,
                      'i=%d, buckets[i]=%d, expected=%d, ratio=%f' % (
                          i, buckets[i],
                          10 * total_runs / i,
                          buckets[i] / (10.0 * total_runs / i)))
      self.assertTrue(buckets[i] < 14 * total_runs / i,
                      'i=%d, buckets[i]=%d, expected=%d, ratio=%f' % (
                          i, buckets[i],
                          10 * total_runs / i,
                          buckets[i] / (10.0 * total_runs / i)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
