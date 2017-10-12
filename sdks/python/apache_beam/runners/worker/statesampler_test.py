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

"""Tests for state sampler."""

import logging
import time
import unittest

from nose.plugins.skip import SkipTest

from apache_beam.utils.counters import CounterFactory


class StateSamplerTest(unittest.TestCase):

  def setUp(self):
    try:
      # pylint: disable=global-variable-not-assigned
      global statesampler
      import statesampler
    except ImportError:
      raise SkipTest('State sampler not compiled.')
    super(StateSamplerTest, self).setUp()

  def test_basic_sampler(self):
    # Set up state sampler.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler('basic', counter_factory,
                                        sampling_period_ms=1)

    # Run basic workload transitioning between 3 states.
    sampler.start()
    with sampler.scoped_state('statea'):
      time.sleep(0.1)
      with sampler.scoped_state('stateb'):
        time.sleep(0.2 / 2)
        with sampler.scoped_state('statec'):
          time.sleep(0.3)
        time.sleep(0.2 / 2)
    sampler.stop()
    sampler.commit_counters()

    # Test that sampled state timings are close to their expected values.
    expected_counter_values = {
        'basic-statea-msecs': 100,
        'basic-stateb-msecs': 200,
        'basic-statec-msecs': 300,
    }
    for counter in counter_factory.get_counters():
      self.assertIn(counter.name, expected_counter_values)
      expected_value = expected_counter_values[counter.name]
      actual_value = counter.value()
      self.assertGreater(actual_value, expected_value * 0.75)
      self.assertLess(actual_value, expected_value * 1.25)

  def test_sampler_transition_overhead(self):
    # Set up state sampler.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler('overhead-', counter_factory,
                                        sampling_period_ms=10)

    # Run basic workload transitioning between 3 states.
    state_a = sampler.scoped_state('statea')
    state_b = sampler.scoped_state('stateb')
    state_c = sampler.scoped_state('statec')
    start_time = time.time()
    sampler.start()
    for _ in range(100000):
      with state_a:
        with state_b:
          for _ in range(10):
            with state_c:
              pass
    sampler.stop()
    elapsed_time = time.time() - start_time
    state_transition_count = sampler.get_info().transition_count
    overhead_us = 1000000.0 * elapsed_time / state_transition_count
    logging.info('Overhead per transition: %fus', overhead_us)
    # Conservative upper bound on overhead in microseconds (we expect this to
    # take 0.17us when compiled in opt mode or 0.48 us when compiled with in
    # debug mode).
    self.assertLess(overhead_us, 10.0)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
