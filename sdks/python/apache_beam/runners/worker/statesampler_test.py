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
from __future__ import absolute_import

import logging
import time
import unittest

from apache_beam.runners.worker import statesampler
from apache_beam.utils.counters import CounterFactory
from apache_beam.utils.counters import CounterName


class StateSamplerTest(unittest.TestCase):

  def test_basic_sampler(self):
    # Set up state sampler.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler('basic', counter_factory,
                                        sampling_period_ms=1)

    # Run basic workload transitioning between 3 states.
    sampler.start()
    with sampler.scoped_state('step1', 'statea'):
      time.sleep(0.1)
      self.assertEqual(
          sampler.current_state().name,
          CounterName(
              'statea-msecs', step_name='step1', stage_name='basic'))
      with sampler.scoped_state('step1', 'stateb'):
        time.sleep(0.2 / 2)
        self.assertEqual(
            sampler.current_state().name,
            CounterName(
                'stateb-msecs', step_name='step1', stage_name='basic'))
        with sampler.scoped_state('step1', 'statec'):
          time.sleep(0.3)
          self.assertEqual(
              sampler.current_state().name,
              CounterName(
                  'statec-msecs', step_name='step1', stage_name='basic'))
        time.sleep(0.2 / 2)

    sampler.stop()
    sampler.commit_counters()

    if not statesampler.FAST_SAMPLER:
      # The slow sampler does not implement sampling, so we won't test it.
      return

    # Test that sampled state timings are close to their expected values.
    expected_counter_values = {
        CounterName('statea-msecs', step_name='step1', stage_name='basic'): 100,
        CounterName('stateb-msecs', step_name='step1', stage_name='basic'): 200,
        CounterName('statec-msecs', step_name='step1', stage_name='basic'): 300,
    }
    for counter in counter_factory.get_counters():
      self.assertIn(counter.name, expected_counter_values)
      expected_value = expected_counter_values[counter.name]
      actual_value = counter.value()
      deviation = float(abs(actual_value - expected_value)) / expected_value
      logging.info('Sampling deviation from expectation: %f', deviation)
      self.assertGreater(actual_value, expected_value * 0.75)
      self.assertLess(actual_value, expected_value * 1.25)

  def test_sampler_transition_overhead(self):
    # Set up state sampler.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler('overhead-', counter_factory,
                                        sampling_period_ms=10)

    # Run basic workload transitioning between 3 states.
    state_a = sampler.scoped_state('step1', 'statea')
    state_b = sampler.scoped_state('step1', 'stateb')
    state_c = sampler.scoped_state('step1', 'statec')
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
