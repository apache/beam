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
# pytype: skip-file

import logging
import time
import unittest

from tenacity import retry
from tenacity import stop_after_attempt

from apache_beam.runners.worker import statesampler
from apache_beam.utils.counters import CounterFactory
from apache_beam.utils.counters import CounterName

_LOGGER = logging.getLogger(__name__)


class StateSamplerTest(unittest.TestCase):

  # Due to somewhat non-deterministic nature of state sampling and sleep,
  # this test is flaky when state duration is low.
  # Since increasing state duration significantly would also slow down
  # the test suite, we are retrying twice on failure as a mitigation.
  @retry(reraise=True, stop=stop_after_attempt(3))
  def test_basic_sampler(self):
    # Set up state sampler.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'basic', counter_factory, sampling_period_ms=1)

    # Duration of the fastest state. Total test duration is 6 times longer.
    state_duration_ms = 1000
    margin_of_error = 0.25
    # Run basic workload transitioning between 3 states.
    sampler.start()
    with sampler.scoped_state('step1', 'statea'):
      time.sleep(state_duration_ms / 1000)
      self.assertEqual(
          sampler.current_state().name,
          CounterName('statea-msecs', step_name='step1', stage_name='basic'))
      with sampler.scoped_state('step1', 'stateb'):
        time.sleep(state_duration_ms / 1000)
        self.assertEqual(
            sampler.current_state().name,
            CounterName('stateb-msecs', step_name='step1', stage_name='basic'))
        with sampler.scoped_state('step1', 'statec'):
          time.sleep(3 * state_duration_ms / 1000)
          self.assertEqual(
              sampler.current_state().name,
              CounterName(
                  'statec-msecs', step_name='step1', stage_name='basic'))
        time.sleep(state_duration_ms / 1000)

    sampler.stop()
    sampler.commit_counters()

    if not statesampler.FAST_SAMPLER:
      # The slow sampler does not implement sampling, so we won't test it.
      return

    # Test that sampled state timings are close to their expected values.
    # yapf: disable
    expected_counter_values = {
        CounterName('statea-msecs', step_name='step1', stage_name='basic'):
            state_duration_ms,
        CounterName('stateb-msecs', step_name='step1', stage_name='basic'): 2 *
        state_duration_ms,
        CounterName('statec-msecs', step_name='step1', stage_name='basic'): 3 *
        state_duration_ms,
    }
    # yapf: enable
    for counter in counter_factory.get_counters():
      self.assertIn(counter.name, expected_counter_values)
      expected_value = expected_counter_values[counter.name]
      actual_value = counter.value()
      deviation = float(abs(actual_value - expected_value)) / expected_value
      _LOGGER.info('Sampling deviation from expectation: %f', deviation)
      self.assertGreater(actual_value, expected_value * (1.0 - margin_of_error))
      self.assertLess(actual_value, expected_value * (1.0 + margin_of_error))

  # TODO: This test is flaky when it is run under load. A better solution
  # would be to change the test structure to not depend on specific timings.
  @retry(reraise=True, stop=stop_after_attempt(3))
  def test_sampler_transition_overhead(self):
    # Set up state sampler.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'overhead-', counter_factory, sampling_period_ms=10)

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

    _LOGGER.info('Overhead per transition: %fus', overhead_us)
    # Conservative upper bound on overhead in microseconds (we expect this to
    # take 0.17us when compiled in opt mode or 0.48 us when compiled with in
    # debug mode).
    self.assertLess(overhead_us, 20.0)

  @retry(reraise=True, stop=stop_after_attempt(3))
  def test_timer_sampler(self):
    # Set up state sampler.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'timer', counter_factory, sampling_period_ms=1)

    # Duration of the timer processing.
    state_duration_ms = 100
    margin_of_error = 0.25

    sampler.start()
    with sampler.scoped_state('step1', 'process-timers'):
      time.sleep(state_duration_ms / 1000)
    sampler.stop()
    sampler.commit_counters()

    if not statesampler.FAST_SAMPLER:
      # The slow sampler does not implement sampling, so we won't test it.
      return

    # Test that sampled state timings are close to their expected values.
    expected_counter_values = {
        CounterName(
            'process-timers-msecs', step_name='step1', stage_name='timer'): state_duration_ms,
    }
    for counter in counter_factory.get_counters():
      self.assertIn(counter.name, expected_counter_values)
      expected_value = expected_counter_values[counter.name]
      actual_value = counter.value()
      deviation = float(abs(actual_value - expected_value)) / expected_value
      _LOGGER.info('Sampling deviation from expectation: %f', deviation)
      self.assertGreater(actual_value, expected_value * (1.0 - margin_of_error))
      self.assertLess(actual_value, expected_value * (1.0 + margin_of_error))

  @retry(reraise=True, stop=stop_after_attempt(3))
  def test_process_timers_metric_is_recorded(self):
    """
    Tests that the 'process-timers-msecs' metric is correctly recorded
    when a state sampler is active.
    """
    # Set up a real state sampler and counter factory.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'test_stage', counter_factory, sampling_period_ms=1)

    state_duration_ms = 100
    margin_of_error = 0.25

    # Run a workload inside the 'process-timers' scoped state.
    sampler.start()
    with sampler.scoped_state('test_step', 'process-timers'):
      time.sleep(state_duration_ms / 1000.0)
    sampler.stop()
    sampler.commit_counters()

    if not statesampler.FAST_SAMPLER:
      return

    # Verify that the counter was created with the correct name and value.
    expected_counter_name = CounterName(
        'process-timers-msecs',
        step_name='test_step',
        stage_name='test_stage')

    # Find the specific counter we are looking for.
    found_counter = None
    for counter in counter_factory.get_counters():
      if counter.name == expected_counter_name:
        found_counter = counter
        break

    self.assertIsNotNone(
        found_counter,
        f"The expected counter '{expected_counter_name}' was not created.")

    # Check that its value is approximately correct.
    actual_value = found_counter.value()
    expected_value = state_duration_ms
    self.assertGreater(
        actual_value,
        expected_value * (1.0 - margin_of_error),
        "The timer metric was lower than expected.")
    self.assertLess(
        actual_value,
        expected_value * (1.0 + margin_of_error),
        "The timer metric was higher than expected.")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
