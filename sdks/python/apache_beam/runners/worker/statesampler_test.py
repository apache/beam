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
from unittest.mock import Mock
from unittest.mock import patch

from tenacity import retry
from tenacity import stop_after_attempt

from apache_beam.internal import pickler
from apache_beam.runners import common
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker import operations
from apache_beam.runners.worker import statesampler
from apache_beam.transforms import core
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
  def test_process_timers_metric_is_recorded(self):
    """
    Tests that the 'process-timers-msecs' metric is correctly recorded
    when a state sampler is active.
    """
    # Set up a real state sampler and counter factory.
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'test_stage', counter_factory, sampling_period_ms=1)

    # Keeps range between 50-350 ms, which is fair.
    state_duration_ms = 200
    margin_of_error = 0.75

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
        'process-timers-msecs', step_name='test_step', stage_name='test_stage')

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

  @patch('apache_beam.runners.common.DoFnRunner')
  def test_do_operation_process_timer_metric(self, mock_dofn_runner_class):
    """
    Tests that the 'process-timers-msecs' metric is correctly recorded
    when a timer is processed within a DoOperation.
    """
    mock_dofn_runner_instance = mock_dofn_runner_class.return_value
    state_duration_ms = 200

    def mock_process_user_timer(*args, **kwargs):
      time.sleep(state_duration_ms / 1000.0)

    mock_dofn_runner_instance.process_user_timer = mock_process_user_timer
    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'test_stage', counter_factory, sampling_period_ms=1)

    mock_spec = operation_specs.WorkerDoFn(
        serialized_fn=pickler.dumps((core.DoFn(), None, None, None, None)),
        output_tags=[],
        input=None,
        side_inputs=[],
        output_coders=[])

    op = operations.DoOperation(
        name=common.NameContext('test_op'),
        spec=mock_spec,
        counter_factory=counter_factory,
        sampler=sampler)

    op.setup()
    op.dofn_runner = Mock()
    op.timer_specs = {'timer_id': Mock()}
    margin_of_error = 0.75

    def mock_process_timer_usage(*args, **kwargs):
      time.sleep(state_duration_ms / 1000.0)

    op.dofn_runner.process_user_timer = mock_process_timer_usage

    mock_timer_data = Mock()
    mock_timer_data.windows = [Mock()]
    mock_timer_data.user_key = Mock()
    mock_timer_data.fire_timestamp = Mock()
    mock_timer_data.paneinfo = Mock()
    mock_timer_data.dynamic_timer_tag = Mock()

    sampler.start()
    op.process_timer('timer_id', mock_timer_data)
    sampler.stop()
    sampler.commit_counters()

    if not statesampler.FAST_SAMPLER:
      return

    expected_counter_name = CounterName(
        'process-timers-msecs', step_name='test_op', stage_name='test_stage')

    found_counter = None
    for counter in counter_factory.get_counters():
      if counter.name == expected_counter_name:
        found_counter = counter
        break

    self.assertIsNotNone(
        found_counter,
        f"The expected counter '{expected_counter_name}' was not created.")

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

  @patch('apache_beam.runners.common.DoFnRunner')
  def test_do_operation_process_timer_metric_with_exception(
      self, mock_dofn_runner_class):
    """
    Tests that the 'process-timers-msecs' metric is still recorded
    when a timer callback in a DoOperation raises an exception.
    """
    # Configure the mock instance to raise an exception
    mock_dofn_runner_instance = mock_dofn_runner_class.return_value
    state_duration_ms = 200

    def mock_process_user_timer(*args, **kwargs):
      time.sleep(state_duration_ms / 1000.0)
      raise ValueError("Test Exception")

    mock_dofn_runner_instance.process_user_timer = mock_process_user_timer

    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'test_stage', counter_factory, sampling_period_ms=1)

    mock_spec = operation_specs.WorkerDoFn(
        serialized_fn=pickler.dumps((core.DoFn(), None, None, None, None)),
        output_tags=[],
        input=None,
        side_inputs=[],
        output_coders=[])

    op = operations.DoOperation(
        name=common.NameContext('test_op'),
        spec=mock_spec,
        counter_factory=counter_factory,
        sampler=sampler,
        # Provide a mock context to satisfy the setup method
        user_state_context=Mock())

    # The setup call now succeeds because DoFnRunner is mocked
    op.setup()

    op.timer_specs = {'timer_id': Mock()}
    margin_of_error = 0.75

    mock_timer_data = Mock()
    mock_timer_data.windows = [Mock()]
    mock_timer_data.user_key = Mock()
    mock_timer_data.fire_timestamp = Mock()
    mock_timer_data.paneinfo = Mock()
    mock_timer_data.dynamic_timer_tag = Mock()

    sampler.start()
    # The test correctly asserts that a ValueError is raised
    with self.assertRaises(ValueError):
      op.process_timer('timer_id', mock_timer_data)
    sampler.stop()
    sampler.commit_counters()

    if not statesampler.FAST_SAMPLER:
      return

    expected_counter_name = CounterName(
        'process-timers-msecs', step_name='test_op', stage_name='test_stage')

    found_counter = None
    for counter in counter_factory.get_counters():
      if counter.name == expected_counter_name:
        found_counter = counter
        break

    self.assertIsNotNone(
        found_counter,
        f"The expected counter '{expected_counter_name}' was not created.")

    # Assert that the timer metric was still recorded despite the exception
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
