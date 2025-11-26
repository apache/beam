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
from unittest import mock
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
from apache_beam.transforms import userstate
from apache_beam.transforms.core import GlobalWindows
from apache_beam.transforms.core import Windowing
from apache_beam.transforms.window import GlobalWindow
from apache_beam.utils.counters import CounterFactory
from apache_beam.utils.counters import CounterName
from apache_beam.utils.windowed_value import PaneInfo

_LOGGER = logging.getLogger(__name__)


class TimerDoFn(core.DoFn):
  TIMER_SPEC = userstate.TimerSpec('timer', userstate.TimeDomain.WATERMARK)

  def __init__(self, sleep_duration_s=0):
    self._sleep_duration_s = sleep_duration_s

  @userstate.on_timer(TIMER_SPEC)
  def on_timer_f(self):
    if self._sleep_duration_s:
      time.sleep(self._sleep_duration_s)


class ExceptionTimerDoFn(core.DoFn):
  """A DoFn that raises an exception when its timer fires."""
  TIMER_SPEC = userstate.TimerSpec('ts-timer', userstate.TimeDomain.WATERMARK)

  def __init__(self, sleep_duration_s=0):
    self._sleep_duration_s = sleep_duration_s

  @userstate.on_timer(TIMER_SPEC)
  def on_timer_f(self):
    if self._sleep_duration_s:
      time.sleep(self._sleep_duration_s)
    raise RuntimeError("Test exception from timer")


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
  # Patch the problematic function to return the correct timer spec
  @patch('apache_beam.transforms.userstate.get_dofn_specs')
  def test_do_operation_process_timer(self, mock_get_dofn_specs):
    fn = TimerDoFn()
    mock_get_dofn_specs.return_value = ([], [fn.TIMER_SPEC])

    if not statesampler.FAST_SAMPLER:
      self.skipTest('DoOperation test requires FAST_SAMPLER')

    state_duration_ms = 200
    margin_of_error = 0.75

    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'test_do_op', counter_factory, sampling_period_ms=1)

    fn_for_spec = TimerDoFn(sleep_duration_s=state_duration_ms / 1000.0)

    spec = operation_specs.WorkerDoFn(
        serialized_fn=pickler.dumps(
            (fn_for_spec, [], {}, [], Windowing(GlobalWindows()))),
        output_tags=[],
        input=None,
        side_inputs=[],
        output_coders=[])

    mock_user_state_context = mock.MagicMock()
    op = operations.DoOperation(
        common.NameContext('step1'),
        spec,
        counter_factory,
        sampler,
        user_state_context=mock_user_state_context)

    op.setup()

    timer_data = Mock()
    timer_data.user_key = None
    timer_data.windows = [GlobalWindow()]
    timer_data.fire_timestamp = 0
    timer_data.paneinfo = PaneInfo(
        is_first=False,
        is_last=False,
        timing=0,
        index=0,
        nonspeculative_index=0)
    timer_data.dynamic_timer_tag = ''

    sampler.start()
    op.process_timer('ts-timer', timer_data=timer_data)
    sampler.stop()
    sampler.commit_counters()

    expected_name = CounterName(
        'process-timers-msecs', step_name='step1', stage_name='test_do_op')

    found_counter = None
    for counter in counter_factory.get_counters():
      if counter.name == expected_name:
        found_counter = counter
        break

    self.assertIsNotNone(
        found_counter, f"Expected counter '{expected_name}' to be created.")

    actual_value = found_counter.value()
    logging.info("Actual value %d", actual_value)
    self.assertGreater(
        actual_value, state_duration_ms * (1.0 - margin_of_error))

  @retry(reraise=True, stop=stop_after_attempt(3))
  @patch('apache_beam.runners.worker.operations.userstate.get_dofn_specs')
  def test_do_operation_process_timer_with_exception(self, mock_get_dofn_specs):
    fn = ExceptionTimerDoFn()
    mock_get_dofn_specs.return_value = ([], [fn.TIMER_SPEC])

    if not statesampler.FAST_SAMPLER:
      self.skipTest('DoOperation test requires FAST_SAMPLER')

    state_duration_ms = 200
    margin_of_error = 0.50

    counter_factory = CounterFactory()
    sampler = statesampler.StateSampler(
        'test_do_op_exception', counter_factory, sampling_period_ms=1)

    fn_for_spec = ExceptionTimerDoFn(
        sleep_duration_s=state_duration_ms / 1000.0)

    spec = operation_specs.WorkerDoFn(
        serialized_fn=pickler.dumps(
            (fn_for_spec, [], {}, [], Windowing(GlobalWindows()))),
        output_tags=[],
        input=None,
        side_inputs=[],
        output_coders=[])

    mock_user_state_context = mock.MagicMock()
    op = operations.DoOperation(
        common.NameContext('step1'),
        spec,
        counter_factory,
        sampler,
        user_state_context=mock_user_state_context)

    op.setup()

    timer_data = Mock()
    timer_data.user_key = None
    timer_data.windows = [GlobalWindow()]
    timer_data.fire_timestamp = 0
    timer_data.paneinfo = PaneInfo(
        is_first=False,
        is_last=False,
        timing=0,
        index=0,
        nonspeculative_index=0)
    timer_data.dynamic_timer_tag = ''

    sampler.start()
    # Assert that the expected exception is raised
    with self.assertRaises(RuntimeError):
      op.process_timer('ts-ts-timer', timer_data=timer_data)
    sampler.stop()
    sampler.commit_counters()

    expected_name = CounterName(
        'process-timers-msecs',
        step_name='step1',
        stage_name='test_do_op_exception')

    found_counter = None
    for counter in counter_factory.get_counters():
      if counter.name == expected_name:
        found_counter = counter
        break

    self.assertIsNotNone(
        found_counter, f"Expected counter '{expected_name}' to be created.")

    actual_value = found_counter.value()
    self.assertGreater(
        actual_value, state_duration_ms * (1.0 - margin_of_error))
    _LOGGER.info("Exception test finished successfully.")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
