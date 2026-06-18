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

"""Tests for the Watch transform."""

import collections
import unittest

import apache_beam as beam
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import Watch
from apache_beam.io.watch import _GrowthRestrictionTracker
from apache_beam.io.watch import _GrowthStateCoder
from apache_beam.io.watch import _NonPollingGrowthState
from apache_beam.io.watch import _plan_poll_round
from apache_beam.io.watch import _PollingGrowthState
from apache_beam.io.watch import _replay_plan
from apache_beam.io.watch import after_total_of
from apache_beam.io.watch import never
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.runners.sdf_utils import ThreadsafeRestrictionTracker
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import TestWindowedValue
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import GlobalWindow
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints import typehints
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Duration
from apache_beam.utils.timestamp import Timestamp


def _ts(value, timestamp):
  return TimestampedValue(value, Timestamp(timestamp))


def _plan(restriction, poll_fn, now=0.0, termination=None):
  termination = termination or never()
  return _plan_poll_round(
      restriction,
      poll_fn('input'),
      termination,
      StrUtf8Coder(),
      Timestamp.of(now))


def _initial_polling(termination=None, now=Timestamp(0)):
  termination = termination or never()
  return _PollingGrowthState(
      collections.OrderedDict(), None, termination.for_new_input(now, 'input'))


class GrowthStateCoderTest(unittest.TestCase):
  def test_polling_round_trip_preserves_resume_state(self):
    termination = after_total_of(Duration(30))
    coder = _GrowthStateCoder(StrUtf8Coder(), termination)
    completed = collections.OrderedDict([
        (b'a' * 16, Timestamp(1)),
        (b'b' * 16, Timestamp(2)),
        (b'c' * 16, Timestamp(3)),
    ])
    termination_state = termination.for_new_input(Timestamp(7), 'input')
    state = _PollingGrowthState(completed, Timestamp(5), termination_state)
    decoded = coder.decode(coder.encode(state))
    self.assertEqual(list(completed.items()), list(decoded.completed.items()))
    self.assertEqual(Timestamp(5), decoded.poll_watermark)
    self.assertEqual(termination_state, decoded.termination_state)

  def test_non_polling_round_trip_preserves_pending_outputs(self):
    coder = _GrowthStateCoder(StrUtf8Coder(), never())
    pending = PollResult((_ts('a', 1), _ts('b', 2)), MAX_TIMESTAMP)
    state = _NonPollingGrowthState(pending)
    decoded = coder.decode(coder.encode(state))
    self.assertEqual(MAX_TIMESTAMP, decoded.pending.watermark)
    self.assertEqual([('a', Timestamp(1)), ('b', Timestamp(2))],
                     [(o.value, o.timestamp) for o in decoded.pending.outputs])


class GrowthTrackerTest(unittest.TestCase):
  def test_poll_claims_dedups_and_checkpoints(self):
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 1), _ts('a', 1), _ts('b', 2)])

    plan = _plan(_initial_polling(), poll)
    self.assertEqual(['a', 'b'], [o.value for o in plan.emit])
    self.assertFalse(plan.stop)
    self.assertEqual(Timestamp(1), plan.watermark)
    self.assertIsInstance(plan.primary, _NonPollingGrowthState)
    self.assertIsInstance(plan.residual, _PollingGrowthState)
    self.assertEqual(2, len(plan.residual.completed))

    tracker = _GrowthRestrictionTracker(_initial_polling())
    self.assertFalse(tracker.is_bounded())
    self.assertTrue(tracker.try_claim(plan))
    primary, residual = tracker.try_split(0)
    self.assertIs(plan.primary, primary)
    self.assertIs(plan.residual, residual)
    self.assertTrue(tracker.check_done())

    def explicit_watermark_poll(unused_element):
      return PollResult.incomplete([_ts('c', 3)]).with_watermark(5)

    plan = _plan(_initial_polling(), explicit_watermark_poll)
    self.assertEqual(Timestamp(5), plan.watermark)
    self.assertEqual(Timestamp(5), plan.residual.poll_watermark)

  def test_second_round_repolls_and_dedups_against_completed(self):
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([_ts('a', 1), _ts('b', 2)])
      return PollResult.incomplete([_ts('a', 1), _ts('c', 3)])

    first = _plan(_initial_polling(), poll)
    resumed = _plan(first.residual, poll)
    self.assertEqual(2, len(polls))
    self.assertEqual(['c'], [o.value for o in resumed.emit])

  def test_termination_condition_sets_stop(self):
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 1)])

    termination = after_total_of(10)
    for now, expected_stop in [(10.0, False), (11.0, True)]:
      with self.subTest(now=now):
        plan = _plan(
            _initial_polling(termination, Timestamp(0)),
            poll,
            now=now,
            termination=termination)
        self.assertEqual(expected_stop, plan.stop)

  def test_non_polling_replays(self):
    pending = PollResult((_ts('a', 1), _ts('b', 2)), MAX_TIMESTAMP)
    restriction = _NonPollingGrowthState(pending)
    plan = _replay_plan(restriction)
    self.assertEqual(['a', 'b'], [o.value for o in plan.emit])
    self.assertTrue(plan.stop)

    tracker = _GrowthRestrictionTracker(restriction)
    self.assertTrue(tracker.is_bounded())
    self.assertTrue(tracker.try_claim(plan))
    # A checkpoint after replay resumes an empty state that emits nothing.
    primary, residual = tracker.try_split(0)
    self.assertIs(restriction, primary)
    self.assertEqual((), residual.pending.outputs)
    self.assertTrue(tracker.check_done())

  def test_terminal_split_residual_is_empty_for_all_stop_causes(self):
    termination = after_total_of(Duration(10))
    cases = [
        (
            'reached_max',
            never(),
            _initial_polling(), lambda element: PollResult(
                (TimestampedValue('a', MAX_TIMESTAMP), ), None),
            0.0),
        (
            'complete',
            never(),
            _initial_polling(),
            lambda element: PollResult.complete([_ts('a', 1)]),
            0.0),
        (
            'after_total_of',
            termination,
            _initial_polling(termination, Timestamp(0)),
            lambda element: PollResult.incomplete([_ts('a', 1)]),
            100.0),
    ]
    for name, condition, restriction, poll_fn, now in cases:
      with self.subTest(name=name):
        plan = _plan(restriction, poll_fn, now=now, termination=condition)
        self.assertTrue(plan.stop)
        self.assertIsInstance(plan.residual, _NonPollingGrowthState)
        self.assertEqual((), plan.residual.pending.outputs)

  def test_wrapper_chain_defers_merged_residual(self):
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 1), _ts('b', 2)])

    plan = _plan(_initial_polling(), poll)
    threadsafe = ThreadsafeRestrictionTracker(
        _GrowthRestrictionTracker(_initial_polling()))
    view = RestrictionTrackerView(threadsafe)
    self.assertTrue(view.try_claim(plan))
    view.defer_remainder(Duration(5))
    residual, _ = threadsafe.deferred_status()
    self.assertIsInstance(residual, _PollingGrowthState)
    self.assertEqual(2, len(residual.completed))


# Module-level so the poll function pickles by reference; the call counter is
# shared within the single in-memory DirectRunner process.
_POLL_CALLS = collections.defaultdict(int)


def _growing_poll(prefix):
  _POLL_CALLS[prefix] += 1
  count = _POLL_CALLS[prefix]
  outputs = [_ts('%s%d' % (prefix, i), i + 1) for i in range(count)]
  if count >= 3:
    return PollResult.complete(outputs)
  return PollResult.incomplete(outputs)


def _complete_poll(prefix):
  return PollResult.complete([_ts(prefix + 'a', 1), _ts(prefix + 'b', 2)])


def _windowed_group(kv, window=beam.DoFn.WindowParam):
  return ((window.start, window.end), sorted(kv[1]))


class WatchEndToEndTest(unittest.TestCase):
  def _in_memory_pipeline(self):
    return TestPipeline(
        options=PipelineOptions(direct_running_mode='in_memory'))

  def test_complete_outputs_values_and_timestamps(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch.growth_of(_complete_poll).with_poll_interval(
              Duration(1)).with_output_coder(StrUtf8Coder()))
      assert_that(
          output,
          equal_to([
              TestWindowedValue(('k:', 'k:a'), Timestamp(1), [GlobalWindow()]),
              TestWindowedValue(('k:', 'k:b'), Timestamp(2), [GlobalWindow()]),
          ]),
          reify_windows=True)

  def test_complete_advances_watermark_for_windowed_pipeline(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch.growth_of(_complete_poll).with_poll_interval(
              Duration(1)).with_output_coder(StrUtf8Coder()))
      grouped = (
          output
          | beam.WindowInto(FixedWindows(10))
          | beam.Map(lambda kv: ('all', kv[1]))
          | beam.GroupByKey()
          | beam.Map(_windowed_group))
      assert_that(
          grouped,
          equal_to([
              ((Timestamp(0), Timestamp(10)), ['k:a', 'k:b']),
          ]))

  def test_multi_round_dedups_stops_and_is_per_input(self):
    _POLL_CALLS.clear()
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['x:', 'y:'])
          | Watch.growth_of(_growing_poll).with_poll_interval(
              Duration(0.05)).with_output_coder(StrUtf8Coder()))
      assert_that(
          output,
          equal_to([('x:', 'x:0'), ('x:', 'x:1'), ('x:', 'x:2'), ('y:', 'y:0'),
                    ('y:', 'y:1'), ('y:', 'y:2')]))
    self.assertEqual(3, _POLL_CALLS['x:'])
    self.assertEqual(3, _POLL_CALLS['y:'])

  def test_rejects_non_deterministic_output_coder(self):
    # No output coder resolves to PickleCoder, which is non-deterministic, so
    # dedup could re-emit equal outputs. Expansion must reject it.
    with self.assertRaises(ValueError):
      with self._in_memory_pipeline() as p:
        _ = (
            p | beam.Create(['k:'])
            | Watch.growth_of(_complete_poll).with_poll_interval(Duration(1)))

  def test_derives_output_type_from_input_and_coder(self):
    # expand() types the (input, output) pairs from the input type and the
    # resolved coder, so downstream stays typed without a manual hint.
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch.growth_of(_complete_poll).with_poll_interval(
              Duration(1)).with_output_coder(StrUtf8Coder()))
      self.assertEqual(typehints.Tuple[str, str], output.element_type)


if __name__ == '__main__':
  unittest.main()
