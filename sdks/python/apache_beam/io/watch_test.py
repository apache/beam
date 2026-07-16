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
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.io.watch import PollFn
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import Watch
from apache_beam.io.watch import _GrowthRestrictionTracker
from apache_beam.io.watch import _GrowthStateCoder
from apache_beam.io.watch import _never_seen_before
from apache_beam.io.watch import _NonPollingGrowthState
from apache_beam.io.watch import _PollingGrowthState
from apache_beam.io.watch import _WatchGrowthDoFn
from apache_beam.io.watch import after_total_of
from apache_beam.io.watch import never
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.sdf_utils import RestrictionTrackerView
from apache_beam.runners.sdf_utils import ThreadsafeRestrictionTracker
from apache_beam.runners.sdf_utils import ThreadsafeWatermarkEstimator
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


def _identity(output):
  return output


def _new_results(restriction, result, key_fn=None, use_timestamp=False):
  return _never_seen_before(
      restriction, result, key_fn or _identity, StrUtf8Coder(), use_timestamp)


def _tracker(restriction, use_timestamp=False):
  return _GrowthRestrictionTracker(
      restriction, _identity, StrUtf8Coder(), use_timestamp)


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


class NeverSeenBeforeTest(unittest.TestCase):
  def test_dedups_and_sorts_by_timestamp(self):
    result = PollResult.incomplete([_ts('b', 2), _ts('a', 1), _ts('a', 1)])
    new_results = _new_results(_initial_polling(), result)
    self.assertEqual(['a', 'b'], [o.value for o in new_results.outputs])

  def test_dedups_against_completed_keys(self):
    state = _initial_polling()
    first = _new_results(
        state, PollResult.incomplete([_ts('a', 1), _ts('b', 2)]))
    tracker = _tracker(state)
    self.assertTrue(tracker.try_claim((first, 0)))
    _, residual = tracker.try_split(0)
    second = _new_results(
        residual, PollResult.incomplete([_ts('a', 1), _ts('c', 3)]))
    self.assertEqual(['c'], [o.value for o in second.outputs])

  def test_output_key_dedups_by_derived_key(self):
    result = PollResult.incomplete([_ts('a1', 1), _ts('a2', 2), _ts('b1', 3)])
    # The key is the first character, so 'a1' and 'a2' collapse to one output.
    new_results = _new_results(
        _initial_polling(), result, key_fn=lambda output: output[0])
    self.assertEqual(['a1', 'b1'], [o.value for o in new_results.outputs])

  def test_use_timestamp_dedups_by_key_and_timestamp(self):
    result = PollResult.incomplete([_ts('a', 1), _ts('a', 2), _ts('a', 1)])
    new_results = _new_results(
        _initial_polling(), result, use_timestamp=True)
    self.assertEqual([('a', Timestamp(1)), ('a', Timestamp(2))],
                     [(o.value, o.timestamp) for o in new_results.outputs])

  def test_preserves_explicit_watermark(self):
    result = PollResult.incomplete([_ts('c', 3)]).with_watermark(5)
    new_results = _new_results(_initial_polling(), result)
    self.assertEqual(Timestamp(5), new_results.watermark)


class GrowthTrackerTest(unittest.TestCase):
  def test_claim_then_split_builds_replay_primary_and_merged_residual(self):
    state = _initial_polling()
    new_results = _new_results(
        state, PollResult.incomplete([_ts('a', 1), _ts('b', 2)]))
    tracker = _tracker(state)
    self.assertFalse(tracker.is_bounded())
    self.assertTrue(tracker.try_claim((new_results, 0)))
    primary, residual = tracker.try_split(0)
    self.assertIsInstance(primary, _NonPollingGrowthState)
    self.assertEqual(new_results, primary.pending)
    self.assertIsInstance(residual, _PollingGrowthState)
    self.assertEqual(2, len(residual.completed))
    self.assertEqual(0, residual.termination_state)
    self.assertTrue(tracker.check_done())

  def test_split_merges_explicit_watermark_into_residual(self):
    state = _initial_polling()
    result = PollResult.incomplete([_ts('c', 3)]).with_watermark(5)
    tracker = _tracker(state)
    self.assertTrue(tracker.try_claim((_new_results(state, result), 0)))
    _, residual = tracker.try_split(0)
    self.assertEqual(Timestamp(5), residual.poll_watermark)

  def test_second_claim_is_rejected(self):
    state = _initial_polling()
    new_results = _new_results(state, PollResult.incomplete([_ts('a', 1)]))
    tracker = _tracker(state)
    self.assertTrue(tracker.try_claim((new_results, 0)))
    self.assertFalse(tracker.try_claim((new_results, 0)))

  def test_claim_rejects_already_completed_keys(self):
    # The tracker re-validates a claim, so a poll round that was not deduped
    # against the restriction is rejected instead of emitting duplicates.
    state = _initial_polling()
    first = _new_results(state, PollResult.incomplete([_ts('a', 1)]))
    tracker = _tracker(state)
    self.assertTrue(tracker.try_claim((first, 0)))
    _, residual = tracker.try_split(0)
    stale = PollResult.incomplete([_ts('a', 1)])
    self.assertFalse(_tracker(residual).try_claim((stale, 0)))

  def test_split_before_claim_moves_all_work_to_residual(self):
    state = _initial_polling()
    tracker = _tracker(state)
    primary, residual = tracker.try_split(0)
    self.assertIs(state, residual)
    self.assertIsInstance(primary, _NonPollingGrowthState)
    self.assertEqual((), primary.pending.outputs)
    new_results = _new_results(state, PollResult.incomplete([_ts('a', 1)]))
    self.assertFalse(tracker.try_claim((new_results, 0)))
    self.assertTrue(tracker.check_done())

  def test_non_polling_replays_exactly_the_pending_outputs(self):
    pending = PollResult((_ts('a', 1), _ts('b', 2)), MAX_TIMESTAMP)
    tracker = _tracker(_NonPollingGrowthState(pending))
    self.assertTrue(tracker.is_bounded())
    # A replay must claim the pending poll result exactly.
    partial = PollResult((_ts('a', 1), ), None)
    self.assertFalse(tracker.try_claim((partial, None)))
    self.assertTrue(tracker.try_claim((pending, None)))
    # A checkpoint after the replay leaves no residual work.
    _, residual = tracker.try_split(0)
    self.assertEqual((), residual.pending.outputs)
    self.assertTrue(tracker.check_done())

  def test_check_done_raises_without_claim_or_split(self):
    tracker = _tracker(_initial_polling())
    with self.assertRaises(ValueError):
      tracker.check_done()

  def test_wrapper_chain_defers_merged_residual(self):
    state = _initial_polling()
    new_results = _new_results(
        state, PollResult.incomplete([_ts('a', 1), _ts('b', 2)]))
    threadsafe = ThreadsafeRestrictionTracker(_tracker(state))
    view = RestrictionTrackerView(threadsafe)
    self.assertTrue(view.try_claim((new_results, 0)))
    view.defer_remainder(Duration(5))
    residual, _ = threadsafe.deferred_status()
    self.assertIsInstance(residual, _PollingGrowthState)
    self.assertEqual(2, len(residual.completed))


class TerminationConditionTest(unittest.TestCase):
  def test_never_does_not_stop(self):
    termination = never()
    state = termination.for_new_input(Timestamp(0), 'input')
    self.assertFalse(termination.can_stop_polling(MAX_TIMESTAMP, state))

  def test_after_total_of_stops_once_duration_elapsed(self):
    termination = after_total_of(10)
    state = termination.for_new_input(Timestamp(0), 'input')
    self.assertFalse(termination.can_stop_polling(Timestamp(10), state))
    self.assertTrue(termination.can_stop_polling(Timestamp(11), state))


# Module-level so the poll function pickles by reference; the call counter is
# shared within the single in-memory DirectRunner process.
_POLL_CALLS = collections.defaultdict(int)


def _growing_poll(prefix):
  # Unannotated on purpose: dedup must hold on the inferred fallback coder.
  _POLL_CALLS[prefix] += 1
  count = _POLL_CALLS[prefix]
  outputs = [_ts('%s%d' % (prefix, i), i + 1) for i in range(count)]
  if count >= 3:
    return PollResult.complete(outputs)
  return PollResult.incomplete(outputs)


def _complete_poll(prefix) -> PollResult[str]:
  return PollResult.complete([_ts(prefix + 'a', 1), _ts(prefix + 'b', 2)])


def _first_char(output):
  return output[0]


def _empty_poll(unused_element):
  return PollResult.incomplete([])


def _keyed_poll(prefix):
  # 'a1' and 'a2' share the dedup key 'a', so only 'a1' is emitted.
  return PollResult.complete([_ts('a1', 1), _ts('a2', 2), _ts('b1', 3)])


class _StrCoderPollFn(PollFn):
  def __call__(self, element):
    return PollResult.complete([_ts(element + 'a', 1)])

  def default_output_coder(self):
    return StrUtf8Coder()


class _NoDeterministicFormCoder(Coder):
  def encode(self, value):
    return b''

  def decode(self, encoded):
    return None

  def is_deterministic(self):
    return False


def _windowed_group(kv, window=beam.DoFn.WindowParam):
  return ((window.start, window.end), sorted(kv[1]))


class WatchDoFnProcessTest(unittest.TestCase):
  def _process(self, poll_fn, element, timestamp):
    dofn = _WatchGrowthDoFn(
        poll_fn,
        never(),
        Duration(1),
        StrUtf8Coder(),
        _identity,
        StrUtf8Coder())
    threadsafe = ThreadsafeRestrictionTracker(
        dofn.create_tracker(dofn.initial_restriction(element)))
    estimator = ThreadsafeWatermarkEstimator(ManualWatermarkEstimator(None))
    outputs = list(
        dofn.process(
            element,
            timestamp=timestamp,
            tracker=RestrictionTrackerView(threadsafe),
            watermark_estimator=estimator))
    return outputs, threadsafe, estimator

  def test_empty_round_holds_watermark_at_input_timestamp(self):
    outputs, threadsafe, estimator = self._process(
        _empty_poll, 'in', Timestamp(7))
    self.assertEqual([], outputs)
    # The estimator is seeded from the input timestamp, so the deferred
    # residual holds the watermark there instead of at MIN_TIMESTAMP.
    self.assertEqual(Timestamp(7), estimator.current_watermark())
    residual, _ = threadsafe.deferred_status()
    self.assertIsInstance(residual, _PollingGrowthState)

  def test_complete_round_stops_without_residual(self):
    outputs, threadsafe, _ = self._process(_complete_poll, 'k:', Timestamp(0))
    self.assertEqual([('k:', 'k:a'), ('k:', 'k:b')],
                     [value.value for value in outputs])
    self.assertIsNone(threadsafe.deferred_status())
    self.assertTrue(threadsafe.check_done())


class WatchEndToEndTest(unittest.TestCase):
  def _in_memory_pipeline(self):
    return TestPipeline(
        options=PipelineOptions(direct_running_mode='in_memory'))

  def test_complete_outputs_values_and_timestamps(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch(_complete_poll, poll_interval=Duration(1)))
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
          | Watch(_complete_poll, poll_interval=Duration(1)))
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
          | Watch(_growing_poll, poll_interval=Duration(0.05)))
      assert_that(
          output,
          equal_to([('x:', 'x:0'), ('x:', 'x:1'), ('x:', 'x:2'), ('y:', 'y:0'),
                    ('y:', 'y:1'), ('y:', 'y:2')]))
    self.assertEqual(3, _POLL_CALLS['x:'])
    self.assertEqual(3, _POLL_CALLS['y:'])

  def test_output_key_dedups_across_pipeline(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k'])
          | Watch(
              _keyed_poll, poll_interval=Duration(1),
              output_key_fn=_first_char))
      assert_that(output, equal_to([('k', 'a1'), ('k', 'b1')]))

  def test_rejects_key_coder_without_deterministic_form(self):
    with self.assertRaises(ValueError):
      with self._in_memory_pipeline() as p:
        _ = (
            p | beam.Create(['k:'])
            | Watch(
                _complete_poll,
                poll_interval=Duration(1),
                output_key_coder=_NoDeterministicFormCoder()))

  def test_infers_output_coder_from_return_annotation(self):
    # _complete_poll is annotated ``-> PollResult[str]``, so the output coder
    # and with it the (input, output) element type are inferred without hints.
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch(_complete_poll, poll_interval=Duration(1)))
      self.assertEqual(typehints.Tuple[str, str], output.element_type)

  def test_uses_poll_fn_default_output_coder(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch(_StrCoderPollFn(), poll_interval=Duration(1)))
      self.assertEqual(typehints.Tuple[str, str], output.element_type)


if __name__ == '__main__':
  unittest.main()
