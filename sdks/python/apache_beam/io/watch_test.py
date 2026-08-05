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
import typing
import unittest

import apache_beam as beam
from apache_beam.coders.coders import BytesCoder
from apache_beam.coders.coders import Coder
from apache_beam.coders.coders import ListCoder
from apache_beam.coders.coders import NullableCoder
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.coders.coders import TimestampCoder
from apache_beam.coders.coders import TupleCoder
from apache_beam.coders.coders import VarIntCoder
from apache_beam.io.watch import PollFn
from apache_beam.io.watch import PollResult
from apache_beam.io.watch import Watch
from apache_beam.io.watch import _GrowthRestrictionTracker
from apache_beam.io.watch import _GrowthStateCoder
from apache_beam.io.watch import _never_seen_before
from apache_beam.io.watch import _NonPollingGrowthState
from apache_beam.io.watch import _past_cursor
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


def _new_results(restriction, result, key_fn=None):
  return _never_seen_before(
      restriction, result, key_fn or _identity, StrUtf8Coder())


def _tracker(restriction):
  return _GrowthRestrictionTracker(restriction, _identity, StrUtf8Coder())


def _cursor_tracker(restriction):
  return _GrowthRestrictionTracker(
      restriction, _identity, StrUtf8Coder(), timestamp_cursor=True)


def _initial_polling(termination=None, now=Timestamp(0)):
  termination = termination or never()
  return _PollingGrowthState(
      collections.OrderedDict(), None, termination.for_new_input(now, 'input'))


class PollResultTest(unittest.TestCase):
  def test_normalize_stamps_one_processing_time_when_timestamp_none(self):
    before = Timestamp.now()
    result = PollResult.incomplete(['a', 'b'])
    after = Timestamp.now()
    # Raw outputs share a single processing-time stamp (no per-output jitter).
    stamps = {o.timestamp for o in result.outputs}
    self.assertEqual(1, len(stamps))
    ts = stamps.pop()
    self.assertTrue(before <= ts <= after)

  def test_normalize_preserves_timestamped_and_applies_explicit_default(self):
    result = PollResult.incomplete([_ts('a', 1), 'b'], timestamp=7)
    by_value = {o.value: o.timestamp for o in result.outputs}
    self.assertEqual(Timestamp(1), by_value['a'])  # TimestampedValue preserved
    self.assertEqual(Timestamp(7), by_value['b'])  # raw stamped with default

  def test_complete_releases_watermark_to_max(self):
    self.assertEqual(
        MAX_TIMESTAMP, PollResult.complete([_ts('a', 1)]).watermark)
    self.assertTrue(PollResult.complete([]).is_complete)

  def test_with_watermark_overrides(self):
    self.assertEqual(
        Timestamp(0),
        PollResult.incomplete([_ts('a', 9)]).with_watermark(0).watermark)


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
    self.assertIsNone(decoded.cursor)

  def test_polling_round_trip_preserves_cursor(self):
    coder = _GrowthStateCoder(StrUtf8Coder(), never())
    state = _PollingGrowthState(
        collections.OrderedDict(),
        Timestamp(5),
        never().for_new_input(Timestamp(0), 'input'),
        Timestamp(42))
    decoded = coder.decode(coder.encode(state))
    self.assertEqual(Timestamp(42), decoded.cursor)
    self.assertEqual(0, len(decoded.completed))
    self.assertIsNone(decoded.poll_watermark)  # not part of the payload

  def test_cursorless_state_keeps_the_pre_cursor_byte_format(self):
    # A polling state without a cursor must encode exactly as before the
    # cursor existed, so in-flight hash-mode restrictions decode across an
    # upgrade in either direction.
    termination = never()
    coder = _GrowthStateCoder(StrUtf8Coder(), termination)
    completed = collections.OrderedDict([(b'a' * 16, Timestamp(1))])
    termination_state = termination.for_new_input(Timestamp(0), 'input')
    state = _PollingGrowthState(completed, Timestamp(5), termination_state)
    legacy_polling_coder = TupleCoder([
        termination.state_coder(),
        NullableCoder(TimestampCoder()),
        ListCoder(TupleCoder([BytesCoder(), TimestampCoder()])),
    ])
    legacy_payload = legacy_polling_coder.encode(
        (termination_state, Timestamp(5), list(completed.items())))
    legacy_encoded = TupleCoder([VarIntCoder(), BytesCoder()]).encode(
        (0, legacy_payload))
    self.assertEqual(legacy_encoded, coder.encode(state))
    decoded = coder.decode(legacy_encoded)
    self.assertEqual(list(completed.items()), list(decoded.completed.items()))
    self.assertIsNone(decoded.cursor)

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

  def test_idle_round_reuses_completed_map_object(self):
    # A round that discovers nothing must reuse the parent dedup map rather
    # than copying it O(N), so a steady-state empty poll stays cheap.
    state = _initial_polling()
    first = _new_results(state, PollResult.incomplete([_ts('a', 1)]))
    tracker = _tracker(state)
    self.assertTrue(tracker.try_claim((first, 0)))
    _, residual1 = tracker.try_split(0)
    resumed = _tracker(residual1)
    empty = _new_results(residual1, PollResult.incomplete([]))
    self.assertTrue(resumed.try_claim((empty, 0)))
    _, residual2 = resumed.try_split(0)
    self.assertIs(residual1.completed, residual2.completed)


class TimestampCursorTest(unittest.TestCase):
  """Cursor-mode dedup: high-water-mark timestamp instead of a hash set."""
  def test_keeps_state_o1_and_tracks_high_water_mark(self):
    state = _initial_polling()
    result = PollResult.incomplete([_ts('a', 1), _ts('b', 2), _ts('c', 3)])
    new_results = _past_cursor(state, result)
    self.assertEqual(['a', 'b', 'c'], [o.value for o in new_results.outputs])
    tracker = _cursor_tracker(state)
    self.assertTrue(tracker.try_claim((new_results, 0)))
    _, residual = tracker.try_split(0)
    self.assertIsInstance(residual, _PollingGrowthState)
    self.assertEqual(0, len(residual.completed))  # no hash set
    self.assertEqual(Timestamp(3), residual.cursor)  # high-water mark

  def test_emits_only_outputs_after_the_cursor(self):
    # A later round emits only outputs strictly past the cursor; a re-listed
    # output (== cursor) and an earlier output (< cursor) are both dropped.
    state = _initial_polling()
    tracker = _cursor_tracker(state)
    first = _past_cursor(state, PollResult.incomplete([_ts('a', 10)]))
    self.assertTrue(tracker.try_claim((first, 0)))
    _, residual = tracker.try_split(0)
    self.assertEqual(Timestamp(10), residual.cursor)
    second = _past_cursor(
        residual,
        PollResult.incomplete([_ts('early', 5), _ts('a', 10), _ts('c', 20)]))
    self.assertEqual(['c'], [o.value for o in second.outputs])  # only 20 > 10
    resumed = _cursor_tracker(residual)
    self.assertTrue(resumed.try_claim((second, 0)))
    _, residual = resumed.try_split(0)
    self.assertEqual(Timestamp(20), residual.cursor)

  def test_relist_emits_each_output_exactly_once(self):
    # A full re-list of a growing collection at strictly increasing event
    # times emits each output once; the state never accumulates a hash set.
    state = _initial_polling()
    emitted = collections.Counter()
    for round_index in range(10):
      result = PollResult.incomplete(
          [_ts('f%d' % i, i + 1) for i in range(round_index + 1)])
      new_results = _past_cursor(state, result)
      tracker = _cursor_tracker(state)
      self.assertTrue(tracker.try_claim((new_results, 0)))
      for output in new_results.outputs:
        emitted[output.value] += 1
      _, state = tracker.try_split(0)
      self.assertEqual(0, len(state.completed))  # O(1) throughout
    self.assertEqual([1] * 10, [emitted['f%d' % i] for i in range(10)])
    self.assertEqual(Timestamp(10), state.cursor)

  def test_round_below_high_water_mark_keeps_cursor_and_reuses_state(self):
    # A round whose outputs are all at or below the cursor emits nothing and
    # leaves the cursor unchanged; the (empty) completed map is reused as-is.
    state = _initial_polling()
    tracker = _cursor_tracker(state)
    first = _past_cursor(state, PollResult.incomplete([_ts('a', 10)]))
    self.assertTrue(tracker.try_claim((first, 0)))
    _, residual1 = tracker.try_split(0)
    stale = _past_cursor(
        residual1, PollResult.incomplete([_ts('a', 10), _ts('old', 4)]))
    self.assertEqual((), stale.outputs)
    resumed = _cursor_tracker(residual1)
    self.assertTrue(resumed.try_claim((stale, 0)))
    _, residual2 = resumed.try_split(0)
    self.assertEqual(Timestamp(10), residual2.cursor)  # unchanged
    self.assertIs(residual1.completed, residual2.completed)

  def test_claim_rejects_outputs_at_or_below_the_cursor(self):
    # The tracker re-validates a claim, so a round that was not filtered
    # against the cursor is rejected instead of emitting already-seen outputs.
    state = _initial_polling()
    tracker = _cursor_tracker(state)
    first = _past_cursor(state, PollResult.incomplete([_ts('a', 10)]))
    self.assertTrue(tracker.try_claim((first, 0)))
    _, residual = tracker.try_split(0)
    stale = PollResult.incomplete([_ts('a', 10)])
    self.assertFalse(_cursor_tracker(residual).try_claim((stale, 0)))

  def test_replay_validates_by_timestamps(self):
    # Cursor mode never hashes, so a replay is validated by its timestamps.
    pending = PollResult((_ts('a', 1), _ts('b', 2)), MAX_TIMESTAMP)
    tracker = _cursor_tracker(_NonPollingGrowthState(pending))
    partial = PollResult((_ts('a', 1), ), None)
    self.assertFalse(tracker.try_claim((partial, None)))
    self.assertTrue(tracker.try_claim((pending, None)))

  def test_switching_hash_state_to_cursor_drops_the_hash_map(self):
    # A restriction carried over from hash dedup still holds completed hashes;
    # cursor mode ignores them, so the first cursor round must drop them and
    # make the state O(1) rather than carry dead hashes forever.
    legacy = _PollingGrowthState(
        collections.OrderedDict([(b'a' * 16, Timestamp(1))]),
        None,
        never().for_new_input(Timestamp(0), 'input'))
    result = _past_cursor(legacy, PollResult.incomplete([_ts('a', 100)]))
    tracker = _cursor_tracker(legacy)
    self.assertTrue(tracker.try_claim((result, 0)))
    _, residual = tracker.try_split(0)
    self.assertEqual(0, len(residual.completed))
    self.assertEqual(Timestamp(100), residual.cursor)

  def test_switching_hash_state_to_cursor_seeds_the_cursor(self):
    # Outputs at or below the hash map's greatest recorded event time are
    # already seen and must not re-emit after the switch.
    legacy = _PollingGrowthState(
        collections.OrderedDict([(b'a' * 16, Timestamp(5)),
                                 (b'b' * 16, Timestamp(10))]),
        None,
        never().for_new_input(Timestamp(0), 'input'))
    relist = PollResult.incomplete([_ts('a', 5), _ts('b', 10), _ts('c', 20)])
    new_results = _past_cursor(legacy, relist)
    self.assertEqual(['c'], [o.value for o in new_results.outputs])
    tracker = _cursor_tracker(legacy)
    self.assertTrue(tracker.try_claim((new_results, 0)))
    _, residual = tracker.try_split(0)
    self.assertEqual(0, len(residual.completed))
    self.assertEqual(Timestamp(20), residual.cursor)

  def test_hash_round_drops_a_stale_cursor(self):
    # The reverse switch: a hash round drops the cursor, so a state never
    # holds hashes and a cursor at the same time.
    state = _PollingGrowthState(
        collections.OrderedDict(), None, 0, cursor=Timestamp(10))
    tracker = _tracker(state)
    result = _new_results(state, PollResult.incomplete([_ts('a', 20)]))
    self.assertTrue(tracker.try_claim((result, 0)))
    _, residual = tracker.try_split(0)
    self.assertIsNone(residual.cursor)
    self.assertEqual(1, len(residual.completed))

  def test_cursor_state_encoding_size_is_independent_of_outputs(self):
    coder = _GrowthStateCoder(StrUtf8Coder(), never())

    def encoded_residual_after_claiming(count):
      state = _initial_polling()
      result = PollResult.incomplete(
          [_ts('output%d' % i, i + 1) for i in range(count)])
      tracker = _cursor_tracker(state)
      self.assertTrue(tracker.try_claim((_past_cursor(state, result), 0)))
      _, residual = tracker.try_split(0)
      return coder.encode(residual)

    self.assertEqual(
        len(encoded_residual_after_claiming(1)),
        len(encoded_residual_after_claiming(100)))


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


def _out_of_order_poll(prefix):
  # Round 1 emits late_after@10 (advances the watermark to 10); round 2 emits
  # early@5, which is behind the watermark and therefore late.
  _POLL_CALLS[prefix] += 1
  if _POLL_CALLS[prefix] == 1:
    return PollResult.incomplete([_ts(prefix + 'late_after', 10)])
  return PollResult.complete([_ts(prefix + 'early', 5)])


def _max_timestamp_poll(unused_element):
  return PollResult.incomplete(
      [_ts('a', 10), TimestampedValue('b', MAX_TIMESTAMP)])


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
  def _process(
      self,
      poll_fn,
      element,
      timestamp,
      restriction=None,
      watermark=None,
      timestamp_cursor=False):
    dofn = _WatchGrowthDoFn(
        poll_fn,
        never(),
        Duration(1),
        StrUtf8Coder(),
        _identity,
        StrUtf8Coder(),
        timestamp_cursor)
    if restriction is None:
      restriction = dofn.initial_restriction(element)
    threadsafe = ThreadsafeRestrictionTracker(dofn.create_tracker(restriction))
    estimator = ThreadsafeWatermarkEstimator(
        ManualWatermarkEstimator(watermark))
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

  def test_replay_round_leaves_the_watermark_alone(self):
    pending = PollResult((_ts('k:a', 1), _ts('k:b', 2)), MAX_TIMESTAMP)
    outputs, threadsafe, estimator = self._process(
        _empty_poll,
        'k:',
        Timestamp(7),
        restriction=_NonPollingGrowthState(pending))
    self.assertEqual([('k:', 'k:a'), ('k:', 'k:b')],
                     [value.value for value in outputs])
    # The replay branch holds the watermark at the seed, so it never runs ahead
    # of the replayed outputs and never releases to MAX_TIMESTAMP itself.
    self.assertEqual(Timestamp(7), estimator.current_watermark())
    self.assertIsNone(threadsafe.deferred_status())
    self.assertTrue(threadsafe.check_done())

  def test_terminal_round_after_deferring_leaves_no_residual(self):
    _POLL_CALLS.clear()
    # Round one defers and parks the watermark on the new output's time.
    _, threadsafe, estimator = self._process(_growing_poll, 'd:', Timestamp(0))
    residual, _ = threadsafe.deferred_status()
    self.assertIsInstance(residual, _PollingGrowthState)
    self.assertEqual(Timestamp(1), estimator.current_watermark())
    # Round two resumes from that residual, carrying the watermark forward.
    _, threadsafe, estimator = self._process(
        _growing_poll,
        'd:',
        Timestamp(0),
        restriction=residual,
        watermark=estimator.current_watermark())
    residual, _ = threadsafe.deferred_status()
    self.assertEqual(Timestamp(2), estimator.current_watermark())
    # Round three completes. The watermark stays where round two left it and
    # the round reports no residual, so nothing carries that hold forward.
    outputs, threadsafe, estimator = self._process(
        _growing_poll,
        'd:',
        Timestamp(0),
        restriction=residual,
        watermark=estimator.current_watermark())
    self.assertEqual([('d:', 'd:2')], [value.value for value in outputs])
    self.assertEqual(Timestamp(2), estimator.current_watermark())
    self.assertIsNone(threadsafe.deferred_status())
    self.assertTrue(threadsafe.check_done())

  def test_cursor_at_max_timestamp_stops_polling(self):
    # A cursor reaching MAX is terminal: nothing can be strictly past it, so
    # the round stops instead of polling forever and dropping every output.
    outputs, threadsafe, _ = self._process(
        _max_timestamp_poll, 'k:', Timestamp(0), timestamp_cursor=True)
    self.assertEqual([('k:', 'a'), ('k:', 'b')],
                     [value.value for value in outputs])
    self.assertIsNone(threadsafe.deferred_status())
    self.assertTrue(threadsafe.check_done())

  def test_resumed_cursor_at_max_stops_without_polling(self):
    # A restriction resumed with the cursor already at MAX (persisted by a
    # checkpoint after a MAX-timestamped round) must stop without invoking the
    # poll function at all.
    polls = []

    def poll(unused_element):
      polls.append(1)
      return PollResult.incomplete([])

    resumed = _PollingGrowthState(
        collections.OrderedDict(),
        None,
        never().for_new_input(Timestamp(0), 'input'),
        MAX_TIMESTAMP)
    outputs, threadsafe, _ = self._process(
        poll, 'k:', Timestamp(0), restriction=resumed, timestamp_cursor=True)
    self.assertEqual([], outputs)
    self.assertEqual([], polls)  # the poll function never ran
    self.assertIsNone(threadsafe.deferred_status())
    self.assertTrue(threadsafe.check_done())

  def test_out_of_order_new_output_emits_late_and_warns(self):
    # Round 1 surfaces late_after@10 and parks the watermark there; round 2
    # surfaces a brand-new early@5. The output is emitted at its true (earlier)
    # time, so it is late for downstream windowing, and Watch warns about it.
    _POLL_CALLS.clear()
    _, threadsafe, estimator = self._process(
        _out_of_order_poll, 'k:', Timestamp(0))
    self.assertEqual(Timestamp(10), estimator.current_watermark())
    residual, _ = threadsafe.deferred_status()
    with self.assertLogs('apache_beam.io.watch', level='WARNING') as logs:
      outputs, _, _ = self._process(
          _out_of_order_poll,
          'k:',
          Timestamp(0),
          restriction=residual,
          watermark=estimator.current_watermark())
    self.assertEqual([('k:', 'k:early')], [value.value for value in outputs])
    self.assertEqual([Timestamp(5)], [value.timestamp for value in outputs])
    self.assertTrue(
        any('behind the watermark' in line for line in logs.output),
        'expected a late-emission warning, got: %s' % logs.output)

  def test_first_round_early_output_does_not_warn(self):
    # While the estimator holds the input element's timestamp seed, an output
    # behind it must not trigger the out-of-order warning: the seed is not a
    # poll-order signal.
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 5)])

    with self.assertNoLogs('apache_beam.io.watch', level='WARNING'):
      outputs, _, _ = self._process(poll, 'k:', Timestamp(10))
    self.assertEqual([Timestamp(5)], [value.timestamp for value in outputs])

  def test_early_output_after_empty_poll_does_not_warn(self):
    # An empty first poll defers with the watermark still at the element seed;
    # the next round's first real output must not be treated as out-of-order
    # either; the watermark has not advanced past the seed.
    polls = []

    def poll(unused_element):
      polls.append(len(polls))
      if len(polls) == 1:
        return PollResult.incomplete([])
      return PollResult.incomplete([_ts('a', 5)])

    _, threadsafe, estimator = self._process(poll, 'k:', Timestamp(10))
    self.assertEqual(Timestamp(10), estimator.current_watermark())
    residual, _ = threadsafe.deferred_status()
    with self.assertNoLogs('apache_beam.io.watch', level='WARNING'):
      outputs, _, _ = self._process(
          poll,
          'k:',
          Timestamp(10),
          restriction=residual,
          watermark=estimator.current_watermark())
    self.assertEqual([Timestamp(5)], [value.timestamp for value in outputs])

  def test_explicit_watermark_holds_below_output_time(self):
    # An explicit watermark below the output's own event time is honored, so
    # a later, earlier-timestamped output stays on time (the out-of-order-safe
    # path).
    def poll(unused_element):
      return PollResult.incomplete([_ts('a', 10)]).with_watermark(0)

    _, threadsafe, estimator = self._process(poll, 'k:', Timestamp(0))
    self.assertEqual(Timestamp(0), estimator.current_watermark())
    residual, _ = threadsafe.deferred_status()
    self.assertEqual(Timestamp(0), residual.poll_watermark)


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

  def test_timestamp_cursor_dedups_growing_source(self):
    _POLL_CALLS.clear()
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['x:', 'y:'])
          | Watch(
              _growing_poll,
              poll_interval=Duration(0.05),
              timestamp_cursor=True))
      # Each output is emitted exactly once via the high-water-mark cursor,
      # with no hash set kept, across poll rounds and checkpoints.
      assert_that(
          output,
          equal_to([('x:', 'x:0'), ('x:', 'x:1'), ('x:', 'x:2'), ('y:', 'y:0'),
                    ('y:', 'y:1'), ('y:', 'y:2')]))

  def test_timestamp_cursor_rejects_key_spec(self):
    with self.assertRaises(ValueError):
      Watch(
          _complete_poll,
          poll_interval=Duration(1),
          output_key_fn=_first_char,
          timestamp_cursor=True)

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

  def test_infers_coder_from_generic_annotations(self):
    # tuple[str, float] and typing.Tuple[str, float] resolve to a tuple coder,
    # not the pickling fallback.
    def native_poll(element) -> PollResult[tuple[str, float]]:
      return PollResult.complete([(element, 1.0)])

    def typing_poll(
        element) -> PollResult[typing.Tuple[str, float]]:  # noqa: UP006
      return PollResult.complete([(element, 1.0)])

    for poll in (native_poll, typing_poll):
      with self._in_memory_pipeline() as p:
        output = (
            p | beam.Create(['k:']) | Watch(poll, poll_interval=Duration(1)))
        self.assertEqual(
            typehints.Tuple[str, typehints.Tuple[str, float]],
            output.element_type)

  def test_uses_poll_fn_default_output_coder(self):
    with self._in_memory_pipeline() as p:
      output = (
          p | beam.Create(['k:'])
          | Watch(_StrCoderPollFn(), poll_interval=Duration(1)))
      self.assertEqual(typehints.Tuple[str, str], output.element_type)


if __name__ == '__main__':
  unittest.main()
