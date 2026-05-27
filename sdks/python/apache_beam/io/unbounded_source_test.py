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

"""Tests for apache_beam.io.unbounded_source.

Strategy: checkpoint/resume/watermark/coder semantics are covered by
deterministic unit tests (no pipeline, no wall clock). A single end-to-end
DirectRunner test asserts only ordering + termination -- no defer-timing
assertions, which would be flaky (cf. periodicsequence_test which skips
processing-time tests for the same reason).
"""

# pytype: skip-file

import gc
import logging
import os
import tempfile
import time
import unittest

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import unbounded_source as _unbounded_source_module
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import ReadFromUnboundedSource
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.io.unbounded_source import _NO_DATA
from apache_beam.io.unbounded_source import _UnboundedSourceRestriction
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionCoder
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionProvider
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionTracker
from apache_beam.io.unbounded_source import _set_watermark_if_greater
from apache_beam.runners import sdf_utils
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import core
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

# pylint: disable=expression-not-assigned

# ------------------------------------------------------------------------------
# A tiny in-memory demo source: emits the integers 0..count-1, one per record,
# with event time Timestamp(index). It self-terminates (watermark -> MAX after
# the last record) so a pipeline reading it ends. Resumes from a checkpoint at
# (last_index + 1).
# ------------------------------------------------------------------------------


class _CountingCheckpointMark(CheckpointMark):
  def __init__(self, last_index, finalize_log=None):
    self.last_index = last_index
    self._finalize_log = finalize_log

  def finalize_checkpoint(self):
    if self._finalize_log is not None:
      self._finalize_log.append(self.last_index)

  def __eq__(self, other):
    return (
        isinstance(other, _CountingCheckpointMark) and
        other.last_index == self.last_index)

  def __hash__(self):
    return hash(self.last_index)

  def __repr__(self):
    return '_CountingCheckpointMark(last_index=%r)' % (self.last_index, )


class _CountingReader(UnboundedReader):
  def __init__(self, count, start_index, finalize_log=None):
    self._count = count
    self._next = start_index
    self._current = None
    self._exhausted = False
    self._finalize_log = finalize_log
    self.closed = False

  def _read_next(self):
    if self._next >= self._count:
      self._exhausted = True
      return False
    self._current = self._next
    self._next += 1
    return True

  def start(self):
    return self._read_next()

  def advance(self):
    return self._read_next()

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp(self._current)

  def get_watermark(self):
    if self._exhausted:
      return MAX_TIMESTAMP
    if self._current is None:
      return MIN_TIMESTAMP
    return Timestamp(self._current)

  def get_checkpoint_mark(self):
    last = self._current if self._current is not None else self._next - 1
    return _CountingCheckpointMark(last, finalize_log=self._finalize_log)

  def close(self):
    self.closed = True


class CountingSource(UnboundedSource):
  def __init__(self, count, finalize_log=None):
    self._count = count
    self._finalize_log = finalize_log
    self.last_reader = None

  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _CountingReader(
        self._count, start_index, finalize_log=self._finalize_log)
    return self.last_reader

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class _StringCountingReader(_CountingReader):
  def get_current(self):
    return 'v%s' % self._current


class _StringCountingSource(CountingSource):
  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _StringCountingReader(
        self._count, start_index, finalize_log=self._finalize_log)
    return self.last_reader

  def default_output_coder(self):
    return coders.StrUtf8Coder()


class _NoDataReader(UnboundedReader):
  """Always reports 'no data right now' (watermark < MAX, so never EOF)."""
  def start(self):
    return False

  def advance(self):
    return False

  def get_current(self):
    raise AssertionError('no data available')

  def get_current_timestamp(self):
    raise AssertionError('no data available')

  def get_watermark(self):
    return Timestamp(0)

  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(-1)


class _NoDataSource(UnboundedSource):
  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    return _NoDataReader()

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class _RaisingReader(UnboundedReader):
  def __init__(self, marker_path):
    self._marker_path = marker_path

  def start(self):
    return True  # first record available

  def advance(self):
    raise RuntimeError('reader.advance() boom')

  def get_current(self):
    return 'rec'

  def get_current_timestamp(self):
    return Timestamp(0)

  def get_watermark(self):
    return Timestamp(0)

  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(0)

  def close(self):
    if self._marker_path is not None:
      with open(self._marker_path, 'a') as fp:
        fp.write('closed\n')


class _RaisingSource(UnboundedSource):
  def __init__(self, marker_path=None):
    self._marker_path = marker_path

  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    return _RaisingReader(self._marker_path)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


# A non-raising marker-aware source for testing DoFn-side close on the
# *downstream* yield-raise path (where the source itself is well-behaved but a
# downstream Map raises mid-bundle). Module-level for cloudpickle.
class _MarkerCloseReader(UnboundedReader):
  def __init__(self, marker_path):
    self._marker_path = marker_path
    self._idx = -1

  def start(self):
    self._idx = 0
    return True

  def advance(self):
    self._idx += 1
    return self._idx < 3

  def get_current(self):
    return self._idx

  def get_current_timestamp(self):
    return Timestamp(self._idx)

  def get_watermark(self):
    return Timestamp(self._idx) if self._idx < 2 else MAX_TIMESTAMP

  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(self._idx)

  def close(self):
    if self._marker_path is not None:
      with open(self._marker_path, 'a') as fp:
        fp.write('closed\n')


class _MarkerCloseSource(UnboundedSource):
  def __init__(self, marker_path=None):
    self._marker_path = marker_path

  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    return _MarkerCloseReader(self._marker_path)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


def _downstream_boom(_unused):
  """Module-level so it pickles cleanly through Beam's bundle worker boundary.
  Used by ``DoFnReaderCloseOnDownstreamRaiseTest`` to simulate a downstream
  transform that raises mid-bundle (the harness-driven yield-raise path).
  """
  raise RuntimeError('downstream boom')


def _new_tracker(source, checkpoint=None):
  restriction = _UnboundedSourceRestriction(
      source=source, checkpoint_mark=checkpoint)
  return _UnboundedSourceRestrictionTracker(restriction)


def _claim(tracker):
  """Claims once; returns (claimed_bool, holder_value)."""
  holder = [None]
  claimed = tracker.try_claim(holder)
  return claimed, holder[0]


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


class AbcContractTest(unittest.TestCase):
  def test_checkpointmark_default_finalize_is_noop(self):
    self.assertIsNone(CheckpointMark().finalize_checkpoint())

  def test_unboundedsource_is_bounded_false(self):
    self.assertFalse(CountingSource(3).is_bounded())

  def test_reader_lifecycle_start_advance_eof(self):
    reader = CountingSource(3).create_reader(None, None)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 0)
    self.assertEqual(reader.get_current_timestamp(), Timestamp(0))
    self.assertTrue(reader.advance())
    self.assertEqual(reader.get_current(), 1)
    self.assertTrue(reader.advance())
    self.assertEqual(reader.get_current(), 2)
    self.assertFalse(reader.advance())
    self.assertEqual(reader.get_watermark(), MAX_TIMESTAMP)


class RestrictionCoderTest(unittest.TestCase):
  def test_roundtrip_no_checkpoint(self):
    source = CountingSource(3)
    coder = _UnboundedSourceRestrictionCoder(source.get_checkpoint_mark_coder())
    decoded = coder.decode(
        coder.encode(_UnboundedSourceRestriction(source=source)))
    self.assertIsNone(decoded.checkpoint_mark)
    self.assertEqual(decoded.watermark, MIN_TIMESTAMP)
    self.assertFalse(decoded.is_done)
    reader = decoded.source.create_reader(None, None)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 0)

  def test_roundtrip_with_checkpoint_resumes(self):
    source = CountingSource(5)
    coder = _UnboundedSourceRestrictionCoder(source.get_checkpoint_mark_coder())
    restriction = _UnboundedSourceRestriction(
        source=source,
        checkpoint_mark=_CountingCheckpointMark(1),
        watermark=Timestamp(1),
        is_done=False)
    decoded = coder.decode(coder.encode(restriction))
    self.assertEqual(decoded.checkpoint_mark.last_index, 1)
    self.assertEqual(decoded.watermark, Timestamp(1))
    self.assertFalse(decoded.is_done)
    # A reader built from the decoded checkpoint resumes at the next index.
    reader = decoded.source.create_reader(None, decoded.checkpoint_mark)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 2)


class RestrictionProviderTest(unittest.TestCase):
  def test_initial_split_calls_source_split(self):
    split_log = []

    class _NamedSource(CountingSource):
      def __init__(self, name):
        super().__init__(0)
        self.name = name

      def split(self, desired_num_splits, options=None):
        split_log.append((desired_num_splits, options))
        return [_NamedSource('a'), _NamedSource('b')]

    source = _NamedSource('root')
    provider = _UnboundedSourceRestrictionProvider(options='opts')
    restriction = _UnboundedSourceRestriction(
        source=source, watermark=Timestamp(7))

    splits = list(provider.split(source, restriction))

    self.assertEqual(split_log, [(20, 'opts')])
    self.assertEqual([split.source.name for split in splits], ['a', 'b'])
    self.assertEqual([split.watermark for split in splits], [Timestamp(7)] * 2)
    self.assertTrue(all(split.checkpoint_mark is None for split in splits))
    self.assertTrue(
        all(split.finalization_checkpoint_mark is None for split in splits))

  def test_initial_split_does_not_split_checkpointed_restriction(self):
    split_log = []

    class _SplitSource(CountingSource):
      def split(self, desired_num_splits, options=None):
        split_log.append((desired_num_splits, options))
        return [self]

    source = _SplitSource(5)
    provider = _UnboundedSourceRestrictionProvider(options='opts')
    restriction = _UnboundedSourceRestriction(
        source=source, checkpoint_mark=_CountingCheckpointMark(2))

    self.assertEqual(list(provider.split(source, restriction)), [restriction])
    self.assertEqual(split_log, [])

  def test_initial_split_falls_back_to_original_on_split_error(self):
    class _BoomSource(CountingSource):
      def split(self, desired_num_splits, options=None):
        raise RuntimeError('split boom')

    source = _BoomSource(5)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(source=source)

    self.assertEqual(list(provider.split(source, restriction)), [restriction])


class RestrictionTrackerTest(unittest.TestCase):
  def test_claim_emits_in_order(self):
    tracker = _new_tracker(CountingSource(3))
    values = []
    while True:
      claimed, record = _claim(tracker)
      if not claimed:
        break
      self.assertIsNot(record, _NO_DATA)
      values.append(record[0])
    self.assertEqual(values, [0, 1, 2])
    self.assertTrue(tracker.check_done())

  def test_claim_emits_final_record_when_watermark_is_max(self):
    # Regression: a reader may return its final record (has_data True) while
    # simultaneously reporting a MAX_TIMESTAMP watermark ("nothing after this").
    # The record must still be emitted; EOF is realized on the next claim.
    class _FinalRecordReader(UnboundedReader):
      def start(self):
        return True

      def advance(self):
        return False

      def get_current(self):
        return 'last'

      def get_current_timestamp(self):
        return Timestamp(0)

      def get_watermark(self):
        return MAX_TIMESTAMP

      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

    class _FinalSource(UnboundedSource):
      def split(self, desired_num_splits, options=None):
        return [self]

      def create_reader(self, options, checkpoint_mark):
        return _FinalRecordReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    tracker = _new_tracker(_FinalSource())
    claimed, record = _claim(tracker)
    self.assertTrue(claimed)
    self.assertIsNot(record, _NO_DATA)
    self.assertEqual(record[0], 'last')
    # The next claim observes EOF and finishes (no second, phantom record).
    claimed_again, _ = _claim(tracker)
    self.assertFalse(claimed_again)
    self.assertTrue(tracker.check_done())

  def test_try_split_zero_produces_resumable_residual(self):
    source = CountingSource(5)
    tracker = _new_tracker(source)
    # Claim 0 and 1.
    self.assertEqual(_claim(tracker)[1][0], 0)
    self.assertEqual(_claim(tracker)[1][0], 1)

    split = tracker.try_split(0)
    self.assertIsNotNone(split)
    primary, residual = split
    self.assertTrue(primary.is_done)
    self.assertFalse(residual.is_done)
    # Resume / finalize channel separation: primary carries only the
    # finalize hook, residual carries only the resume state.
    self.assertIsNone(primary.checkpoint_mark)
    self.assertIsNotNone(primary.finalization_checkpoint_mark)
    self.assertEqual(primary.finalization_checkpoint_mark.last_index, 1)
    self.assertEqual(residual.checkpoint_mark.last_index, 1)
    self.assertIsNone(residual.finalization_checkpoint_mark)
    # check_done passes on the (now done) primary.
    self.assertTrue(tracker.check_done())

    # Resuming from the residual continues at index 2.
    resumed = _new_tracker(source, checkpoint=residual.checkpoint_mark)
    self.assertEqual(_claim(resumed)[1][0], 2)

  def test_no_data_returns_sentinel_without_finishing(self):
    tracker = _new_tracker(_NoDataSource())
    claimed, record = _claim(tracker)
    self.assertTrue(claimed)  # not EOF
    self.assertIs(record, _NO_DATA)
    # A self-checkpoint is still possible (poll/resume path).
    self.assertIsNotNone(tracker.try_split(0))

  def test_check_done_raises_when_not_done(self):
    tracker = _new_tracker(CountingSource(3))
    with self.assertRaises(ValueError):
      tracker.check_done()

  def test_is_bounded_false(self):
    self.assertFalse(_new_tracker(CountingSource(3)).is_bounded())


class WatermarkTest(unittest.TestCase):
  def test_set_watermark_is_monotonic(self):
    estimator = ManualWatermarkEstimator(None)
    _set_watermark_if_greater(estimator, Timestamp(5))
    self.assertEqual(estimator.current_watermark(), Timestamp(5))
    # A regression is ignored (would otherwise raise inside set_watermark).
    _set_watermark_if_greater(estimator, Timestamp(3))
    self.assertEqual(estimator.current_watermark(), Timestamp(5))
    _set_watermark_if_greater(estimator, Timestamp(7))
    self.assertEqual(estimator.current_watermark(), Timestamp(7))


class FinalizationTest(unittest.TestCase):
  def test_finalize_checkpoint_invoked(self):
    # Authoritative finalize test at the unit level: the e2e finalize may run in
    # a worker process, so its side effect is not observable from the test.
    # The finalize hook lives on the PRIMARY (commit channel), independent of
    # the residual's resume state.
    finalize_log = []
    source = CountingSource(5, finalize_log=finalize_log)
    tracker = _new_tracker(source)
    _claim(tracker)  # 0
    _claim(tracker)  # 1
    primary, _ = tracker.try_split(0)
    primary.finalization_checkpoint_mark.finalize_checkpoint()
    self.assertEqual(finalize_log, [1])


class EndToEndTest(unittest.TestCase):
  def test_direct_runner_emits_all_in_order(self):
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(CountingSource(5))
      self.assertFalse(out.is_bounded)
      assert_that(out, equal_to([0, 1, 2, 3, 4]))

  def test_eof_lets_event_time_window_fire(self):
    # Regression for the EOF-watermark fix: the DoFn must advance the watermark
    # estimator to MAX_TIMESTAMP on the terminal claim so downstream FixedWindow
    # closes. Without that advance the GBK below never fires and assert_that
    # observes an empty output.
    with TestPipeline() as p:
      out = (
          p
          | ReadFromUnboundedSource(CountingSource(5))
          | beam.WindowInto(FixedWindows(100))
          | beam.Map(lambda v: ('all', v))
          | beam.GroupByKey()
          | beam.MapTuple(lambda _key, values: sorted(values)))
      assert_that(out, equal_to([[0, 1, 2, 3, 4]]))

  def test_read_dispatches_through_iobase_read(self):
    # Parity check: `beam.io.Read(unbounded_source)` must produce the same
    # records as `ReadFromUnboundedSource(unbounded_source)`. The dispatch is
    # the elif branch added to iobase.Read.expand().
    with TestPipeline() as p:
      out = p | beam.io.Read(CountingSource(5))
      self.assertFalse(out.is_bounded)
      assert_that(out, equal_to([0, 1, 2, 3, 4]))

  def test_source_default_output_coder_sets_output_type(self):
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(_StringCountingSource(2))
      self.assertEqual(out.element_type, str)
      assert_that(out, equal_to(['v0', 'v1']))


# ------------------------------------------------------------------------------
# Regression tests for the BLOCKER fixes (EOF watermark, reader close on every
# exit path) plus contract regressions (NotImplementedError message,
# finalize_checkpoint idempotency).
# ------------------------------------------------------------------------------


class ReaderCloseTest(unittest.TestCase):
  """Reader lifecycle: close() must run on every tracker-driven exit path."""

  def test_tracker_closes_reader_on_eof(self):
    source = CountingSource(0)  # immediately exhausted
    tracker = _new_tracker(source)
    holder = [None]
    self.assertFalse(tracker.try_claim(holder))
    self.assertIsNone(tracker._reader)
    self.assertTrue(source.last_reader.closed)

  def test_tracker_closes_reader_on_split(self):
    source = CountingSource(5)
    tracker = _new_tracker(source)
    _claim(tracker)  # creates reader, claims 0
    reader = source.last_reader
    self.assertFalse(reader.closed)
    split = tracker.try_split(0)
    self.assertIsNotNone(split)
    self.assertIsNone(tracker._reader)
    self.assertTrue(reader.closed)

  def test_close_helper_is_idempotent_and_safe_on_empty_tracker(self):
    tracker = _new_tracker(CountingSource(3))
    # No reader yet -- helper must be a no-op.
    tracker._close_reader_if_open()
    _claim(tracker)
    reader = tracker._reader
    tracker._close_reader_if_open()
    self.assertTrue(reader.closed)
    self.assertIsNone(tracker._reader)
    # Second call is a no-op (no reader to close).
    tracker._close_reader_if_open()

  def test_close_helper_swallows_reader_close_errors(self):
    class _BoomReader(UnboundedReader):
      def start(self):
        return True

      def advance(self):
        return False

      def get_current(self):
        return 'x'

      def get_current_timestamp(self):
        return Timestamp(0)

      def get_watermark(self):
        return Timestamp(0)

      def get_checkpoint_mark(self):
        return CheckpointMark()

      def close(self):
        raise RuntimeError('close blew up')

    class _BoomSource(UnboundedSource):
      def split(self, desired_num_splits, options=None):
        return [self]

      def create_reader(self, options, checkpoint_mark):
        return _BoomReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    tracker = _new_tracker(_BoomSource())
    _claim(tracker)
    # Helper must not propagate the reader's close() exception, otherwise the
    # DoFn's finally / split paths would mask the original error.
    tracker._close_reader_if_open()
    self.assertIsNone(tracker._reader)


class BestPracticeRegressionTest(unittest.TestCase):
  """Regression guards for the round-2 best-practice fixes:
    B1: data-path watermark uses source.get_watermark(), not record event time
    B2: finalization_checkpoint_mark separate from resume checkpoint_mark
    H4: tracker-internal exception close on reader-method failure
  """

  def test_b1_data_path_holder_carries_source_watermark(self):
    """The holder's 3rd slot is the SOURCE's reported watermark, not the
    record's event time. A reader that reports event time 1000 with a source
    watermark of 990 (out-of-order data) must surface 990 to the wrapper, not
    1000.
    """
    class _LaggingReader(UnboundedReader):
      def start(self):
        return True

      def advance(self):
        return False

      def get_current(self):
        return 'rec'

      def get_current_timestamp(self):
        return Timestamp(1000)  # record event time

      def get_watermark(self):
        return Timestamp(990)  # source watermark lags 10us behind

      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

    class _LaggingSource(UnboundedSource):
      def split(self, desired_num_splits, options=None):
        return [self]

      def create_reader(self, options, checkpoint_mark):
        return _LaggingReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    tracker = _new_tracker(_LaggingSource())
    claimed, record = _claim(tracker)
    self.assertTrue(claimed)
    self.assertIsNot(record, _NO_DATA)
    value, record_timestamp, source_watermark = record
    self.assertEqual(value, 'rec')
    self.assertEqual(record_timestamp, Timestamp(1000))
    # Critical: watermark slot is the SOURCE watermark, NOT record timestamp.
    self.assertEqual(source_watermark, Timestamp(990))
    self.assertNotEqual(source_watermark, record_timestamp)

  def test_b2_split_separates_finalize_and_resume_channels(self):
    source = CountingSource(5)
    tracker = _new_tracker(source)
    _claim(tracker)  # claim 0 so reader has progress
    primary, residual = tracker.try_split(0)
    # Primary carries ONLY the finalize hook -- no resume state.
    self.assertIsNone(primary.checkpoint_mark)
    self.assertIsNotNone(primary.finalization_checkpoint_mark)
    self.assertTrue(primary.is_done)
    # Residual carries ONLY the resume state -- no finalize hook (a future
    # bundle that splits THIS residual will produce ITS own finalize mark).
    self.assertIsNotNone(residual.checkpoint_mark)
    self.assertIsNone(residual.finalization_checkpoint_mark)
    self.assertFalse(residual.is_done)
    # The two marks reference the same underlying checkpoint object.
    self.assertEqual(
        primary.finalization_checkpoint_mark.last_index,
        residual.checkpoint_mark.last_index)

  def test_b2_eof_populates_finalize_and_clears_resume(self):
    # EOF transition: restriction.checkpoint_mark goes to None (no more
    # records to resume from), finalization_checkpoint_mark carries the
    # final commit hook.
    source = CountingSource(0)  # immediately exhausted
    tracker = _new_tracker(source)
    holder = [None]
    self.assertFalse(tracker.try_claim(holder))
    r = tracker.current_restriction()
    self.assertTrue(r.is_done)
    self.assertEqual(r.watermark, MAX_TIMESTAMP)
    self.assertIsNone(r.checkpoint_mark)
    self.assertIsNotNone(r.finalization_checkpoint_mark)

  def test_h4_tracker_closes_reader_when_advance_raises(self):
    # If reader.advance() raises, the tracker's try_claim wraps it and
    # closes the reader BEFORE re-raising. The DoFn's finally does not need
    # to traverse the private SDF chain for reader-method failures.
    class _BoomReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      def start(self):
        return True

      def advance(self):
        raise RuntimeError('advance boom')

      def get_current(self):
        return 'first'

      def get_current_timestamp(self):
        return Timestamp(0)

      def get_watermark(self):
        return Timestamp(0)

      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

      def close(self):
        self.closed = True

    class _BoomSource(UnboundedSource):
      def split(self, desired_num_splits, options=None):
        return [self]

      def create_reader(self, options, checkpoint_mark):
        return _BoomReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    src = _BoomSource()
    tracker = _new_tracker(src)
    # First claim succeeds (start returns True).
    self.assertTrue(tracker.try_claim([None]))
    reader_after_first = tracker._reader
    self.assertIsNotNone(reader_after_first)
    # Second claim invokes advance() which raises. Tracker must close the
    # reader before propagating the exception.
    with self.assertRaises(RuntimeError):
      tracker.try_claim([None])
    self.assertTrue(reader_after_first.closed)
    self.assertIsNone(tracker._reader)

  def test_h4_tracker_closes_reader_when_get_watermark_raises(self):
    # Reader method failures other than advance() also trigger close.
    class _WatermarkBoomReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      def start(self):
        return False  # no data -> drops into get_watermark path

      def advance(self):
        return False

      def get_current(self):
        raise AssertionError

      def get_current_timestamp(self):
        raise AssertionError

      def get_watermark(self):
        raise RuntimeError('watermark boom')

      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

      def close(self):
        self.closed = True

    class _WatermarkBoomSource(UnboundedSource):
      def split(self, desired_num_splits, options=None):
        return [self]

      def create_reader(self, options, checkpoint_mark):
        return _WatermarkBoomReader()

      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    src = _WatermarkBoomSource()
    tracker = _new_tracker(src)
    with self.assertRaises(RuntimeError):
      tracker.try_claim([None])
    self.assertIsNone(tracker._reader)


class UnboundedSourceContractTest(unittest.TestCase):
  def test_get_checkpoint_mark_coder_default_names_subclass(self):
    class MySource(UnboundedSource):
      pass

    with self.assertRaises(NotImplementedError) as cm:
      MySource().get_checkpoint_mark_coder()
    self.assertIn('MySource', str(cm.exception))

  def test_default_finalize_is_idempotent(self):
    mark = CheckpointMark()
    # Default no-op must tolerate repeated invocation; the SDK's bundle
    # finalizer makes no exactly-once guarantee on this callback.
    self.assertIsNone(mark.finalize_checkpoint())
    self.assertIsNone(mark.finalize_checkpoint())


class ReadFromUnboundedSourceValidationTest(unittest.TestCase):
  def test_non_source_argument_raises(self):
    with self.assertRaises(TypeError):
      ReadFromUnboundedSource('not-a-source')  # type: ignore[arg-type]

  def test_poll_interval_must_be_positive(self):
    src = CountingSource(3)
    with self.assertRaises(ValueError):
      ReadFromUnboundedSource(src, poll_interval_seconds=0)
    with self.assertRaises(ValueError):
      ReadFromUnboundedSource(src, poll_interval_seconds=-1)
    with self.assertRaises(ValueError):
      ReadFromUnboundedSource(src, poll_interval_seconds=-0.5)
    # Positive values OK.
    ReadFromUnboundedSource(src, poll_interval_seconds=0.001)
    ReadFromUnboundedSource(src, poll_interval_seconds=60)


class CloudpicklePicklabilityTest(unittest.TestCase):
  """The DoFn class is defined inside ``ReadFromUnboundedSource.expand`` so it
  can close over the source-specific provider. Beam's default pickler is
  cloudpickle; stdlib pickle would fail on a closure-defined class. This is a
  regression guard for cross-runner portability (Dataflow / Flink portable
  workers also use cloudpickle).
  """

  def test_transform_round_trips_through_cloudpickle(self):
    from apache_beam.internal import pickler
    transform = ReadFromUnboundedSource(CountingSource(5))
    blob = pickler.dumps(transform)
    self.assertIsInstance(blob, bytes)
    restored = pickler.loads(blob)
    self.assertIsInstance(restored, ReadFromUnboundedSource)

  def test_source_object_round_trips_through_cloudpickle(self):
    from apache_beam.internal import pickler
    src = CountingSource(5)
    restored = pickler.loads(pickler.dumps(src))
    self.assertIsInstance(restored, CountingSource)
    self.assertEqual(restored._count, 5)


class CircularImportOrderTest(unittest.TestCase):
  """`iobase.py` and `unbounded_source.py` form a cycle (UnboundedSource extends
  iobase.SourceBase; iobase.Read.expand lazy-imports unbounded_source). All
  three import-order scenarios must complete without ImportError. Subprocesses
  ensure each test starts from a clean module cache.
  """

  def _run_in_subprocess(self, script):
    import subprocess
    import sys
    import os
    env = os.environ.copy()
    beam_python = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(
            __file__)))))
    env['PYTHONPATH'] = beam_python + os.pathsep + env.get('PYTHONPATH', '')
    fd, path = tempfile.mkstemp(suffix='.py')
    try:
      with os.fdopen(fd, 'w') as fp:
        fp.write(script)
      return subprocess.run(
          [sys.executable, path],
          capture_output=True,
          text=True,
          env=env,
          timeout=60)
    finally:
      if os.path.exists(path):
        os.unlink(path)

  def test_iobase_then_unbounded_source(self):
    result = self._run_in_subprocess(
        'import apache_beam.io.iobase\n'
        'import apache_beam.io.unbounded_source\n'
        'print("ok")\n')
    self.assertEqual(
        result.returncode, 0,
        'stderr=%r stdout=%r' % (result.stderr, result.stdout))
    self.assertIn('ok', result.stdout)

  def test_unbounded_source_then_iobase(self):
    result = self._run_in_subprocess(
        'import apache_beam.io.unbounded_source\n'
        'import apache_beam.io.iobase\n'
        'print("ok")\n')
    self.assertEqual(
        result.returncode, 0,
        'stderr=%r stdout=%r' % (result.stderr, result.stdout))
    self.assertIn('ok', result.stdout)

  def test_read_expand_lazy_imports_unbounded_source(self):
    # Import only iobase, then trigger Read.expand() on an UnboundedSource.
    # The expand() must lazy-import unbounded_source without ImportError and
    # produce a valid expanded transform tree.
    script = '''
import sys
import apache_beam as beam
from apache_beam import coders
import apache_beam.io.iobase as iobase
# Now import unbounded_source AFTER iobase, then verify Read.expand
# successfully lazy-imports ReadFromUnboundedSource:
from apache_beam.io.unbounded_source import UnboundedSource

class _S(UnboundedSource):
  def split(self, n, options=None):
    return [self]
  def create_reader(self, o, cp):
    return None
  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()

r = iobase.Read(_S())
p = beam.Pipeline()
result = r.expand(p)
assert not result.is_bounded, 'expanded PCollection should be unbounded'
print("ok")
'''
    result = self._run_in_subprocess(script)
    self.assertEqual(
        result.returncode, 0,
        'stderr=%r stdout=%r' % (result.stderr, result.stdout))
    self.assertIn('ok', result.stdout)


class DoFnReaderCloseOnDownstreamRaiseTest(unittest.TestCase):
  """H4 second half: tracker-internal exception close (already tested in
  ``BestPracticeRegressionTest.test_h4_*``) handles reader-method failures.
  This test covers the OTHER half -- the source is well-behaved but the
  downstream output handler raises, so the exception happens AFTER
  ``try_claim`` returned with a live reader. Beam's
  ``common._OutputHandler.handle_process_outputs`` iterates the DoFn's
  generator with ``for result in results`` and calls
  ``receiver.receive(...)``; when a downstream receiver raises, the
  exception is OUTSIDE the user generator. The SDK harness then drops
  the generator (no explicit ``throw``); the generator's ``finally`` runs
  when the generator is closed (``GeneratorExit``) or garbage collected.

  We exercise that path two ways:
    1. Unit-level: simulate the harness drop with ``generator.close()``
       (raises ``GeneratorExit`` at the active yield, running ``finally``).
    2. Integration: run a real pipeline with a downstream ``Map`` that
       raises, and confirm the reader was closed before the pipeline
       surfaced the error.
  """

  def test_dofn_finally_closes_reader_on_generator_close(self):
    marker = _new_marker_path('.gen_close.log')
    try:
      source = _MarkerCloseSource(marker)
      p = beam.Pipeline()
      out = p | ReadFromUnboundedSource(source)
      dofn = out.producer.transform.fn
      inner_tracker = _UnboundedSourceRestrictionTracker(
          _UnboundedSourceRestriction(source=source))
      tracker = sdf_utils.RestrictionTrackerView(
          sdf_utils.ThreadsafeRestrictionTracker(inner_tracker))
      generator = dofn.process(
          None,
          bundle_finalizer=core.DoFn.BundleFinalizerParam(),
          tracker=tracker,
          watermark_estimator=ManualWatermarkEstimator(None))

      next(generator)
      # Simulate the harness dropping the generator after a downstream
      # receiver raised. Beam's SDK harness does NOT call
      # ``generator.throw`` -- the downstream exception happens outside
      # the user generator, and the harness lets GC / ``close`` clean up.
      generator.close()
      self.assertTrue(
          _wait_for_marker(marker),
          'DoFn finally did not invoke reader.close() when the generator '
          'was closed (GeneratorExit) -- reader leaked. Private-chain '
          'close in unbounded_source.py:expand finally may be broken.')
    finally:
      if os.path.exists(marker):
        os.unlink(marker)

  def test_dofn_finally_closes_reader_via_integration_pipeline(self):
    """End-to-end harness coverage: a real pipeline with a downstream
    ``Map`` that raises must surface the exception AND must have closed
    the reader. This complements the unit-level ``generator.close`` test
    above by exercising the actual SDK harness output-handler path
    (``common._OutputHandler.handle_process_outputs``).
    """
    marker = _new_marker_path('.integration_close.log')
    try:
      raised = False
      try:
        with beam.Pipeline() as p:
          _ = (
              p
              | ReadFromUnboundedSource(_MarkerCloseSource(marker))
              | 'BoomMap' >> beam.Map(_downstream_boom))
      except Exception:  # pylint: disable=broad-except
        raised = True
      self.assertTrue(
          raised,
          'pipeline did not surface the downstream Map exception')
      self.assertTrue(
          _wait_for_marker(marker),
          'reader leaked across the integration pipeline -- the SDK '
          'harness path that drops the DoFn generator on downstream '
          'failure did not trigger our finally close.')
    finally:
      if os.path.exists(marker):
        os.unlink(marker)


# ------------------------------------------------------------------------------
# Stronger regression guards (added after independent second-opinion review).
# The windowed e2e test above is suggestive but not bulletproof, because the
# FnApiRunner watermark manager advances PCollection watermarks to MAX once a
# bundle has no deferred work (fn_runner.py ~819 and ~969). These tests probe
# the DoFn-level behavior directly via file-based side-channels so the BLOCKER
# fixes cannot regress silently. (In-memory closures don't propagate across
# Beam's cloudpickle worker boundary even when the worker runs in the same
# process, so we go through the filesystem.)
# ------------------------------------------------------------------------------


def _new_marker_path(suffix):
  """Create a fresh temp file path used as a cross-bundle side-channel.

  Returns a path that does NOT exist (deleted after mkstemp). The DoFn-side
  code writes to it; the test reads it back.
  """
  fd, path = tempfile.mkstemp(suffix=suffix)
  os.close(fd)
  os.unlink(path)
  return path


def _wait_for_marker(path, timeout_secs=5):
  deadline = time.time() + timeout_secs
  while time.time() < deadline:
    gc.collect()
    if os.path.exists(path):
      return True
    time.sleep(0.05)
  return os.path.exists(path)


class DoFnWatermarkAdvanceTest(unittest.TestCase):
  """B-1 regression: the DoFn MUST advance the watermark estimator to
  MAX_TIMESTAMP on the terminal claim, not rely on the runner's auto-advance.
  """

  def test_eof_invokes_set_watermark_with_max_timestamp(self):
    marker = _new_marker_path('.watermarks.log')

    original = _unbounded_source_module._set_watermark_if_greater

    def _recording(estimator, watermark):
      # File side-channel: closure variables are deep-copied across Beam's
      # bundle boundary even in embedded FnApiRunner; the filesystem is the
      # only reliable cross-bundle assertion target.
      with open(marker, 'a') as fp:
        fp.write(repr(watermark) + '\n')
      return original(estimator, watermark)

    _unbounded_source_module._set_watermark_if_greater = _recording
    try:
      with TestPipeline() as p:
        _ = p | ReadFromUnboundedSource(CountingSource(3))
    finally:
      _unbounded_source_module._set_watermark_if_greater = original

    try:
      with open(marker) as fp:
        lines = fp.read().splitlines()
    finally:
      if os.path.exists(marker):
        os.unlink(marker)

    self.assertIn(
        repr(MAX_TIMESTAMP),
        lines,
        '_set_watermark_if_greater was never called with MAX_TIMESTAMP -- '
        'the EOF branch in process() is not advancing the estimator. '
        'Captured calls: %r' % (lines, ))


class DoFnReaderCloseOnExceptionTest(unittest.TestCase):
  """B-2 regression: the DoFn's ``finally`` MUST close the reader even when
  ``process()`` raises mid-bundle, otherwise we leak sockets/fds in production.
  """

  def test_reader_close_runs_when_process_raises(self):
    marker = _new_marker_path('.close.log')
    try:
      raised = False
      try:
        with beam.Pipeline() as p:
          _ = p | ReadFromUnboundedSource(_RaisingSource(marker))
      except Exception:  # pylint: disable=broad-except
        raised = True
      self.assertTrue(
          raised, 'pipeline did not surface the reader.advance() exception')
      # Generator finalisation (which runs the DoFn's ``finally``) may be
      # deferred inside Beam's bundle processor; wait briefly so the
      # close-marker is observable in slow test environments.
      self.assertTrue(
          _wait_for_marker(marker),
          'DoFn finally did not invoke reader.close() on the exception path '
          '-- reader leaked.')
    finally:
      if os.path.exists(marker):
        os.unlink(marker)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
