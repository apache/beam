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

import logging
import unittest

from apache_beam import coders
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import ReadFromUnboundedSource
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.io.unbounded_source import _NO_DATA
from apache_beam.io.unbounded_source import _UnboundedSourceRestriction
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionCoder
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionTracker
from apache_beam.io.unbounded_source import _set_watermark_if_greater
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
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


class CountingSource(UnboundedSource):
  def __init__(self, count, finalize_log=None):
    self._count = count
    self._finalize_log = finalize_log

  def split(self, desired_num_splits, options=None):
    return [self]

  def create_reader(self, options, checkpoint_mark):
    start_index = 0 if checkpoint_mark is None else checkpoint_mark.last_index + 1
    return _CountingReader(
        self._count, start_index, finalize_log=self._finalize_log)

  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


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
    self.assertEqual(residual.checkpoint_mark.last_index, 1)
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
    finalize_log = []
    source = CountingSource(5, finalize_log=finalize_log)
    tracker = _new_tracker(source)
    _claim(tracker)  # 0
    _claim(tracker)  # 1
    _, residual = tracker.try_split(0)
    residual.checkpoint_mark.finalize_checkpoint()
    self.assertEqual(finalize_log, [1])


class EndToEndTest(unittest.TestCase):
  def test_direct_runner_emits_all_in_order(self):
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(CountingSource(5))
      self.assertFalse(out.is_bounded)
      assert_that(out, equal_to([0, 1, 2, 3, 4]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
