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

Semantics are covered by deterministic unit tests; the end-to-end DirectRunner
tests assert ordering and termination only (no flaky defer-timing assertions).
"""

import logging
import unittest

from typing_extensions import override

import apache_beam as beam
from apache_beam import coders
from apache_beam.io import unbounded_source as _unbounded_source_module
from apache_beam.io.unbounded_source import _NO_DATA
from apache_beam.io.unbounded_source import CheckpointMark
from apache_beam.io.unbounded_source import ReadFromUnboundedSource
from apache_beam.io.unbounded_source import UnboundedReader
from apache_beam.io.unbounded_source import UnboundedSource
from apache_beam.io.unbounded_source import _FinalizeCheckpointOnce
from apache_beam.io.unbounded_source import _ReaderCache
from apache_beam.io.unbounded_source import _ReadFromUnboundedSourceDoFn
from apache_beam.io.unbounded_source import _set_watermark_if_greater
from apache_beam.io.unbounded_source import _UnboundedSourceRestriction
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionCoder
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionProvider
from apache_beam.io.unbounded_source import _UnboundedSourceRestrictionTracker
from apache_beam.io.watermark_estimators import ManualWatermarkEstimator
from apache_beam.runners import sdf_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import core
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp

# pylint: disable=expression-not-assigned

# Realistic event-time base away from the Unix epoch.
_EVENT_TIME_BASE = Timestamp(1729987200)  # 2024-10-27T00:00:00Z

# ------------------------------------------------------------------------------
# In-memory demo source emitting integers 0..count-1 with event time
# ``_EVENT_TIME_BASE + index``. It self-terminates at EOF, resumes from
# ``last_index + 1``, and splits into even/odd sub-sources when configured.
# ------------------------------------------------------------------------------


class _CountingCheckpointMark(CheckpointMark):
  def __init__(self, last_index, finalize_log=None):
    self.last_index = last_index
    self._finalize_log = finalize_log

  @override
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
  def __init__(
      self, count, start_index, finalize_log=None, modulus=1, residue=0):
    self._count = count
    self._next = start_index
    self._modulus = modulus
    self._residue = residue
    self._current = None
    self._exhausted = False
    self._finalize_log = finalize_log
    self.closed = False

  def _read_next(self):
    while self._next < self._count:
      index = self._next
      self._next += 1
      if index % self._modulus == self._residue:
        self._current = index
        return True
    self._exhausted = True
    return False

  @override
  def start(self):
    return self._read_next()

  @override
  def advance(self):
    return self._read_next()

  @override
  def get_current(self):
    return self._current

  @override
  def get_current_timestamp(self):
    return _EVENT_TIME_BASE + self._current

  @override
  def get_watermark(self):
    if self._exhausted:
      return MAX_TIMESTAMP
    if self._current is None:
      return MIN_TIMESTAMP
    return _EVENT_TIME_BASE + self._current

  @override
  def get_checkpoint_mark(self):
    last = self._current if self._current is not None else self._next - 1
    return _CountingCheckpointMark(last, finalize_log=self._finalize_log)

  @override
  def close(self):
    self.closed = True


class UnboundedCountingSource(UnboundedSource):
  def __init__(
      self,
      count,
      finalize_log=None,
      is_splittable=False,
      modulus=1,
      residue=0):
    self._count = count
    self._finalize_log = finalize_log
    self._is_splittable = is_splittable
    self._modulus = modulus
    self._residue = residue
    self.last_reader = None

  @override
  def split(self, desired_num_splits, options=None):
    if not self._is_splittable or desired_num_splits < 2:
      return [self]
    # Split into independent even/odd sub-sources (each non-splittable).
    return [
        UnboundedCountingSource(
            self._count,
            finalize_log=self._finalize_log,
            modulus=2,
            residue=residue) for residue in (0, 1)
    ]

  @override
  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _CountingReader(
        self._count,
        start_index,
        finalize_log=self._finalize_log,
        modulus=self._modulus,
        residue=self._residue)
    return self.last_reader

  @override
  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class _StringCountingReader(_CountingReader):
  @override
  def get_current(self):
    return 'v%s' % self._current


class _StringCountingSource(UnboundedCountingSource):
  @override
  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _StringCountingReader(
        self._count, start_index, finalize_log=self._finalize_log)
    return self.last_reader

  @override
  def default_output_coder(self):
    return coders.StrUtf8Coder()


class _PrefixStrCoder(coders.Coder):
  def __init__(self, prefix):
    self._prefix = prefix

  @override
  def encode(self, value):
    if not value.startswith(self._prefix):
      raise ValueError('expected %r prefix' % self._prefix)
    return value[len(self._prefix):].encode('utf-8')

  @override
  def decode(self, value):
    return self._prefix + value.decode('utf-8')

  @override
  def is_deterministic(self):
    return True

  @override
  def to_type_hint(self):
    return str


class _PrefixStringReader(_StringCountingReader):
  @override
  def get_current(self):
    return 'prefix:%s' % super().get_current()


class _PrefixStringSource(_StringCountingSource):
  @override
  def create_reader(self, options, checkpoint_mark):
    start_index = (
        0 if checkpoint_mark is None else checkpoint_mark.last_index + 1)
    self.last_reader = _PrefixStringReader(
        self._count, start_index, finalize_log=self._finalize_log)
    return self.last_reader

  @override
  def default_output_coder(self):
    return _PrefixStrCoder('prefix:')


class _NoDataReader(UnboundedReader):
  """Always reports temporary absence of data with watermark below MAX."""
  @override
  def start(self):
    return False

  @override
  def advance(self):
    return False

  @override
  def get_current(self):
    raise AssertionError('no data available')

  @override
  def get_current_timestamp(self):
    raise AssertionError('no data available')

  @override
  def get_watermark(self):
    return _EVENT_TIME_BASE

  @override
  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(-1)


class _NoDataSource(UnboundedSource):
  @override
  def split(self, desired_num_splits, options=None):
    return [self]

  @override
  def create_reader(self, options, checkpoint_mark):
    return _NoDataReader()

  @override
  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class _MutatingCheckpointMark(CheckpointMark):
  """A mark that mutates itself on finalize, to test primary/residual mark
  isolation across a checkpoint cut."""
  def __init__(self, last_index):
    self.last_index = last_index

  @override
  def finalize_checkpoint(self):
    self.last_index = -999


class _MutatingReader(UnboundedReader):
  def __init__(self):
    self._index = -1

  @override
  def start(self):
    self._index = 0
    return True

  @override
  def advance(self):
    self._index += 1
    return True

  @override
  def get_current(self):
    return self._index

  @override
  def get_current_timestamp(self):
    return _EVENT_TIME_BASE + self._index

  @override
  def get_watermark(self):
    return _EVENT_TIME_BASE + self._index

  @override
  def get_checkpoint_mark(self):
    return _MutatingCheckpointMark(self._index)


class _MutatingSource(UnboundedSource):
  @override
  def split(self, desired_num_splits, options=None):
    return [self]

  @override
  def create_reader(self, options, checkpoint_mark):
    return _MutatingReader()

  @override
  def get_checkpoint_mark_coder(self):
    return coders.PickleCoder()


class _MaxOnLastReader(UnboundedReader):
  """Returns its only record with a MAX_TIMESTAMP watermark on the same claim,
  then EOF on the next claim."""
  @override
  def start(self):
    return True

  @override
  def advance(self):
    return False

  @override
  def get_current(self):
    return 7

  @override
  def get_current_timestamp(self):
    return _EVENT_TIME_BASE

  @override
  def get_watermark(self):
    return MAX_TIMESTAMP

  @override
  def get_checkpoint_mark(self):
    return _CountingCheckpointMark(0)


class _MaxOnLastSource(UnboundedSource):
  @override
  def split(self, desired_num_splits, options=None):
    return [self]

  @override
  def create_reader(self, options, checkpoint_mark):
    return _MaxOnLastReader()

  @override
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
    self.assertFalse(UnboundedCountingSource(3).is_bounded())

  def test_reader_lifecycle_start_advance_eof(self):
    reader = UnboundedCountingSource(3).create_reader(None, None)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 0)
    self.assertEqual(reader.get_current_timestamp(), _EVENT_TIME_BASE)
    self.assertTrue(reader.advance())
    self.assertEqual(reader.get_current(), 1)
    self.assertTrue(reader.advance())
    self.assertEqual(reader.get_current(), 2)
    self.assertFalse(reader.advance())
    self.assertEqual(reader.get_watermark(), MAX_TIMESTAMP)


class RestrictionCoderTest(unittest.TestCase):
  def test_roundtrip_no_checkpoint(self):
    source = UnboundedCountingSource(3)
    coder = _UnboundedSourceRestrictionCoder()
    decoded = coder.decode(
        coder.encode(_UnboundedSourceRestriction(source=source)))
    self.assertIsNone(decoded.checkpoint_mark)
    self.assertEqual(decoded.watermark, MIN_TIMESTAMP)
    self.assertFalse(decoded.is_done)
    reader = decoded.source.create_reader(None, None)
    self.assertTrue(reader.start())
    self.assertEqual(reader.get_current(), 0)

  def test_roundtrip_with_checkpoint_resumes(self):
    source = UnboundedCountingSource(5)
    coder = _UnboundedSourceRestrictionCoder()
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

    class _NamedSource(UnboundedCountingSource):
      def __init__(self, name):
        super().__init__(0)
        self.name = name

      @override
      def split(self, desired_num_splits, options=None):
        split_log.append((desired_num_splits, options))
        return [_NamedSource('a'), _NamedSource('b')]

    source = _NamedSource('root')
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(
        source=source, watermark=Timestamp(7))

    splits = list(provider.split(source, restriction))

    # The provider is a stateless module-level singleton, so it always
    # passes ``None`` as the ``options`` argument to ``UnboundedSource.split``.
    self.assertEqual(split_log, [(20, None)])
    self.assertEqual([split.source.name for split in splits], ['a', 'b'])
    self.assertEqual([split.watermark for split in splits], [Timestamp(7)] * 2)
    self.assertTrue(all(split.checkpoint_mark is None for split in splits))
    self.assertTrue(
        all(split.finalization_checkpoint_mark is None for split in splits))

  def test_initial_split_does_not_split_checkpointed_restriction(self):
    split_log = []

    class _SplitSource(UnboundedCountingSource):
      @override
      def split(self, desired_num_splits, options=None):
        split_log.append((desired_num_splits, options))
        return [self]

    source = _SplitSource(5)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(
        source=source, checkpoint_mark=_CountingCheckpointMark(2))

    self.assertEqual(list(provider.split(source, restriction)), [restriction])
    self.assertEqual(split_log, [])

  def test_initial_split_falls_back_to_original_on_split_error(self):
    class _BoomSource(UnboundedCountingSource):
      @override
      def split(self, desired_num_splits, options=None):
        raise RuntimeError('split boom')

    source = _BoomSource(5)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(source=source)

    self.assertEqual(list(provider.split(source, restriction)), [restriction])

  def test_truncate_returns_none_for_drain(self):
    # On drain the SDF stops emitting; truncate yields no residual.
    provider = _UnboundedSourceRestrictionProvider()
    source = UnboundedCountingSource(5)
    restriction = _UnboundedSourceRestriction(source=source)
    self.assertIsNone(provider.truncate(source, restriction))

  def test_splittable_source_partitions_into_independent_subsources(self):
    # A splittable source fans out into two sub-sources; reading each in
    # isolation yields the even and the odd integers, and their union is the
    # full sequence with no overlap.
    source = UnboundedCountingSource(6, is_splittable=True)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = _UnboundedSourceRestriction(source=source)

    splits = list(provider.split(source, restriction))
    self.assertEqual(len(splits), 2)

    shards = []
    for split in splits:
      tracker = _UnboundedSourceRestrictionTracker(split)
      shard = []
      while True:
        claimed, record = _claim(tracker)
        if not claimed:
          break
        if record is not _NO_DATA:
          shard.append(record[0])
      shards.append(shard)
    self.assertEqual(sorted(shards), [[0, 2, 4], [1, 3, 5]])


class RestrictionTrackerTest(unittest.TestCase):
  def test_claim_emits_in_order(self):
    tracker = _new_tracker(UnboundedCountingSource(3))
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
    # A reader may return its last record with a MAX_TIMESTAMP watermark on the
    # same call; the record must still be emitted (EOF comes on the next claim).
    class _FinalRecordReader(UnboundedReader):
      @override
      def start(self):
        return True

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        return 'last'

      @override
      def get_current_timestamp(self):
        return _EVENT_TIME_BASE

      @override
      def get_watermark(self):
        return MAX_TIMESTAMP

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

    class _FinalSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _FinalRecordReader()

      @override
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
    source = UnboundedCountingSource(5)
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

  def test_try_split_isolates_residual_from_finalize_mutation(self):
    # The primary's finalize hook and the residual's resume state must not
    # share one object, so a mutating finalize_checkpoint() cannot corrupt the
    # residual's resume position.
    tracker = _new_tracker(_MutatingSource())
    _claim(tracker)  # start -> index 0
    split = tracker.try_split(0)
    self.assertIsNotNone(split)
    primary, residual = split
    self.assertIsNot(
        primary.finalization_checkpoint_mark, residual.checkpoint_mark)
    self.assertEqual(residual.checkpoint_mark.last_index, 0)
    primary.finalization_checkpoint_mark.finalize_checkpoint()
    self.assertEqual(residual.checkpoint_mark.last_index, 0)

  def test_try_split_nonzero_declined(self):
    source = UnboundedCountingSource(5)
    tracker = _new_tracker(source)
    self.assertEqual(_claim(tracker)[1][0], 0)

    self.assertIsNone(tracker.try_split(0.5))
    self.assertFalse(tracker.current_restriction().is_done)
    self.assertIsNotNone(tracker._reader)
    self.assertEqual(_claim(tracker)[1][0], 1)

  def test_no_data_returns_sentinel_without_finishing(self):
    tracker = _new_tracker(_NoDataSource())
    claimed, record = _claim(tracker)
    self.assertTrue(claimed)
    self.assertIs(record, _NO_DATA)
    # A self-checkpoint is still possible (poll/resume path).
    self.assertIsNotNone(tracker.try_split(0))

  def test_check_done_raises_when_not_done(self):
    tracker = _new_tracker(UnboundedCountingSource(3))
    with self.assertRaises(ValueError):
      tracker.check_done()

  def test_is_bounded_false(self):
    self.assertFalse(_new_tracker(UnboundedCountingSource(3)).is_bounded())


class _RecordingBundleFinalizer:
  def __init__(self):
    self.registered = []

  def register(self, callback):
    self.registered.append(callback)


class _ManualClock:
  """A deterministic monotonic clock for the time-cap tests."""
  def __init__(self, now=0.0):
    self.now = now

  def __call__(self):
    return self.now


class BundleCapTest(unittest.TestCase):
  """A busy reader self-checkpoints once the per-bundle record or time cap is
  reached, so the runner can commit progress and run finalization."""
  def _bundle(self, dofn, source, checkpoint=None, estimator=None):
    """Builds the SDF tracker chain and returns the process() generator plus the
    tracker, threadsafe tracker, finalizer, and watermark estimator."""
    tracker = _UnboundedSourceRestrictionTracker(
        _UnboundedSourceRestriction(source=source, checkpoint_mark=checkpoint))
    threadsafe = sdf_utils.ThreadsafeRestrictionTracker(tracker)
    view = sdf_utils.RestrictionTrackerView(threadsafe)
    finalizer = _RecordingBundleFinalizer()
    estimator = estimator or ManualWatermarkEstimator(None)
    gen = dofn.process(
        None,
        bundle_finalizer=finalizer,
        tracker=view,
        watermark_estimator=estimator)
    return gen, tracker, threadsafe, finalizer, estimator

  def test_record_cap_checkpoints_busy_source(self):
    finalize_log = []
    dofn = _ReadFromUnboundedSourceDoFn(
        poll_interval=0, max_records_per_bundle=5, max_read_time_seconds=1e9)
    # 1000 records is effectively unbounded against a cap of 5.
    gen, tracker, threadsafe, finalizer, estimator = self._bundle(
        dofn, UnboundedCountingSource(1000, finalize_log=finalize_log))
    outputs = list(gen)

    self.assertEqual([tv.value for tv in outputs], [0, 1, 2, 3, 4])
    self.assertTrue(tracker.current_restriction().is_done)
    self.assertTrue(tracker.check_done())
    # The estimator holds the last emitted record's source watermark.
    self.assertEqual(estimator.current_watermark(), _EVENT_TIME_BASE + 4)
    # Residual resumes after the cut and carries no finalize hook.
    residual, _ = threadsafe.deferred_status()
    self.assertEqual(residual.checkpoint_mark.last_index, 4)
    self.assertIsNone(residual.finalization_checkpoint_mark)
    # Exactly one finalizer is registered; firing it commits the cut index once.
    self.assertEqual(len(finalizer.registered), 1)
    finalizer.registered[0]()
    finalizer.registered[0]()
    self.assertEqual(finalize_log, [4])

  def test_time_cap_checkpoints_busy_source(self):
    clock = _ManualClock(1000.0)
    dofn = _ReadFromUnboundedSourceDoFn(
        poll_interval=0,
        max_records_per_bundle=10**9,
        max_read_time_seconds=5.0,
        _now=clock)
    gen, tracker, threadsafe, _, _ = self._bundle(
        dofn, UnboundedCountingSource(1000))

    # The deadline arms at 1000 + 5 after the first record and is checked
    # between records, so records keep flowing until the clock passes it.
    self.assertEqual(next(gen).value, 0)
    self.assertEqual(next(gen).value, 1)
    self.assertEqual(next(gen).value, 2)
    clock.now = 1006.0
    with self.assertRaises(StopIteration):
      next(gen)

    self.assertTrue(tracker.current_restriction().is_done)
    residual, _ = threadsafe.deferred_status()
    self.assertEqual(residual.checkpoint_mark.last_index, 2)

  def test_cap_residual_resumes_in_next_bundle(self):
    dofn = _ReadFromUnboundedSourceDoFn(
        poll_interval=0, max_records_per_bundle=5, max_read_time_seconds=1e9)
    source = UnboundedCountingSource(1000)
    # Bundle 1 emits 0-4 and cuts a residual at index 4.
    gen1, _, threadsafe1, _, _ = self._bundle(dofn, source)
    self.assertEqual([tv.value for tv in gen1], [0, 1, 2, 3, 4])
    residual1, _ = threadsafe1.deferred_status()

    # Bundle 2 rebuilds the reader from the residual and emits 5-9.
    gen2, _, threadsafe2, _, _ = self._bundle(
        dofn, source, checkpoint=residual1.checkpoint_mark)
    self.assertEqual([tv.value for tv in gen2], [5, 6, 7, 8, 9])
    residual2, _ = threadsafe2.deferred_status()
    self.assertEqual(residual2.checkpoint_mark.last_index, 9)

  def test_busy_reader_is_reused_across_bundles(self):
    # The self-checkpoint parks the reader; the resuming bundle reclaims the
    # same started reader.
    dofn = _ReadFromUnboundedSourceDoFn(
        poll_interval=0, max_records_per_bundle=5, max_read_time_seconds=1e9)
    dofn.setup()  # creates the cross-bundle reader cache
    source = UnboundedCountingSource(1000)
    gen1, _, threadsafe1, _, _ = self._bundle(dofn, source)
    self.assertEqual([tv.value for tv in gen1], [0, 1, 2, 3, 4])
    reader1 = source.last_reader
    self.assertFalse(reader1.closed)  # parked, not closed
    residual1, _ = threadsafe1.deferred_status()

    gen2, _, _, _, _ = self._bundle(
        dofn, source, checkpoint=residual1.checkpoint_mark)
    self.assertEqual([tv.value for tv in gen2], [5, 6, 7, 8, 9])
    # No new reader was created; source.last_reader is unchanged.
    self.assertIs(source.last_reader, reader1)

  def test_teardown_closes_parked_readers(self):
    dofn = _ReadFromUnboundedSourceDoFn(
        poll_interval=0, max_records_per_bundle=5, max_read_time_seconds=1e9)
    dofn.setup()
    source = UnboundedCountingSource(1000)
    gen, _, _, _, _ = self._bundle(dofn, source)
    list(gen)  # the bundle parks its reader
    reader = source.last_reader
    self.assertFalse(reader.closed)
    dofn.teardown()
    self.assertTrue(reader.closed)

  def test_eof_exactly_at_cap_resumes_then_finishes(self):
    dofn = _ReadFromUnboundedSourceDoFn(
        poll_interval=0, max_records_per_bundle=5, max_read_time_seconds=1e9)
    source = UnboundedCountingSource(5)  # exactly cap records
    # Bundle 1 hits the cap on the last record before observing EOF.
    gen1, t1, threadsafe1, _, _ = self._bundle(dofn, source)
    self.assertEqual([tv.value for tv in gen1], [0, 1, 2, 3, 4])
    self.assertTrue(t1.current_restriction().is_done)
    residual1, _ = threadsafe1.deferred_status()
    self.assertEqual(residual1.checkpoint_mark.last_index, 4)

    # Bundle 2 resumes at index 5, finds EOF, and finishes with no output.
    gen2, t2, threadsafe2, _, _ = self._bundle(
        dofn, source, checkpoint=residual1.checkpoint_mark)
    self.assertEqual(list(gen2), [])
    self.assertTrue(t2.current_restriction().is_done)
    self.assertIsNone(threadsafe2.deferred_status())

  def test_eof_before_cap_finishes_without_residual(self):
    dofn = _ReadFromUnboundedSourceDoFn(
        poll_interval=0, max_records_per_bundle=100, max_read_time_seconds=1e9)
    gen, tracker, threadsafe, _, _ = self._bundle(
        dofn, UnboundedCountingSource(3))

    self.assertEqual([tv.value for tv in gen], [0, 1, 2])
    self.assertTrue(tracker.current_restriction().is_done)
    self.assertIsNone(threadsafe.deferred_status())

  def test_max_watermark_on_final_record_emitted_before_estimator_advances(
      self):
    # A reader that returns its final record with a MAX_TIMESTAMP watermark on
    # the same claim must have that record emitted before the estimator reaches
    # MAX, so the element is not stranded behind the output watermark.
    dofn = _ReadFromUnboundedSourceDoFn(poll_interval=0)
    gen, _, _, _, estimator = self._bundle(dofn, _MaxOnLastSource())

    first = next(gen)
    self.assertEqual(first.value, 7)
    # The estimator has not yet been advanced to MAX when the record is yielded.
    self.assertNotEqual(estimator.current_watermark(), MAX_TIMESTAMP)
    # Draining the bundle then advances the estimator to MAX on EOF.
    self.assertEqual(list(gen), [])
    self.assertEqual(estimator.current_watermark(), MAX_TIMESTAMP)


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
  def test_finalize_checkpoint_callback_is_at_most_once(self):
    finalize_log = []
    finalize_once = _FinalizeCheckpointOnce(
        _CountingCheckpointMark(1, finalize_log=finalize_log))

    finalize_once()
    finalize_once()

    self.assertEqual(finalize_log, [1])

  def test_finalize_checkpoint_invoked(self):
    # Unit-level finalize test (the e2e finalize may run in a worker process);
    # the hook lives on the primary, independent of the residual's resume state.
    finalize_log = []
    source = UnboundedCountingSource(5, finalize_log=finalize_log)
    tracker = _new_tracker(source)
    _claim(tracker)  # 0
    _claim(tracker)  # 1
    primary, _ = tracker.try_split(0)
    primary.finalization_checkpoint_mark.finalize_checkpoint()
    self.assertEqual(finalize_log, [1])


class EndToEndTest(unittest.TestCase):
  def test_direct_runner_emits_all_in_order(self):
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(UnboundedCountingSource(5))
      self.assertFalse(out.is_bounded)
      assert_that(out, equal_to([0, 1, 2, 3, 4]))

  def test_eof_lets_event_time_window_fire(self):
    # On EOF the DoFn advances the watermark estimator to MAX_TIMESTAMP so the
    # downstream FixedWindow closes and the GroupByKey fires; otherwise the
    # output would be empty.
    with TestPipeline() as p:
      out = (
          p
          | ReadFromUnboundedSource(UnboundedCountingSource(5))
          | beam.WindowInto(FixedWindows(100))
          | beam.Map(lambda v: ('all', v))
          | beam.GroupByKey()
          | beam.MapTuple(lambda _key, values: sorted(values)))
      assert_that(out, equal_to([[0, 1, 2, 3, 4]]))

  def test_read_dispatches_through_iobase_read(self):
    # ``beam.io.Read(source)`` must produce the same records as
    # ``ReadFromUnboundedSource(source)``.
    with TestPipeline() as p:
      out = p | beam.io.Read(UnboundedCountingSource(5))
      self.assertFalse(out.is_bounded)
      assert_that(out, equal_to([0, 1, 2, 3, 4]))

  def test_splittable_source_reads_all_records_across_splits(self):
    # A splittable source fans out into even/odd sub-sources during initial
    # SDF splitting; the union of all sub-source reads is the full sequence.
    with TestPipeline() as p:
      out = p | beam.io.Read(UnboundedCountingSource(6, is_splittable=True))
      assert_that(out, equal_to([0, 1, 2, 3, 4, 5]))

  def test_source_default_output_coder_sets_output_type(self):
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(_StringCountingSource(2))
      self.assertEqual(out.element_type, str)
      assert_that(out, equal_to(['v0', 'v1']))

  def test_small_cap_self_checkpoints_and_resumes_to_eof(self):
    # A small per-bundle cap forces several self-checkpoint/resume cycles
    # through the runner before EOF, exercising the cross-bundle reader cache
    # over real residual encode/decode. All records still arrive in order.
    with TestPipeline() as p:
      out = p | ReadFromUnboundedSource(
          UnboundedCountingSource(20), max_records_per_bundle=5)
      assert_that(out, equal_to(list(range(20))))


class ReadFromUnboundedSourceCoderTest(unittest.TestCase):
  def test_parameterized_output_coder_does_not_mutate_global_registry(self):
    try:
      p = beam.Pipeline()
      out = p | ReadFromUnboundedSource(_PrefixStringSource(1))

      self.assertNotEqual(out.element_type, str)
      self.assertEqual(coders.registry.get_coder(str), coders.StrUtf8Coder())
      self.assertEqual(
          ReadFromUnboundedSource(_PrefixStringSource(1))._infer_output_coder(),
          _PrefixStrCoder('prefix:'))
    finally:
      coders.registry.register_coder(str, coders.StrUtf8Coder)


# ------------------------------------------------------------------------------
# Reader lifecycle, watermark, and contract regression tests (reader close on
# every exit path, the NotImplementedError message, finalize idempotency).
# ------------------------------------------------------------------------------


class ReaderCloseTest(unittest.TestCase):
  """Reader lifecycle: close() must run on every tracker-driven exit path."""
  def test_tracker_closes_reader_on_eof(self):
    source = UnboundedCountingSource(0)  # immediately exhausted
    tracker = _new_tracker(source)
    holder = [None]
    self.assertFalse(tracker.try_claim(holder))
    self.assertIsNone(tracker._reader)
    self.assertTrue(source.last_reader.closed)

  def test_tracker_closes_reader_on_split_without_cache(self):
    # With no cache injected, a self-checkpoint closes the reader.
    source = UnboundedCountingSource(5)
    tracker = _new_tracker(source)
    _claim(tracker)  # creates reader, claims 0
    reader = source.last_reader
    self.assertFalse(reader.closed)
    split = tracker.try_split(0)
    self.assertIsNotNone(split)
    self.assertIsNone(tracker._reader)
    self.assertTrue(reader.closed)

  def test_tracker_parks_reader_on_split_with_cache(self):
    # With a cache, a self-checkpoint parks the reader for the residual to
    # reclaim.
    source = UnboundedCountingSource(5)
    cache = _ReaderCache()
    tracker = _new_tracker(source)
    tracker._reader_cache = cache
    _claim(tracker)
    reader = source.last_reader
    _, residual = tracker.try_split(0)
    self.assertIsNone(tracker._reader)
    self.assertFalse(reader.closed)
    # The residual's key reclaims the same started reader.
    self.assertEqual(
        cache.acquire(tracker._cache_key(residual)), (reader, True))

  def test_close_helper_is_idempotent_and_safe_on_empty_tracker(self):
    tracker = _new_tracker(UnboundedCountingSource(3))
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
      @override
      def start(self):
        return True

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        return 'x'

      @override
      def get_current_timestamp(self):
        return _EVENT_TIME_BASE

      @override
      def get_watermark(self):
        return _EVENT_TIME_BASE

      @override
      def get_checkpoint_mark(self):
        return CheckpointMark()

      @override
      def close(self):
        raise RuntimeError('close blew up')

    class _BoomSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _BoomReader()

      @override
      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    tracker = _new_tracker(_BoomSource())
    _claim(tracker)
    with self.assertLogs(_unbounded_source_module._LOGGER, 'WARNING') as logs:
      tracker._close_reader_if_open()
    self.assertTrue(
        any('Error closing UnboundedReader' in line for line in logs.output))
    self.assertIsNone(tracker._reader)


class ReaderCacheTest(unittest.TestCase):
  """The cache parks a reader under one key, hands it to the next acquirer,
  bounds itself by idle time and entry count, and closes on teardown."""
  def _reader(self):
    class _FakeReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      @override
      def close(self):
        self.closed = True

    return _FakeReader()

  def test_park_then_acquire_returns_same_reader_and_started_flag(self):
    cache = _ReaderCache()
    reader = self._reader()
    cache.park('k', reader, True)
    self.assertEqual(cache.acquire('k'), (reader, True))
    # Acquire removes the entry so two trackers cannot share one reader.
    self.assertIsNone(cache.acquire('k'))
    self.assertFalse(reader.closed)

  def test_acquire_miss_returns_none(self):
    self.assertIsNone(_ReaderCache().acquire('absent'))

  def test_park_replacing_same_key_closes_displaced_reader(self):
    # Two parks under one key without an intervening acquire (e.g. a bundle
    # retry) must not leak the first reader.
    cache = _ReaderCache()
    old, new = self._reader(), self._reader()
    cache.park('k', old, True)
    cache.park('k', new, True)
    self.assertTrue(old.closed)
    self.assertFalse(new.closed)
    self.assertEqual(cache.acquire('k'), (new, True))

  def test_idle_reader_is_closed_on_next_touch(self):
    clock = _ManualClock(1000.0)
    cache = _ReaderCache(idle_seconds=30.0, now=clock)
    reader = self._reader()
    cache.park('k', reader, True)
    clock.now = 1031.0  # past the idle window
    cache.park('other', self._reader(), False)  # triggers idle eviction
    self.assertTrue(reader.closed)
    self.assertIsNone(cache.acquire('k'))

  def test_max_size_evicts_and_closes_oldest(self):
    cache = _ReaderCache(max_size=2)
    readers = [self._reader() for _ in range(3)]
    cache.park('a', readers[0], True)
    cache.park('b', readers[1], True)
    cache.park('c', readers[2], True)  # exceeds the cap, evicts 'a'
    self.assertTrue(readers[0].closed)
    self.assertIsNone(cache.acquire('a'))
    self.assertIsNotNone(cache.acquire('b'))
    self.assertIsNotNone(cache.acquire('c'))

  def test_close_all_closes_every_parked_reader(self):
    cache = _ReaderCache()
    readers = [self._reader() for _ in range(3)]
    for i, reader in enumerate(readers):
      cache.park(str(i), reader, True)
    cache.close_all()
    self.assertTrue(all(reader.closed for reader in readers))
    self.assertIsNone(cache.acquire('0'))

  def test_close_all_swallows_reader_close_errors(self):
    class _BoomReader(UnboundedReader):
      @override
      def close(self):
        raise RuntimeError('close blew up')

    cache = _ReaderCache()
    cache.park('k', _BoomReader(), True)
    with self.assertLogs(_unbounded_source_module._LOGGER, 'WARNING') as logs:
      cache.close_all()
    self.assertTrue(
        any('Error closing UnboundedReader' in line for line in logs.output))


class TrackerContractRegressionTest(unittest.TestCase):
  """Tracker contract: source-watermark on the data path, finalize/resume
  channel separation, and reader close on a reader-method failure."""
  def test_data_path_holder_carries_source_watermark(self):
    class _LaggingReader(UnboundedReader):
      @override
      def start(self):
        return True

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        return 'rec'

      @override
      def get_current_timestamp(self):
        return Timestamp(1000)  # record event time

      @override
      def get_watermark(self):
        return Timestamp(990)  # source watermark lags 10us behind

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

    class _LaggingSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _LaggingReader()

      @override
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

  def test_split_separates_finalize_and_resume_channels(self):
    source = UnboundedCountingSource(5)
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

  def test_eof_populates_finalize_and_clears_resume(self):
    # EOF transition: restriction.checkpoint_mark goes to None (no more
    # records to resume from), finalization_checkpoint_mark carries the
    # final commit hook.
    source = UnboundedCountingSource(0)  # immediately exhausted
    tracker = _new_tracker(source)
    holder = [None]
    self.assertFalse(tracker.try_claim(holder))
    r = tracker.current_restriction()
    self.assertTrue(r.is_done)
    self.assertEqual(r.watermark, MAX_TIMESTAMP)
    self.assertIsNone(r.checkpoint_mark)
    self.assertIsNotNone(r.finalization_checkpoint_mark)

  def test_tracker_closes_reader_when_advance_raises(self):
    # try_claim closes the reader before re-raising a reader-method failure, so
    # the DoFn's finally need not traverse the SDF chain for these.
    class _BoomReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      @override
      def start(self):
        return True

      @override
      def advance(self):
        raise RuntimeError('advance boom')

      @override
      def get_current(self):
        return 'first'

      @override
      def get_current_timestamp(self):
        return _EVENT_TIME_BASE

      @override
      def get_watermark(self):
        return _EVENT_TIME_BASE

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

      @override
      def close(self):
        self.closed = True

    class _BoomSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _BoomReader()

      @override
      def get_checkpoint_mark_coder(self):
        return coders.PickleCoder()

    src = _BoomSource()
    tracker = _new_tracker(src)
    # First claim succeeds (start returns True).
    self.assertTrue(tracker.try_claim([None]))
    reader_after_first = tracker._reader
    self.assertIsNotNone(reader_after_first)
    # The second claim's advance() raises; the tracker must close the reader
    # before propagating.
    with self.assertRaises(RuntimeError):
      tracker.try_claim([None])
    self.assertTrue(reader_after_first.closed)
    self.assertIsNone(tracker._reader)

  def test_tracker_closes_reader_when_get_watermark_raises(self):
    # Reader method failures other than advance() also trigger close.
    class _WatermarkBoomReader(UnboundedReader):
      def __init__(self):
        self.closed = False

      @override
      def start(self):
        return False  # no data -> drops into get_watermark path

      @override
      def advance(self):
        return False

      @override
      def get_current(self):
        raise AssertionError

      @override
      def get_current_timestamp(self):
        raise AssertionError

      @override
      def get_watermark(self):
        raise RuntimeError('watermark boom')

      @override
      def get_checkpoint_mark(self):
        return _CountingCheckpointMark(0)

      @override
      def close(self):
        self.closed = True

    class _WatermarkBoomSource(UnboundedSource):
      @override
      def split(self, desired_num_splits, options=None):
        return [self]

      @override
      def create_reader(self, options, checkpoint_mark):
        return _WatermarkBoomReader()

      @override
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


class ReadFromUnboundedSourceValidationTest(unittest.TestCase):
  def test_non_source_argument_raises(self):
    with self.assertRaises(TypeError):
      ReadFromUnboundedSource('not-a-source')  # type: ignore[arg-type]

  def test_invalid_caps_raise(self):
    source = UnboundedCountingSource(1)
    with self.assertRaises(ValueError):
      ReadFromUnboundedSource(source, max_records_per_bundle=0)
    with self.assertRaises(ValueError):
      ReadFromUnboundedSource(source, max_read_time_seconds=0)
    with self.assertRaises(ValueError):
      ReadFromUnboundedSource(source, poll_interval=-1)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
