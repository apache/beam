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

"""Tests for UnboundedSource implemented as a Splittable DoFn wrapper."""

import logging
import unittest

import apache_beam as beam
from apache_beam.io.iobase import CheckpointMark
from apache_beam.io.iobase import NOOP_CHECKPOINT_MARK
from apache_beam.io.iobase import UnboundedReader
from apache_beam.io.iobase import UnboundedSource
from apache_beam.io.iobase import _EmptyUnboundedSource
from apache_beam.io.iobase import _NoopCheckpointMark
from apache_beam.io.iobase import _UnboundedSourceRestriction
from apache_beam.io.iobase import _UnboundedSourceRestrictionCoder
from apache_beam.io.iobase import _UnboundedSourceRestrictionProvider
from apache_beam.io.iobase import _UnboundedSourceRestrictionTracker
from apache_beam.io.iobase import Read
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.utils.timestamp import MIN_TIMESTAMP
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


# ---- Test CheckpointMark ----

class _CounterCheckpointMark(CheckpointMark):
  """A simple checkpoint mark that stores the count of elements read."""
  def __init__(self, count=0):
    self.count = count
    self.finalized = False

  def finalize_checkpoint(self):
    self.finalized = True


# ---- Test UnboundedReader ----

class _CounterUnboundedReader(UnboundedReader):
  """An UnboundedReader that generates a finite sequence of integers.
  Used for testing the SDF wrapper.
  """
  def __init__(self, source, checkpoint_mark=None):
    self._source = source
    self._count = checkpoint_mark.count if checkpoint_mark else 0
    self._current = None
    self._started = False

  def start(self):
    self._started = True
    if self._count < self._source.num_elements:
      self._current = self._count
      self._count += 1
      return True
    return False

  def advance(self):
    if self._count < self._source.num_elements:
      self._current = self._count
      self._count += 1
      return True
    return False

  def get_current(self):
    if self._current is None:
      raise StopIteration('No current element.')
    return self._current

  def get_current_timestamp(self):
    return Timestamp.of(self._current or 0)

  def get_current_record_id(self):
    return str(self._current).encode('utf-8')

  def get_watermark(self):
    if self._count >= self._source.num_elements:
      return MAX_TIMESTAMP
    return Timestamp.of(self._count)

  def get_checkpoint_mark(self):
    return _CounterCheckpointMark(self._count)

  def get_current_source(self):
    return self._source

  def get_split_backlog_bytes(self):
    remaining = self._source.num_elements - self._count
    return max(0, remaining)

  def close(self):
    pass


# ---- Test UnboundedSource ----

class _CounterUnboundedSource(UnboundedSource):
  """An UnboundedSource that produces a finite sequence of integers.
  Used for testing the SDF wrapper end-to-end.
  """
  def __init__(self, num_elements, start=0):
    self.num_elements = num_elements
    self.start = start

  def split(self, desired_num_splits, pipeline_options=None):
    # For simplicity, don't split further
    return [self]

  def create_reader(self, pipeline_options, checkpoint_mark=None):
    return _CounterUnboundedReader(self, checkpoint_mark)

  def requires_deduping(self):
    return False


class _SplittableCounterSource(UnboundedSource):
  """An UnboundedSource that can split into multiple sub-sources."""
  def __init__(self, num_elements, num_splits=1, split_index=0):
    self.num_elements = num_elements
    self.num_splits = num_splits
    self.split_index = split_index

  def split(self, desired_num_splits, pipeline_options=None):
    actual_splits = min(desired_num_splits, self.num_elements)
    if actual_splits <= 1:
      return [self]
    sources = []
    per_split = self.num_elements // actual_splits
    for i in range(actual_splits):
      start = i * per_split
      end = (i + 1) * per_split if i < actual_splits - 1 else self.num_elements
      sources.append(
          _RangeCounterSource(start, end))
    return sources

  def create_reader(self, pipeline_options, checkpoint_mark=None):
    return _CounterUnboundedReader(self, checkpoint_mark)


class _RangeCounterSource(UnboundedSource):
  """A source that generates integers in a specific range."""
  def __init__(self, start, end):
    self.start_val = start
    self.end_val = end
    self.num_elements = end - start

  def split(self, desired_num_splits, pipeline_options=None):
    return [self]

  def create_reader(self, pipeline_options, checkpoint_mark=None):
    return _RangeCounterReader(self, checkpoint_mark)


class _RangeCounterReader(UnboundedReader):
  def __init__(self, source, checkpoint_mark=None):
    self._source = source
    self._offset = checkpoint_mark.count if checkpoint_mark else 0
    self._current = None

  def start(self):
    actual_pos = self._source.start_val + self._offset
    if actual_pos < self._source.end_val:
      self._current = actual_pos
      self._offset += 1
      return True
    return False

  def advance(self):
    actual_pos = self._source.start_val + self._offset
    if actual_pos < self._source.end_val:
      self._current = actual_pos
      self._offset += 1
      return True
    return False

  def get_current(self):
    return self._current

  def get_current_timestamp(self):
    return Timestamp.of(self._current or 0)

  def get_current_record_id(self):
    return str(self._current).encode('utf-8')

  def get_watermark(self):
    actual_pos = self._source.start_val + self._offset
    if actual_pos >= self._source.end_val:
      return MAX_TIMESTAMP
    return Timestamp.of(actual_pos)

  def get_checkpoint_mark(self):
    return _CounterCheckpointMark(self._offset)

  def get_current_source(self):
    return self._source

  def close(self):
    pass


# ==== Test classes ====

class CheckpointMarkTest(unittest.TestCase):
  def test_noop_checkpoint_mark(self):
    """NoopCheckpointMark should be finalizable without error."""
    NOOP_CHECKPOINT_MARK.finalize_checkpoint()

  def test_custom_checkpoint_mark(self):
    """Custom CheckpointMark should support finalization."""
    mark = _CounterCheckpointMark(42)
    self.assertEqual(mark.count, 42)
    self.assertFalse(mark.finalized)
    mark.finalize_checkpoint()
    self.assertTrue(mark.finalized)


class UnboundedSourceBaseClassTest(unittest.TestCase):
  def test_is_bounded(self):
    source = _CounterUnboundedSource(10)
    self.assertFalse(source.is_bounded())

  def test_requires_deduping_default(self):
    source = _CounterUnboundedSource(10)
    self.assertFalse(source.requires_deduping())

  def test_split(self):
    source = _CounterUnboundedSource(10)
    splits = source.split(3)
    self.assertEqual(len(splits), 1)

  def test_create_reader(self):
    source = _CounterUnboundedSource(10)
    reader = source.create_reader(None)
    self.assertIsInstance(reader, UnboundedReader)


class UnboundedReaderTest(unittest.TestCase):
  def test_reader_produces_elements(self):
    source = _CounterUnboundedSource(5)
    reader = source.create_reader(None)
    elements = []
    if reader.start():
      elements.append(reader.get_current())
      while reader.advance():
        elements.append(reader.get_current())
    self.assertEqual(elements, [0, 1, 2, 3, 4])

  def test_reader_empty_source(self):
    source = _CounterUnboundedSource(0)
    reader = source.create_reader(None)
    self.assertFalse(reader.start())

  def test_reader_checkpoint_resume(self):
    source = _CounterUnboundedSource(10)
    reader = source.create_reader(None)
    # Read first 3 elements
    reader.start()
    reader.advance()
    reader.advance()
    checkpoint = reader.get_checkpoint_mark()
    self.assertEqual(checkpoint.count, 3)

    # Resume from checkpoint
    reader2 = source.create_reader(None, checkpoint)
    elements = []
    if reader2.start():
      elements.append(reader2.get_current())
      while reader2.advance():
        elements.append(reader2.get_current())
    self.assertEqual(elements, [3, 4, 5, 6, 7, 8, 9])

  def test_reader_watermark(self):
    source = _CounterUnboundedSource(5)
    reader = source.create_reader(None)
    reader.start()
    watermark = reader.get_watermark()
    self.assertIsNotNone(watermark)

  def test_reader_record_id(self):
    source = _CounterUnboundedSource(5)
    reader = source.create_reader(None)
    reader.start()
    record_id = reader.get_current_record_id()
    self.assertIsInstance(record_id, bytes)

  def test_reader_timestamp(self):
    source = _CounterUnboundedSource(5)
    reader = source.create_reader(None)
    reader.start()
    ts = reader.get_current_timestamp()
    self.assertIsInstance(ts, Timestamp)

  def test_reader_backlog(self):
    source = _CounterUnboundedSource(10)
    reader = source.create_reader(None)
    reader.start()
    backlog = reader.get_split_backlog_bytes()
    self.assertGreater(backlog, 0)


class UnboundedSourceRestrictionTest(unittest.TestCase):
  def test_restriction_creation(self):
    source = _CounterUnboundedSource(10)
    restriction = _UnboundedSourceRestriction(source)
    self.assertEqual(restriction.source, source)
    self.assertIsNone(restriction.checkpoint)
    self.assertEqual(restriction.watermark, MIN_TIMESTAMP)

  def test_restriction_with_checkpoint(self):
    source = _CounterUnboundedSource(10)
    checkpoint = _CounterCheckpointMark(5)
    restriction = _UnboundedSourceRestriction(
        source, checkpoint, Timestamp.of(5))
    self.assertEqual(restriction.checkpoint, checkpoint)
    self.assertEqual(restriction.watermark, Timestamp.of(5))


class UnboundedSourceRestrictionTrackerTest(unittest.TestCase):
  def test_try_claim_produces_elements(self):
    source = _CounterUnboundedSource(3)
    restriction = _UnboundedSourceRestriction(source)
    tracker = _UnboundedSourceRestrictionTracker(restriction)

    elements = []
    out = [None]
    while tracker.try_claim(out):
      if out[0] is not None:
        value, ts, watermark, record_id = out[0]
        elements.append(value)
        out = [None]
      else:
        break
    self.assertEqual(elements, [0, 1, 2])

  def test_current_progress(self):
    source = _CounterUnboundedSource(10)
    restriction = _UnboundedSourceRestriction(source)
    tracker = _UnboundedSourceRestrictionTracker(restriction)

    out = [None]
    tracker.try_claim(out)  # triggers reader creation
    progress = tracker.current_progress()
    self.assertIsNotNone(progress)

  def test_try_split(self):
    source = _CounterUnboundedSource(10)
    restriction = _UnboundedSourceRestriction(source)
    tracker = _UnboundedSourceRestrictionTracker(restriction)

    # Read some elements first
    out = [None]
    tracker.try_claim(out)
    tracker.try_claim(out)

    # Now split
    result = tracker.try_split(0.5)
    self.assertIsNotNone(result)
    primary, residual = result
    self.assertIsInstance(primary.source, _EmptyUnboundedSource)
    self.assertIsNotNone(residual.checkpoint)

  def test_try_split_before_start_returns_none(self):
    source = _CounterUnboundedSource(10)
    restriction = _UnboundedSourceRestriction(source)
    tracker = _UnboundedSourceRestrictionTracker(restriction)

    result = tracker.try_split(0.5)
    self.assertIsNone(result)

  def test_is_bounded(self):
    source = _CounterUnboundedSource(10)
    restriction = _UnboundedSourceRestriction(source)
    tracker = _UnboundedSourceRestrictionTracker(restriction)
    self.assertFalse(tracker.is_bounded())


class UnboundedSourceRestrictionCoderTest(unittest.TestCase):
  def test_encode_decode_roundtrip(self):
    source = _CounterUnboundedSource(10)
    checkpoint = _CounterCheckpointMark(5)
    restriction = _UnboundedSourceRestriction(
        source, checkpoint, Timestamp.of(5))

    coder = _UnboundedSourceRestrictionCoder()
    encoded = coder.encode(restriction)
    decoded = coder.decode(encoded)

    self.assertIsInstance(decoded, _UnboundedSourceRestriction)
    self.assertIsInstance(decoded.source, _CounterUnboundedSource)
    self.assertEqual(decoded.source.num_elements, 10)
    self.assertEqual(decoded.checkpoint.count, 5)

  def test_encode_decode_no_checkpoint(self):
    source = _CounterUnboundedSource(10)
    restriction = _UnboundedSourceRestriction(source)

    coder = _UnboundedSourceRestrictionCoder()
    encoded = coder.encode(restriction)
    decoded = coder.decode(encoded)

    self.assertIsInstance(decoded, _UnboundedSourceRestriction)
    self.assertIsNone(decoded.checkpoint)


class UnboundedSourceRestrictionProviderTest(unittest.TestCase):
  def test_initial_restriction(self):
    source = _CounterUnboundedSource(10)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = provider.initial_restriction(source)
    self.assertIsInstance(restriction, _UnboundedSourceRestriction)
    self.assertEqual(restriction.source, source)
    self.assertIsNone(restriction.checkpoint)

  def test_create_tracker(self):
    source = _CounterUnboundedSource(10)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = provider.initial_restriction(source)
    tracker = provider.create_tracker(restriction)
    self.assertIsInstance(tracker, _UnboundedSourceRestrictionTracker)

  def test_split_initial(self):
    source = _CounterUnboundedSource(10)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = provider.initial_restriction(source)
    splits = list(provider.split(source, restriction))
    # CounterUnboundedSource doesn't split, so we should get 1 restriction
    self.assertEqual(len(splits), 1)

  def test_split_with_checkpoint_does_not_split(self):
    source = _CounterUnboundedSource(10)
    checkpoint = _CounterCheckpointMark(5)
    restriction = _UnboundedSourceRestriction(source, checkpoint)
    provider = _UnboundedSourceRestrictionProvider()
    splits = list(provider.split(source, restriction))
    self.assertEqual(len(splits), 1)
    self.assertEqual(splits[0].checkpoint, checkpoint)

  def test_restriction_size(self):
    source = _CounterUnboundedSource(10)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = provider.initial_restriction(source)
    size = provider.restriction_size(source, restriction)
    self.assertGreater(size, 0)

  def test_truncate_returns_none(self):
    source = _CounterUnboundedSource(10)
    provider = _UnboundedSourceRestrictionProvider()
    restriction = provider.initial_restriction(source)
    result = provider.truncate(source, restriction)
    self.assertIsNone(result)

  def test_invalid_source_type(self):
    provider = _UnboundedSourceRestrictionProvider()
    with self.assertRaises(RuntimeError):
      provider.initial_restriction("not a source")


class EmptyUnboundedSourceTest(unittest.TestCase):
  def test_empty_reader(self):
    source = _EmptyUnboundedSource()
    reader = source.create_reader(None)
    self.assertFalse(reader.start())
    self.assertFalse(reader.advance())
    self.assertEqual(reader.get_watermark(), MAX_TIMESTAMP)

  def test_empty_reader_checkpoint(self):
    checkpoint = _CounterCheckpointMark(5)
    source = _EmptyUnboundedSource()
    reader = source.create_reader(None, checkpoint)
    mark = reader.get_checkpoint_mark()
    self.assertEqual(mark, checkpoint)


class ReadUnboundedSourceEndToEndTest(unittest.TestCase):
  """End-to-end tests using Read(UnboundedSource) via the DirectRunner."""

  def test_read_unbounded_source_simple(self):
    """Read from a simple counter unbounded source."""
    with TestPipeline() as p:
      result = p | Read(_CounterUnboundedSource(5))
      assert_that(result, equal_to([0, 1, 2, 3, 4]))

  def test_read_unbounded_source_empty(self):
    """Read from an empty unbounded source."""
    with TestPipeline() as p:
      result = p | Read(_CounterUnboundedSource(0))
      assert_that(result, equal_to([]))

  def test_read_unbounded_source_with_map(self):
    """Read from unbounded source and apply a Map transform."""
    with TestPipeline() as p:
      result = (
          p
          | Read(_CounterUnboundedSource(5))
          | beam.Map(lambda x: x * 2))
      assert_that(result, equal_to([0, 2, 4, 6, 8]))

  def test_read_unbounded_source_larger(self):
    """Read a larger set of elements."""
    n = 100
    with TestPipeline() as p:
      result = p | Read(_CounterUnboundedSource(n))
      assert_that(result, equal_to(list(range(n))))


class PeriodicImpulseSourceEndToEndTest(unittest.TestCase):
  """End-to-end tests for the native Python streaming IO example."""

  def test_periodic_impulse_finite(self):
    """PeriodicImpulseSource with max_elements produces the right count."""
    from apache_beam.io.periodic_impulse_source import PeriodicImpulseSource
    with TestPipeline() as p:
      result = (
          p
          | Read(PeriodicImpulseSource(fire_interval=0, max_elements=5))
          | beam.Map(lambda x: x))
      assert_that(result, equal_to([0, 1, 2, 3, 4]))

  def test_periodic_impulse_empty(self):
    """PeriodicImpulseSource with max_elements=0 produces nothing."""
    from apache_beam.io.periodic_impulse_source import PeriodicImpulseSource
    with TestPipeline() as p:
      result = p | Read(PeriodicImpulseSource(fire_interval=0, max_elements=0))
      assert_that(result, equal_to([]))

  def test_periodic_impulse_checkpoint_resume(self):
    """PeriodicImpulseSource supports checkpoint/resume at the reader level."""
    from apache_beam.io.periodic_impulse_source import PeriodicImpulseSource
    source = PeriodicImpulseSource(fire_interval=0, max_elements=10)
    reader = source.create_reader(None)
    # Read 3 elements
    reader.start()
    reader.advance()
    reader.advance()
    mark = reader.get_checkpoint_mark()
    self.assertEqual(mark.count, 3)

    # Resume from checkpoint — should produce 7 more
    reader2 = source.create_reader(None, mark)
    elements = []
    if reader2.start():
      elements.append(reader2.get_current())
      while reader2.advance():
        elements.append(reader2.get_current())
    self.assertEqual(len(elements), 7)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
