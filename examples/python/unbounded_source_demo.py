#!/usr/bin/env python
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

"""Demo: Python UnboundedSource example with correctness verification.

This example demonstrates how to implement a custom UnboundedSource in
Python using the new UnboundedSource/UnboundedReader API (which is
internally wrapped as a Splittable DoFn).

The source generates integers [0, N) with:
  - Timestamps: each element i has timestamp = epoch + i seconds
  - Watermarks: advances with the element count
  - Checkpoints: stores how many elements have been read

The pipeline reads from this source, applies transforms, and verifies
correctness using assert_that.

Usage:
  python unbounded_source_demo.py
  python unbounded_source_demo.py --num_elements=50
"""

import argparse
import logging
import sys

import apache_beam as beam
from apache_beam.io.iobase import CheckpointMark
from apache_beam.io.iobase import Read
from apache_beam.io.iobase import UnboundedReader
from apache_beam.io.iobase import UnboundedSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.window import FixedWindows
from apache_beam.utils.timestamp import MAX_TIMESTAMP
from apache_beam.utils.timestamp import Timestamp


# =============================================================================
# 1. Define CheckpointMark — stores how many elements we've read
# =============================================================================

class CounterCheckpointMark(CheckpointMark):
  """Checkpoint that tracks the number of elements read so far."""
  def __init__(self, count=0):
    self.count = count

  def finalize_checkpoint(self):
    # In a real source (e.g., Pub/Sub), this would acknowledge messages.
    # For our demo, nothing to do.
    pass


# =============================================================================
# 2. Define UnboundedReader — produces integers [0, N)
# =============================================================================

class CounterUnboundedReader(UnboundedReader):
  """Reads integers from 0 up to source.num_elements.

  Supports checkpoint/resume: if created with a CounterCheckpointMark,
  it resumes from that count.
  """
  def __init__(self, source, checkpoint_mark=None):
    self._source = source
    self._count = checkpoint_mark.count if checkpoint_mark else 0
    self._current = None

  def start(self):
    """Initialize and read the first element."""
    if self._count < self._source.num_elements:
      self._current = self._count
      self._count += 1
      return True
    return False

  def advance(self):
    """Advance to the next element."""
    if self._count < self._source.num_elements:
      self._current = self._count
      self._count += 1
      return True
    return False

  def get_current(self):
    """Return the current integer element."""
    if self._current is None:
      raise StopIteration('No current element.')
    return self._current

  def get_current_timestamp(self):
    """Each element i has timestamp = epoch + i seconds."""
    return Timestamp.of(self._current or 0)

  def get_current_record_id(self):
    """Unique ID for deduplication (not needed here, but implemented)."""
    return str(self._current).encode('utf-8')

  def get_watermark(self):
    """Watermark = MAX_TIMESTAMP when done, otherwise the current count."""
    if self._count >= self._source.num_elements:
      return MAX_TIMESTAMP
    return Timestamp.of(self._count)

  def get_checkpoint_mark(self):
    """Snapshot current progress so we can resume later."""
    return CounterCheckpointMark(self._count)

  def get_current_source(self):
    return self._source

  def get_split_backlog_bytes(self):
    """Report remaining work for autoscaling."""
    return max(0, self._source.num_elements - self._count)

  def close(self):
    pass


# =============================================================================
# 3. Define UnboundedSource — the source itself
# =============================================================================

class CounterUnboundedSource(UnboundedSource):
  """An UnboundedSource that produces integers [0, num_elements).

  This is a finite unbounded source (it generates a fixed number of
  elements with MAX_TIMESTAMP watermark to signal completion), making it
  suitable for testing and verification.
  """
  def __init__(self, num_elements):
    self.num_elements = num_elements

  def split(self, desired_num_splits, pipeline_options=None):
    """For simplicity, we don't split — return self as a single split."""
    return [self]

  def create_reader(self, pipeline_options, checkpoint_mark=None):
    """Create a reader, optionally resuming from a checkpoint."""
    return CounterUnboundedReader(self, checkpoint_mark)

  def requires_deduping(self):
    return False


# =============================================================================
# 4. Run the demo pipeline and verify results
# =============================================================================

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument(
      '--num_elements',
      type=int,
      default=20,
      help='Number of elements to generate (default: 20)')
  known_args, pipeline_args = parser.parse_known_args(argv)
  n = known_args.num_elements

  print('=' * 60)
  print(f'Python UnboundedSource Demo')
  print(f'Generating {n} elements: [0, {n})')
  print('=' * 60)

  # --- Test 1: Basic read ---
  print('\n--- Test 1: Read from CounterUnboundedSource ---')
  with TestPipeline(argv=pipeline_args) as p:
    result = p | 'ReadCounter' >> Read(CounterUnboundedSource(n))
    assert_that(
        result,
        equal_to(list(range(n))),
        label='VerifyElements')
  print(f'PASS: Read {n} elements: {list(range(n))}')

  # --- Test 2: Read + Map (double each element) ---
  print('\n--- Test 2: Read + Map(x * 2) ---')
  with TestPipeline(argv=pipeline_args) as p:
    result = (
        p
        | 'ReadCounter2' >> Read(CounterUnboundedSource(n))
        | 'Double' >> beam.Map(lambda x: x * 2))
    expected = [x * 2 for x in range(n)]
    assert_that(result, equal_to(expected), label='VerifyDoubled')
  print(f'PASS: Doubled elements: {expected}')

  # --- Test 3: Read + Filter (even numbers only) ---
  print('\n--- Test 3: Read + Filter(even) ---')
  with TestPipeline(argv=pipeline_args) as p:
    result = (
        p
        | 'ReadCounter3' >> Read(CounterUnboundedSource(n))
        | 'FilterEven' >> beam.Filter(lambda x: x % 2 == 0))
    expected = [x for x in range(n) if x % 2 == 0]
    assert_that(result, equal_to(expected), label='VerifyEven')
  print(f'PASS: Even elements: {expected}')

  # --- Test 4: Read + Window + CombineGlobally (sum) ---
  # Note: CombineGlobally uses GroupByKey internally, which requires
  # explicit windowing for unbounded PCollections.
  print('\n--- Test 4: Read + Window + Sum ---')
  with TestPipeline(argv=pipeline_args) as p:
    result = (
        p
        | 'ReadCounter4' >> Read(CounterUnboundedSource(n))
        # Window all elements into a single FixedWindow (large enough
        # to cover timestamps 0..n-1 seconds).
        | 'Window' >> beam.WindowInto(FixedWindows(max(n, 1)))
        | 'Sum' >> beam.CombineGlobally(sum).without_defaults())
    expected_sum = sum(range(n))
    assert_that(result, equal_to([expected_sum]), label='VerifySum')
  print(f'PASS: Sum of [0..{n}) = {expected_sum}')

  # --- Test 5: Read empty source ---
  print('\n--- Test 5: Empty source ---')
  with TestPipeline(argv=pipeline_args) as p:
    result = p | 'ReadEmpty' >> Read(CounterUnboundedSource(0))
    assert_that(result, equal_to([]), label='VerifyEmpty')
  print('PASS: Empty source produced 0 elements')

  # --- Test 6: Checkpoint/resume at reader level ---
  print('\n--- Test 6: Checkpoint/Resume ---')
  source = CounterUnboundedSource(n)
  reader = source.create_reader(None)

  first_half = []
  reader.start()
  first_half.append(reader.get_current())
  for _ in range(n // 2 - 1):
    reader.advance()
    first_half.append(reader.get_current())

  checkpoint = reader.get_checkpoint_mark()
  print(f'  First {n // 2} elements: {first_half}')
  print(f'  Checkpoint at count={checkpoint.count}')

  reader2 = source.create_reader(None, checkpoint)
  second_half = []
  if reader2.start():
    second_half.append(reader2.get_current())
    while reader2.advance():
      second_half.append(reader2.get_current())

  all_elements = first_half + second_half
  assert all_elements == list(range(n)), (
      f'Checkpoint/resume failed: {all_elements} != {list(range(n))}')
  print(f'  Resumed: {second_half}')
  print(f'  Combined: {all_elements}')
  print(f'PASS: Checkpoint/resume produced all {n} elements')

  print('\n' + '=' * 60)
  print('ALL 6 TESTS PASSED')
  print('=' * 60)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.WARNING)
  run()
