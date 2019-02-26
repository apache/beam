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

"""Helper functions and test harnesses for source implementations.

This module contains helper functions and test harnesses for checking
correctness of source (a subclass of ``iobase.BoundedSource``) and range
tracker (a subclass of``iobase.RangeTracker``) implementations.

Contains a few lightweight utilities (e.g. reading items from a source such as
``readFromSource()``, as well as heavyweight property testing and stress
testing harnesses that help getting a large amount of test coverage with few
code.

Most notable ones are:
* ``assertSourcesEqualReferenceSource()`` helps testing that the data read by
the union of sources produced by ``BoundedSource.split()`` is the same as data
read by the original source.
* If your source implements dynamic work rebalancing, use the
``assertSplitAtFraction()`` family of functions - they test behavior of
``RangeTracker.try_split()``, in particular, that various consistency
properties are respected and the total set of data read by the source is
preserved when splits happen. Use ``assertSplitAtFractionBehavior()`` to test
individual cases of ``RangeTracker.try_split()`` and use
``assertSplitAtFractionExhaustive()`` as a heavy-weight stress test including
concurrency. We strongly recommend to use both.

For example usages, see the unit tests of modules such as
 * apache_beam.io.source_test_utils_test.py
 * apache_beam.io.avroio_test.py
"""
from __future__ import absolute_import
from __future__ import division

import logging
import threading
import weakref
from builtins import next
from builtins import object
from builtins import range
from collections import namedtuple
from multiprocessing.pool import ThreadPool

from apache_beam.io import iobase
from apache_beam.testing.util import equal_to

__all__ = ['read_from_source',
           'assert_sources_equal_reference_source',
           'assert_reentrant_reads_succeed',
           'assert_split_at_fraction_behavior',
           'assert_split_at_fraction_binary',
           'assert_split_at_fraction_exhaustive',
           'assert_split_at_fraction_fails',
           'assert_split_at_fraction_succeeds_and_consistent']


class ExpectedSplitOutcome(object):
  MUST_SUCCEED_AND_BE_CONSISTENT = 1
  MUST_FAIL = 2
  MUST_BE_CONSISTENT_IF_SUCCEEDS = 3


SplitAtFractionResult = namedtuple(
    'SplitAtFractionResult', 'num_primary_items num_residual_items')

SplitFractionStatistics = namedtuple(
    'SplitFractionStatistics',
    'successful_fractions non_trivial_fractions')


def read_from_source(source, start_position=None, stop_position=None):
  """Reads elements from the given ```BoundedSource```.

  Only reads elements within the given position range.
  Args:
    source (~apache_beam.io.iobase.BoundedSource):
      :class:`~apache_beam.io.iobase.BoundedSource` implementation.
    start_position (int): start position for reading.
    stop_position (int): stop position for reading.

  Returns:
    List[str]: the set of values read from the sources.
  """
  values = []
  range_tracker = source.get_range_tracker(start_position, stop_position)
  assert isinstance(range_tracker, iobase.RangeTracker)
  reader = source.read(range_tracker)
  for value in reader:
    values.append(value)

  return values


def _ThreadPool(threads):
  # ThreadPool crashes in old versions of Python (< 2.7.5) if created from a
  # child thread. (http://bugs.python.org/issue10015)
  if not hasattr(threading.current_thread(), '_children'):
    threading.current_thread()._children = weakref.WeakKeyDictionary()
  return ThreadPool(threads)


def assert_sources_equal_reference_source(reference_source_info, sources_info):
  """Tests if a reference source is equal to a given set of sources.

  Given a reference source (a :class:`~apache_beam.io.iobase.BoundedSource`
  and a position range) and a list of sources, assert that the union of the
  records read from the list of sources is equal to the records read from the
  reference source.

  Args:
    reference_source_info\
        (Tuple[~apache_beam.io.iobase.BoundedSource, int, int]):
      a three-tuple that gives the reference
      :class:`~apache_beam.io.iobase.BoundedSource`, position to start
      reading at, and position to stop reading at.
    sources_info\
        (Iterable[Tuple[~apache_beam.io.iobase.BoundedSource, int, int]]):
      a set of sources. Each source is a three-tuple that is of the same
      format described above.

  Raises:
    ~exceptions.ValueError: if the set of data produced by the reference source
      and the given set of sources are not equivalent.

  """

  if not (isinstance(reference_source_info, tuple) and
          len(reference_source_info) == 3 and
          isinstance(reference_source_info[0], iobase.BoundedSource)):
    raise ValueError('reference_source_info must a three-tuple where first'
                     'item of the tuple gives a '
                     'iobase.BoundedSource. Received: %r'
                     % reference_source_info)
  reference_records = read_from_source(
      *reference_source_info)

  source_records = []
  for source_info in sources_info:
    assert isinstance(source_info, tuple)
    assert len(source_info) == 3
    if not (isinstance(source_info, tuple) and
            len(source_info) == 3 and
            isinstance(source_info[0], iobase.BoundedSource)):
      raise ValueError('source_info must a three tuple where first'
                       'item of the tuple gives a '
                       'iobase.BoundedSource. Received: %r'
                       % source_info)
    if (type(reference_source_info[0].default_output_coder()) !=
        type(source_info[0].default_output_coder())):
      raise ValueError(
          'Reference source %r and the source %r must use the same coder. '
          'They are using %r and %r respectively instead.'
          % (reference_source_info[0], source_info[0],
             type(reference_source_info[0].default_output_coder()),
             type(source_info[0].default_output_coder())))
    source_records.extend(read_from_source(*source_info))

  if len(reference_records) != len(source_records):
    raise ValueError(
        'Reference source must produce the same number of records as the '
        'list of sources. Number of records were %d and %d instead.'
        % (len(reference_records), len(source_records)))

  if equal_to(reference_records)(source_records):
    raise ValueError(
        'Reference source and provided list of sources must produce the '
        'same set of records.')


def assert_reentrant_reads_succeed(source_info):
  """Tests if a given source can be read in a reentrant manner.

  Assume that given source produces the set of values ``{v1, v2, v3, ... vn}``.
  For ``i`` in range ``[1, n-1]`` this method performs a reentrant read after
  reading ``i`` elements and verifies that both the original and reentrant read
  produce the expected set of values.

  Args:
    source_info (Tuple[~apache_beam.io.iobase.BoundedSource, int, int]):
      a three-tuple that gives the reference
      :class:`~apache_beam.io.iobase.BoundedSource`, position to start reading
      at, and a position to stop reading at.

  Raises:
    ~exceptions.ValueError: if source is too trivial or reentrant read result
      in an incorrect read.
  """

  source, start_position, stop_position = source_info
  assert isinstance(source, iobase.BoundedSource)

  expected_values = [val for val in source.read(source.get_range_tracker(
      start_position, stop_position))]
  if len(expected_values) < 2:
    raise ValueError('Source is too trivial since it produces only %d '
                     'values. Please give a source that reads at least 2 '
                     'values.' % len(expected_values))

  for i in range(1, len(expected_values) - 1):
    read_iter = source.read(source.get_range_tracker(
        start_position, stop_position))
    original_read = []
    for _ in range(i):
      original_read.append(next(read_iter))

    # Reentrant read
    reentrant_read = [val for val in source.read(
        source.get_range_tracker(start_position, stop_position))]

    # Continuing original read.
    for val in read_iter:
      original_read.append(val)

    if equal_to(original_read)(expected_values):
      raise ValueError('Source did not produce expected values when '
                       'performing a reentrant read after reading %d values. '
                       'Expected %r received %r.'
                       % (i, expected_values, original_read))

    if equal_to(reentrant_read)(expected_values):
      raise ValueError('A reentrant read of source after reading %d values '
                       'did not produce expected values. Expected %r '
                       'received %r.'
                       % (i, expected_values, reentrant_read))


def assert_split_at_fraction_behavior(source, num_items_to_read_before_split,
                                      split_fraction, expected_outcome):
  """Verifies the behaviour of splitting a source at a given fraction.

  Asserts that splitting a :class:`~apache_beam.io.iobase.BoundedSource` either
  fails after reading **num_items_to_read_before_split** items, or succeeds in
  a way that is consistent according to
  :func:`assert_split_at_fraction_succeeds_and_consistent()`.

  Args:
    source (~apache_beam.io.iobase.BoundedSource): the source to perform
      dynamic splitting on.
    num_items_to_read_before_split (int): number of items to read before
      splitting.
    split_fraction (float): fraction to split at.
    expected_outcome (int): a value from
      :class:`~apache_beam.io.source_test_utils.ExpectedSplitOutcome`.

  Returns:
    Tuple[int, int]: a tuple that gives the number of items produced by reading
    the two ranges produced after dynamic splitting. If splitting did not
    occur, the first value of the tuple will represent the full set of records
    read by the source while the second value of the tuple will be ``-1``.
  """
  assert isinstance(source, iobase.BoundedSource)
  expected_items = read_from_source(source, None, None)
  return _assert_split_at_fraction_behavior(
      source, expected_items, num_items_to_read_before_split, split_fraction,
      expected_outcome)


def _assert_split_at_fraction_behavior(
    source, expected_items, num_items_to_read_before_split,
    split_fraction, expected_outcome, start_position=None, stop_position=None):

  range_tracker = source.get_range_tracker(start_position, stop_position)
  assert isinstance(range_tracker, iobase.RangeTracker)
  current_items = []
  reader = source.read(range_tracker)
  # Reading 'num_items_to_read_before_split' items.
  reader_iter = iter(reader)
  for _ in range(num_items_to_read_before_split):
    current_items.append(next(reader_iter))

  suggested_split_position = range_tracker.position_at_fraction(
      split_fraction)

  stop_position_before_split = range_tracker.stop_position()
  split_result = range_tracker.try_split(suggested_split_position)

  if split_result is not None:
    if len(split_result) != 2:
      raise ValueError('Split result must be a tuple that contains split '
                       'position and split fraction. Received: %r' %
                       (split_result,))

    if range_tracker.stop_position() != split_result[0]:
      raise ValueError('After a successful split, the stop position of the '
                       'RangeTracker must be the same as the returned split '
                       'position. Observed %r and %r which are different.'
                       % (range_tracker.stop_position() % (split_result[0],)))

    if split_fraction < 0 or split_fraction > 1:
      raise ValueError('Split fraction must be within the range [0,1]',
                       'Observed split fraction was %r.' % (split_result[1],))

  stop_position_after_split = range_tracker.stop_position()
  if split_result and stop_position_after_split == stop_position_before_split:
    raise ValueError('Stop position %r did not change after a successful '
                     'split of source %r at fraction %r.' %
                     (stop_position_before_split, source, split_fraction))

  if expected_outcome == ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT:
    if not split_result:
      raise ValueError('Expected split of source %r at fraction %r to be '
                       'successful after reading %d elements. But '
                       'the split failed.' %
                       (source, split_fraction, num_items_to_read_before_split))
  elif expected_outcome == ExpectedSplitOutcome.MUST_FAIL:
    if split_result:
      raise ValueError('Expected split of source %r at fraction %r after '
                       'reading %d elements to fail. But splitting '
                       'succeeded with result %r.' %
                       (source, split_fraction, num_items_to_read_before_split,
                        split_result))

  elif (expected_outcome !=
        ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS):
    raise ValueError('Unknown type of expected outcome: %r' %
                     expected_outcome)
  current_items.extend([value for value in reader_iter])

  residual_range = (
      split_result[0], stop_position_before_split) if split_result else None

  return _verify_single_split_fraction_result(
      source, expected_items, current_items,
      split_result,
      (range_tracker.start_position(), range_tracker.stop_position()),
      residual_range, split_fraction)


def _range_to_str(start, stop):
  return '[' + (str(start) + ',' + str(stop) + ')')


def _verify_single_split_fraction_result(
    source, expected_items, current_items, split_successful, primary_range,
    residual_range, split_fraction):

  assert primary_range
  primary_items = read_from_source(source, *primary_range)

  if not split_successful:
    # For unsuccessful splits, residual_range should be None.
    assert not residual_range

  residual_items = (
      read_from_source(source, *residual_range)
      if split_successful else [])

  total_items = primary_items + residual_items

  if current_items != primary_items:
    raise ValueError('Current source %r and a source created using the '
                     'range of the primary source %r determined '
                     'by performing dynamic work rebalancing at fraction '
                     '%r produced different values. Expected '
                     'these sources to produce the same list of values.'
                     % (source,
                        _range_to_str(*primary_range),
                        split_fraction)
                    )

  if expected_items != total_items:
    raise ValueError('Items obtained by reading the source %r for primary '
                     'and residual ranges %s and %s did not produce the '
                     'expected list of values.'
                     % (source,
                        _range_to_str(*primary_range),
                        _range_to_str(*residual_range)))

  result = (len(primary_items),
            len(residual_items) if split_successful else -1)
  return result


def assert_split_at_fraction_succeeds_and_consistent(
    source, num_items_to_read_before_split, split_fraction):
  """Verifies some consistency properties of dynamic work rebalancing.

  Equivalent to the following pseudocode:::

    original_range_tracker = source.getRangeTracker(None, None)
    original_reader = source.read(original_range_tracker)
    items_before_split = read N items from original_reader
    suggested_split_position = original_range_tracker.position_for_fraction(
      split_fraction)
    original_stop_position - original_range_tracker.stop_position()
    split_result = range_tracker.try_split()
    split_position, split_fraction = split_result
    primary_range_tracker = source.get_range_tracker(
      original_range_tracker.start_position(), split_position)
    residual_range_tracker = source.get_range_tracker(split_position,
      original_stop_position)

    assert that: items when reading source.read(primary_range_tracker) ==
      items_before_split + items from continuing to read 'original_reader'
    assert that: items when reading source.read(original_range_tracker) =
      items when reading source.read(primary_range_tracker) + items when reading
    source.read(residual_range_tracker)

  Args:

    source: source to perform dynamic work rebalancing on.
    num_items_to_read_before_split: number of items to read before splitting.
    split_fraction: fraction to split at.
  """

  assert_split_at_fraction_behavior(
      source, num_items_to_read_before_split, split_fraction,
      ExpectedSplitOutcome.MUST_SUCCEED_AND_BE_CONSISTENT)


def assert_split_at_fraction_fails(source, num_items_to_read_before_split,
                                   split_fraction):
  """Asserts that dynamic work rebalancing at a given fraction fails.

  Asserts that trying to perform dynamic splitting after reading
  'num_items_to_read_before_split' items from the source fails.

  Args:
    source: source to perform dynamic splitting on.
    num_items_to_read_before_split: number of items to read before splitting.
    split_fraction: fraction to split at.
  """

  assert_split_at_fraction_behavior(
      source, num_items_to_read_before_split, split_fraction,
      ExpectedSplitOutcome.MUST_FAIL)


def assert_split_at_fraction_binary(
    source, expected_items, num_items_to_read_before_split, left_fraction,
    left_result, right_fraction, right_result, stats, start_position=None,
    stop_position=None):
  """Performs dynamic work rebalancing for fractions within a given range.

  Asserts that given a start position, a source can be split at every
  interesting fraction (halfway between two fractions that differ by at
  least one item) and the results are consistent if a split succeeds.

  Args:
    source: source to perform dynamic splitting on.
    expected_items: total set of items expected when reading the source.
    num_items_to_read_before_split: number of items to read before splitting.
    left_fraction: left fraction for binary splitting.
    left_result: result received by splitting at left fraction.
    right_fraction: right fraction for binary splitting.
    right_result: result received by splitting at right fraction.
    stats: a ``SplitFractionStatistics`` for storing results.
  """
  assert right_fraction > left_fraction

  if right_fraction - left_fraction < 0.001:
    # This prevents infinite recursion.
    return

  middle_fraction = (left_fraction + right_fraction) / 2

  if left_result is None:
    left_result = _assert_split_at_fraction_behavior(
        source, expected_items, num_items_to_read_before_split, left_fraction,
        ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS)

  if right_result is None:
    right_result = _assert_split_at_fraction_behavior(
        source, expected_items, num_items_to_read_before_split,
        right_fraction, ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS)

  middle_result = _assert_split_at_fraction_behavior(
      source, expected_items, num_items_to_read_before_split, middle_fraction,
      ExpectedSplitOutcome.MUST_BE_CONSISTENT_IF_SUCCEEDS)

  if middle_result[1] != -1:
    stats.successful_fractions.append(middle_fraction)
  if middle_result[1] > 0:
    stats.non_trivial_fractions.append(middle_fraction)

  # Two split results are equivalent if primary and residual ranges of them
  # produce the same number of records (simply checking the size of primary
  # enough since the total number of records is constant).

  if left_result[0] != middle_result[0]:
    assert_split_at_fraction_binary(
        source, expected_items, num_items_to_read_before_split, left_fraction,
        left_result, middle_fraction, middle_result, stats)

  # We special case right_fraction=1.0 since that could fail due to being out
  # of range. (even if a dynamic split fails at 'middle_fraction' and at
  # fraction 1.0, there might be fractions in range ('middle_fraction', 1.0)
  # where dynamic splitting succeeds).
  if right_fraction == 1.0 or middle_result[0] != right_result[0]:
    assert_split_at_fraction_binary(
        source, expected_items, num_items_to_read_before_split,
        middle_fraction, middle_result, right_fraction, right_result, stats)


MAX_CONCURRENT_SPLITTING_TRIALS_PER_ITEM = 100
MAX_CONCURRENT_SPLITTING_TRIALS_TOTAL = 1000


def assert_split_at_fraction_exhaustive(
    source, start_position=None, stop_position=None,
    perform_multi_threaded_test=True):
  """Performs and tests dynamic work rebalancing exhaustively.

  Asserts that for each possible start position, a source can be split at
  every interesting fraction (halfway between two fractions that differ by at
  least one item) and the results are consistent if a split succeeds.
  Verifies multi threaded splitting as well.

  Args:
    source (~apache_beam.io.iobase.BoundedSource): the source to perform
      dynamic splitting on.
    perform_multi_threaded_test (bool): if :data:`True` performs a
      multi-threaded test, otherwise this test is skipped.

  Raises:
    ~exceptions.ValueError: if the exhaustive splitting test fails.
  """

  expected_items = read_from_source(source, start_position, stop_position)
  if not expected_items:
    raise ValueError('Source %r is empty.' % source)

  if len(expected_items) == 1:
    raise ValueError('Source %r only reads a single item.' % source)

  all_non_trivial_fractions = []

  any_successful_fractions = False
  any_non_trivial_fractions = False

  for i in range(len(expected_items)):
    stats = SplitFractionStatistics([], [])

    assert_split_at_fraction_binary(
        source, expected_items, i, 0.0, None, 1.0, None, stats)

    if stats.successful_fractions:
      any_successful_fractions = True
    if stats.non_trivial_fractions:
      any_non_trivial_fractions = True

    all_non_trivial_fractions.append(stats.non_trivial_fractions)

  if not any_successful_fractions:
    raise ValueError('SplitAtFraction test completed vacuously: no '
                     'successful split fractions found')

  if not any_non_trivial_fractions:
    raise ValueError(
        'SplitAtFraction test completed vacuously: no non-trivial split '
        'fractions found')

  if not perform_multi_threaded_test:
    return

  num_total_trials = 0
  for i in range(len(expected_items)):
    non_trivial_fractions = [2.0]  # 2.0 is larger than any valid fraction.
    non_trivial_fractions.extend(all_non_trivial_fractions[i])
    min_non_trivial_fraction = min(non_trivial_fractions)

    if min_non_trivial_fraction == 2.0:
      # This will not happen all the time. Otherwise previous test will fail
      # due to vacuousness.
      continue

    num_trials = 0
    have_success = False
    have_failure = False

    thread_pool = _ThreadPool(2)
    try:
      while True:
        num_trials += 1
        if (num_trials >
            MAX_CONCURRENT_SPLITTING_TRIALS_PER_ITEM):
          logging.warn(
              'After %d concurrent splitting trials at item #%d, observed '
              'only %s, giving up on this item',
              num_trials,
              i,
              'success' if have_success else 'failure'
          )
          break

        if _assert_split_at_fraction_concurrent(
            source, expected_items, i, min_non_trivial_fraction, thread_pool):
          have_success = True
        else:
          have_failure = True

        if have_success and have_failure:
          logging.info('%d trials to observe both success and failure of '
                       'concurrent splitting at item #%d', num_trials, i)
          break
    finally:
      thread_pool.close()

    num_total_trials += num_trials

    if num_total_trials > MAX_CONCURRENT_SPLITTING_TRIALS_TOTAL:
      logging.warn('After %d total concurrent splitting trials, considered '
                   'only %d items, giving up.', num_total_trials, i)
      break

  logging.info('%d total concurrent splitting trials for %d items',
               num_total_trials, len(expected_items))


def _assert_split_at_fraction_concurrent(
    source, expected_items, num_items_to_read_before_splitting,
    split_fraction, thread_pool=None):

  range_tracker = source.get_range_tracker(None, None)
  stop_position_before_split = range_tracker.stop_position()
  reader = source.read(range_tracker)
  reader_iter = iter(reader)

  current_items = []
  for _ in range(num_items_to_read_before_splitting):
    current_items.append(next(reader_iter))

  def read_or_split(test_params):
    if test_params[0]:
      return [val for val in test_params[1]]
    else:
      position = test_params[1].position_at_fraction(test_params[2])
      result = test_params[1].try_split(position)
      return result

  inputs = []
  pool = thread_pool if thread_pool else _ThreadPool(2)
  try:
    inputs.append([True, reader_iter])
    inputs.append([False, range_tracker, split_fraction])

    results = pool.map(read_or_split, inputs)
  finally:
    if not thread_pool:
      pool.close()

  current_items.extend(results[0])
  primary_range = (
      range_tracker.start_position(), range_tracker.stop_position())

  split_result = results[1]
  residual_range = (
      split_result[0], stop_position_before_split) if split_result else None

  res = _verify_single_split_fraction_result(
      source, expected_items, current_items, split_result,
      primary_range, residual_range, split_fraction)

  return res[1] > 0
