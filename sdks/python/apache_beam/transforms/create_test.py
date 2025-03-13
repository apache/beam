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

"""Unit tests for the Create and _CreateSource classes."""
# pytype: skip-file

import logging
import unittest

from apache_beam import Create
from apache_beam import coders
from apache_beam.coders import FastPrimitivesCoder
from apache_beam.internal import pickler
from apache_beam.io import source_test_utils
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class CreateTest(unittest.TestCase):
  def setUp(self):
    self.coder = FastPrimitivesCoder()

  def test_create_transform(self):
    with TestPipeline() as p:
      assert_that(p | 'Empty' >> Create([]), equal_to([]), label='empty')
      assert_that(p | 'One' >> Create([None]), equal_to([None]), label='one')
      assert_that(p | Create(list(range(10))), equal_to(list(range(10))))

  def test_create_source_read(self):
    self.check_read([], self.coder)
    self.check_read([1], self.coder)
    # multiple values.
    self.check_read(list(range(10)), self.coder)

  def check_read(self, values, coder):
    source = Create._create_source_from_iterable(values, coder)
    read_values = source_test_utils.read_from_source(source)
    self.assertEqual(sorted(values), sorted(read_values))

  def test_create_source_read_with_initial_splits(self):
    self.check_read_with_initial_splits([], self.coder, num_splits=2)
    self.check_read_with_initial_splits([1], self.coder, num_splits=2)
    values = list(range(8))
    # multiple values with a single split.
    self.check_read_with_initial_splits(values, self.coder, num_splits=1)
    # multiple values with a single split with a large desired bundle size
    self.check_read_with_initial_splits(values, self.coder, num_splits=0.5)
    # multiple values with many splits.
    self.check_read_with_initial_splits(values, self.coder, num_splits=3)
    # multiple values with uneven sized splits.
    self.check_read_with_initial_splits(values, self.coder, num_splits=4)
    # multiple values with num splits equal to num values.
    self.check_read_with_initial_splits(
        values, self.coder, num_splits=len(values))
    # multiple values with num splits greater than to num values.
    self.check_read_with_initial_splits(values, self.coder, num_splits=30)

  def check_read_with_initial_splits(self, values, coder, num_splits):
    """A test that splits the given source into `num_splits` and verifies that
    the data read from original source is equal to the union of the data read
    from the split sources.
    """
    source = Create._create_source_from_iterable(values, coder)
    desired_bundle_size = source._total_size // num_splits
    splits = source.split(desired_bundle_size)
    splits_info = [(split.source, split.start_position, split.stop_position)
                   for split in splits]
    source_test_utils.assert_sources_equal_reference_source(
        (source, None, None), splits_info)

  def test_create_source_read_reentrant(self):
    source = Create._create_source_from_iterable(range(9), self.coder)
    source_test_utils.assert_reentrant_reads_succeed((source, None, None))

  def test_create_source_read_reentrant_with_initial_splits(self):
    source = Create._create_source_from_iterable(range(24), self.coder)
    for split in source.split(desired_bundle_size=5):
      source_test_utils.assert_reentrant_reads_succeed(
          (split.source, split.start_position, split.stop_position))

  def test_create_source_dynamic_splitting(self):
    # 2 values
    source = Create._create_source_from_iterable(range(2), self.coder)
    source_test_utils.assert_split_at_fraction_exhaustive(source)
    # Multiple values.
    source = Create._create_source_from_iterable(range(11), self.coder)
    source_test_utils.assert_split_at_fraction_exhaustive(
        source, perform_multi_threaded_test=True)

  def test_create_source_progress(self):
    num_values = 10
    source = Create._create_source_from_iterable(range(num_values), self.coder)
    splits = [split for split in source.split(desired_bundle_size=100)]
    assert len(splits) == 1
    fraction_consumed_report = []
    split_points_report = []
    range_tracker = splits[0].source.get_range_tracker(
        splits[0].start_position, splits[0].stop_position)
    for _ in splits[0].source.read(range_tracker):
      fraction_consumed_report.append(range_tracker.fraction_consumed())
      split_points_report.append(range_tracker.split_points())

    self.assertEqual([float(i) / num_values for i in range(num_values)],
                     fraction_consumed_report)

    expected_split_points_report = [((i - 1), num_values - (i - 1))
                                    for i in range(1, num_values + 1)]

    self.assertEqual(expected_split_points_report, split_points_report)

  def test_create_uses_coder_for_pickling(self):
    coders.registry.register_coder(_Unpicklable, _UnpicklableCoder)
    create = Create([_Unpicklable(1), _Unpicklable(2), _Unpicklable(3)])
    unpickled_create = pickler.loads(pickler.dumps(create))
    self.assertEqual(
        sorted(create.values, key=lambda v: v.value),
        sorted(unpickled_create.values, key=lambda v: v.value))

    with self.assertRaises(NotImplementedError):
      # As there is no special coder for Union types, this will fall back to
      # FastPrimitivesCoder, which in turn falls back to pickling.
      create_mixed_types = Create([_Unpicklable(1), 2])
      pickler.dumps(create_mixed_types)


class _Unpicklable(object):
  def __init__(self, value):
    self.value = value

  def __eq__(self, other):
    return self.value == other.value

  def __getstate__(self):
    raise NotImplementedError()

  def __setstate__(self, state):
    raise NotImplementedError()


class _UnpicklableCoder(coders.Coder):
  def encode(self, value):
    return str(value.value).encode()

  def decode(self, encoded):
    return _Unpicklable(int(encoded.decode()))

  def to_type_hint(self):
    return _Unpicklable


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
