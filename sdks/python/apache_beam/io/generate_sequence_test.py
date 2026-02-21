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

"""Tests for GenerateSequence transform."""

import unittest

import apache_beam as beam
from apache_beam.io.generate_sequence import GenerateSequence
from apache_beam.io.generate_sequence import _BoundedCountingSource
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class GenerateSequenceTest(unittest.TestCase):

  def test_basic_bounded(self):
    with TestPipeline() as p:
      result = p | GenerateSequence(start=0, stop=5)
      assert_that(result, equal_to([0, 1, 2, 3, 4]))

  def test_start_not_zero(self):
    with TestPipeline() as p:
      result = p | GenerateSequence(start=3, stop=7)
      assert_that(result, equal_to([3, 4, 5, 6]))

  def test_empty_range(self):
    with TestPipeline() as p:
      result = p | GenerateSequence(start=5, stop=5)
      assert_that(result, equal_to([]))

  def test_single_element(self):
    with TestPipeline() as p:
      result = p | GenerateSequence(start=42, stop=43)
      assert_that(result, equal_to([42]))

  def test_invalid_negative_start(self):
    with self.assertRaises(ValueError):
      GenerateSequence(start=-1, stop=10)

  def test_invalid_stop_less_than_start(self):
    with self.assertRaises(ValueError):
      GenerateSequence(start=10, stop=5)

  def test_large_sequence(self):
    with TestPipeline() as p:
      result = p | GenerateSequence(start=0, stop=1000)
      assert_that(result, equal_to(list(range(1000))))

  def test_unbounded_not_implemented(self):
    with self.assertRaises(NotImplementedError):
      with TestPipeline() as p:
        _ = p | GenerateSequence(start=0)


class BoundedCountingSourceTest(unittest.TestCase):

  def test_estimate_size(self):
    source = _BoundedCountingSource(0, 100)
    # 100 elements * 8 bytes per element = 800 bytes
    self.assertEqual(source.estimate_size(), 800)

  def test_estimate_size_empty(self):
    source = _BoundedCountingSource(5, 5)
    self.assertEqual(source.estimate_size(), 0)

  def test_split_creates_multiple_bundles(self):
    source = _BoundedCountingSource(0, 100)
    # 80 bytes = 10 elements per bundle
    bundles = list(source.split(desired_bundle_size=80))
    self.assertEqual(len(bundles), 10)

    # Verify no elements are missed or duplicated
    all_starts = [b.start_position for b in bundles]
    all_stops = [b.stop_position for b in bundles]
    self.assertEqual(all_starts[0], 0)
    self.assertEqual(all_stops[-1], 100)
    for i in range(len(bundles) - 1):
      self.assertEqual(all_stops[i], all_starts[i + 1])

  def test_split_single_bundle(self):
    source = _BoundedCountingSource(0, 10)
    # Large bundle size means single bundle
    bundles = list(source.split(desired_bundle_size=8000))
    self.assertEqual(len(bundles), 1)
    self.assertEqual(bundles[0].start_position, 0)
    self.assertEqual(bundles[0].stop_position, 10)

  def test_split_with_custom_range(self):
    source = _BoundedCountingSource(0, 100)
    bundles = list(source.split(
        desired_bundle_size=80, start_position=20, stop_position=50))
    # Should only cover range [20, 50)
    self.assertEqual(bundles[0].start_position, 20)
    self.assertEqual(bundles[-1].stop_position, 50)
    # Total elements should be 30
    total_elements = sum(b.stop_position - b.start_position for b in bundles)
    self.assertEqual(total_elements, 30)

  def test_get_range_tracker(self):
    source = _BoundedCountingSource(0, 100)
    tracker = source.get_range_tracker(10, 50)
    self.assertEqual(tracker.start_position(), 10)
    self.assertEqual(tracker.stop_position(), 50)

  def test_get_range_tracker_default_positions(self):
    source = _BoundedCountingSource(5, 15)
    tracker = source.get_range_tracker(None, None)
    self.assertEqual(tracker.start_position(), 5)
    self.assertEqual(tracker.stop_position(), 15)

  def test_read_with_range_tracker(self):
    source = _BoundedCountingSource(0, 10)
    tracker = source.get_range_tracker(0, 10)
    result = list(source.read(tracker))
    self.assertEqual(result, list(range(10)))

  def test_read_subset_range(self):
    source = _BoundedCountingSource(0, 100)
    tracker = source.get_range_tracker(25, 30)
    result = list(source.read(tracker))
    self.assertEqual(result, [25, 26, 27, 28, 29])

  def test_default_output_coder(self):
    source = _BoundedCountingSource(0, 10)
    coder = source.default_output_coder()
    # Should be able to encode/decode integers
    encoded = coder.encode(42)
    decoded = coder.decode(encoded)
    self.assertEqual(decoded, 42)

  def test_display_data(self):
    source = _BoundedCountingSource(10, 100)
    display_data = source.display_data()
    self.assertIn('start', display_data)
    self.assertIn('stop', display_data)
    self.assertEqual(display_data['start'].value, 10)
    self.assertEqual(display_data['stop'].value, 100)


class GenerateSequenceDisplayDataTest(unittest.TestCase):

  def test_display_data(self):
    transform = GenerateSequence(start=5, stop=50)
    display_data = transform.display_data()
    self.assertIn('start', display_data)
    self.assertIn('stop', display_data)
    self.assertEqual(display_data['start'].value, 5)
    self.assertEqual(display_data['stop'].value, 50)


if __name__ == '__main__':
  unittest.main()
