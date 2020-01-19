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

"""Unit tests for the SDFRestrictionProvider module."""

# pytype: skip-file

from __future__ import absolute_import

import time
import unittest

import mock

import apache_beam as beam
from apache_beam.io.concat_source import ConcatSource
from apache_beam.io.concat_source_test import RangeSource
from apache_beam.io import iobase
from apache_beam.io.iobase import SourceBundle
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.utils import timestamp
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class SDFBoundedSourceRestrictionProviderTest(unittest.TestCase):
  def setUp(self):
    self.initial_range_start = 0
    self.initial_range_stop = 4
    self.initial_range_source = RangeSource(self.initial_range_start,
                                            self.initial_range_stop)
    self.sdf_restriction_provider = (
        iobase._SDFBoundedSourceWrapper._SDFBoundedSourceRestrictionProvider(
            self.initial_range_source,
            desired_chunk_size=2))

  def test_initial_restriction(self):
    unused_element = None
    restriction = (
        self.sdf_restriction_provider.initial_restriction(unused_element))
    self.assertTrue(
        isinstance(
            restriction,
            iobase._SDFBoundedSourceWrapper._SDFBoundedSourceRestriction))
    self.assertTrue(isinstance(restriction._source_bundle, SourceBundle))
    self.assertEqual(self.initial_range_start,
                     restriction._source_bundle.start_position)
    self.assertEqual(self.initial_range_stop,
                     restriction._source_bundle.stop_position)
    self.assertTrue(isinstance(restriction._source_bundle.source, RangeSource))
    self.assertEqual(restriction._range_tracker, None)

  def test_create_tracker(self):
    expected_start = 1
    expected_stop = 3
    source_bundle = SourceBundle(expected_stop - expected_start,
                                 RangeSource(1, 3),
                                 expected_start,
                                 expected_stop)
    restriction_tracker = (
        self.sdf_restriction_provider.create_tracker(
            iobase._SDFBoundedSourceWrapper._SDFBoundedSourceRestriction(
                source_bundle)
        ))
    self.assertTrue(isinstance(restriction_tracker,
                               iobase.
                               _SDFBoundedSourceWrapper.
                               _SDFBoundedSourceRestrictionTracker))
    self.assertEqual(expected_start, restriction_tracker.start_pos())
    self.assertEqual(expected_stop, restriction_tracker.stop_pos())

  def test_simple_source_split(self):
    unused_element = None
    restriction = (
        self.sdf_restriction_provider.initial_restriction(unused_element))
    expect_splits = [(0, 2), (2, 4)]
    split_bundles = list(self.sdf_restriction_provider.split(unused_element,
                                                             restriction))
    self.assertTrue(all(
        [isinstance(
            bundle._source_bundle, SourceBundle) for bundle in split_bundles]))

    splits = (
        [(bundle._source_bundle.start_position,
          bundle._source_bundle.stop_position) for bundle in split_bundles])
    self.assertEqual(expect_splits, list(splits))

  def test_concat_source_split(self):
    unused_element = None
    initial_concat_source = ConcatSource([self.initial_range_source])
    sdf_concat_restriction_provider = (
        iobase._SDFBoundedSourceWrapper._SDFBoundedSourceRestrictionProvider(
            initial_concat_source,
            desired_chunk_size=2))
    restriction = (
        self.sdf_restriction_provider.initial_restriction(unused_element))
    expect_splits = [(0, 2), (2, 4)]
    split_bundles = list(sdf_concat_restriction_provider.split(unused_element,
                                                               restriction))
    self.assertTrue(
        all(
            [isinstance(bundle._source_bundle,
                        SourceBundle) for bundle in split_bundles]))
    splits = ([(
        bundle._source_bundle.start_position,
        bundle._source_bundle.stop_position) for bundle in split_bundles])
    self.assertEqual(expect_splits, list(splits))

  def test_restriction_size(self):
    unused_element = None
    restriction = (
        self.sdf_restriction_provider.initial_restriction(unused_element))
    split_1, split_2 = self.sdf_restriction_provider.split(unused_element,
                                                           restriction)
    split_1_size = self.sdf_restriction_provider.restriction_size(
        unused_element, split_1)
    split_2_size = self.sdf_restriction_provider.restriction_size(
        unused_element, split_2)
    self.assertEqual(2, split_1_size)
    self.assertEqual(2, split_2_size)


class SDFBoundedSourceRestrictionTrackerTest(unittest.TestCase):

  def setUp(self):
    self.initial_start_pos = 0
    self.initial_stop_pos = 4
    source_bundle = SourceBundle(
        self.initial_stop_pos - self.initial_start_pos,
        RangeSource(self.initial_start_pos, self.initial_stop_pos),
        self.initial_start_pos,
        self.initial_stop_pos)
    self.sdf_restriction_tracker = (
        iobase._SDFBoundedSourceWrapper._SDFBoundedSourceRestrictionTracker(
            iobase._SDFBoundedSourceWrapper._SDFBoundedSourceRestriction(
                source_bundle)))

  def test_current_restriction_before_split(self):
    current_restriction = (
        self.sdf_restriction_tracker.current_restriction())
    self.assertEqual(self.initial_start_pos,
                     current_restriction._source_bundle.start_position)
    self.assertEqual(self.initial_stop_pos,
                     current_restriction._source_bundle.stop_position)
    self.assertEqual(self.initial_start_pos,
                     current_restriction._range_tracker.start_position())
    self.assertEqual(self.initial_stop_pos,
                     current_restriction._range_tracker.stop_position())

  def test_current_restriction_after_split(self):
    fraction_of_remainder = 0.5
    self.sdf_restriction_tracker.try_claim(1)
    expected_restriction, _ = (
        self.sdf_restriction_tracker.try_split(fraction_of_remainder))
    current_restriction = self.sdf_restriction_tracker.current_restriction()
    self.assertEqual(expected_restriction._source_bundle,
                     current_restriction._source_bundle)
    self.assertTrue(current_restriction._range_tracker)

  def test_try_split_at_remainder(self):
    fraction_of_remainder = 0.4
    expected_primary = (0, 2, 2.0)
    expected_residual = (2, 4, 2.0)
    self.sdf_restriction_tracker.try_claim(0)
    actual_primary, actual_residual = (
        self.sdf_restriction_tracker.try_split(fraction_of_remainder))
    self.assertEqual(expected_primary,
                     (actual_primary._source_bundle.start_position,
                      actual_primary._source_bundle.stop_position,
                      actual_primary._source_bundle.weight))
    self.assertEqual(expected_residual,
                     (actual_residual._source_bundle.start_position,
                      actual_residual._source_bundle.stop_position,
                      actual_residual._source_bundle.weight))
    self.assertEqual(
        actual_primary._source_bundle.weight,
        self.sdf_restriction_tracker.current_restriction().weight())


class UseSdfBoundedSourcesTests(unittest.TestCase):

  def _run_sdf_wrapper_pipeline(self, source, expected_values):
    with beam.Pipeline() as p:
      experiments = (p._options.view_as(DebugOptions).experiments or [])

      # Setup experiment option to enable using SDFBoundedSourceWrapper
      if 'beam_fn_api' not in experiments:
        # Required so mocking below doesn't mock Create used in assert_that.
        experiments.append('beam_fn_api')

      p._options.view_as(DebugOptions).experiments = experiments

      actual = p | beam.io.Read(source)
      assert_that(actual, equal_to(expected_values))

  @mock.patch('apache_beam.io.iobase._SDFBoundedSourceWrapper.expand')
  def test_sdf_wrapper_overrides_read(self, sdf_wrapper_mock_expand):
    def _fake_wrapper_expand(pbegin):
      return (pbegin
              | beam.Create(['fake']))

    sdf_wrapper_mock_expand.side_effect = _fake_wrapper_expand
    self._run_sdf_wrapper_pipeline(RangeSource(0, 4), ['fake'])

  def test_sdf_wrap_range_source(self):
    self._run_sdf_wrapper_pipeline(RangeSource(0, 4), [0, 1, 2, 3])


class ThreadsafeRestrictionTrackerTest(unittest.TestCase):

  def test_initialization(self):
    with self.assertRaises(ValueError):
      iobase.ThreadsafeRestrictionTracker(RangeSource(0, 1))

  def test_defer_remainder_with_wrong_time_type(self):
    threadsafe_tracker = iobase.ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    with self.assertRaises(ValueError):
      threadsafe_tracker.defer_remainder(10)

  def test_self_checkpoint_immediately(self):
    restriction_tracker = OffsetRestrictionTracker(OffsetRange(0, 10))
    threadsafe_tracker = iobase.ThreadsafeRestrictionTracker(
        restriction_tracker)
    threadsafe_tracker.defer_remainder()
    deferred_residual, deferred_time = threadsafe_tracker.deferred_status()
    expected_residual = OffsetRange(0, 10)
    self.assertEqual(deferred_residual, expected_residual)
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    self.assertEqual(deferred_time, 0)

  def test_self_checkpoint_with_relative_time(self):
    threadsafe_tracker = iobase.ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    threadsafe_tracker.defer_remainder(timestamp.Duration(100))
    time.sleep(2)
    _, deferred_time = threadsafe_tracker.deferred_status()
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    # The expectation = 100 - 2 - some_delta
    self.assertTrue(deferred_time <= 98)

  def test_self_checkpoint_with_absolute_time(self):
    threadsafe_tracker = iobase.ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    now = timestamp.Timestamp.now()
    schedule_time = now + timestamp.Duration(100)
    self.assertTrue(isinstance(schedule_time, timestamp.Timestamp))
    threadsafe_tracker.defer_remainder(schedule_time)
    time.sleep(2)
    _, deferred_time = threadsafe_tracker.deferred_status()
    self.assertTrue(isinstance(deferred_time, timestamp.Duration))
    # The expectation =
    # schedule_time - the time when deferred_status is called - some_delta
    self.assertTrue(deferred_time <= 98)


class RestrictionTrackerViewTest(unittest.TestCase):

  def test_initialization(self):
    with self.assertRaises(ValueError):
      iobase.RestrictionTrackerView(
          OffsetRestrictionTracker(OffsetRange(0, 10)))

  def test_api_expose(self):
    threadsafe_tracker = iobase.ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    tracker_view = iobase.RestrictionTrackerView(threadsafe_tracker)
    current_restriction = tracker_view.current_restriction()
    self.assertEqual(current_restriction, OffsetRange(0, 10))
    self.assertTrue(tracker_view.try_claim(0))
    tracker_view.defer_remainder()
    deferred_remainder, deferred_watermark = (
        threadsafe_tracker.deferred_status())
    self.assertEqual(deferred_remainder, OffsetRange(1, 10))
    self.assertEqual(deferred_watermark, timestamp.Duration())

  def test_non_expose_apis(self):
    threadsafe_tracker = iobase.ThreadsafeRestrictionTracker(
        OffsetRestrictionTracker(OffsetRange(0, 10)))
    tracker_view = iobase.RestrictionTrackerView(threadsafe_tracker)
    with self.assertRaises(AttributeError):
      tracker_view.check_done()
    with self.assertRaises(AttributeError):
      tracker_view.current_progress()
    with self.assertRaises(AttributeError):
      tracker_view.try_split()
    with self.assertRaises(AttributeError):
      tracker_view.deferred_status()


if __name__ == '__main__':
  unittest.main()
