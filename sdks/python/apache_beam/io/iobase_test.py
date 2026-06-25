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

"""Unit tests for classes in iobase.py."""

# pytype: skip-file

import unittest

import apache_beam as beam
import mock
from apache_beam.io import iobase
from apache_beam.io import range_trackers
from apache_beam.io.concat_source import ConcatSource
from apache_beam.io.concat_source_test import RangeSource
from apache_beam.io.iobase import SourceBundle
from apache_beam.options.pipeline_options import DebugOptions
from apache_beam.portability import common_urns
from apache_beam.portability import python_urns
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class SDFBoundedSourceRestrictionProviderTest(unittest.TestCase):
  def setUp(self):
    self.initial_range_start = 0
    self.initial_range_stop = 4
    self.initial_range_source = RangeSource(
        self.initial_range_start, self.initial_range_stop)
    self.sdf_restriction_provider = (
        iobase._SDFBoundedSourceRestrictionProvider(desired_chunk_size=2))

  def test_initial_restriction(self):
    element = self.initial_range_source
    restriction = (self.sdf_restriction_provider.initial_restriction(element))
    self.assertTrue(
        isinstance(restriction, iobase._SDFBoundedSourceRestriction))
    self.assertTrue(isinstance(restriction._source_bundle, SourceBundle))
    self.assertEqual(
        self.initial_range_start, restriction._source_bundle.start_position)
    self.assertEqual(
        self.initial_range_stop, restriction._source_bundle.stop_position)
    self.assertTrue(isinstance(restriction._source_bundle.source, RangeSource))
    self.assertEqual(restriction._range_tracker, None)

  def test_create_tracker(self):
    expected_start = 1
    expected_stop = 3
    source_bundle = SourceBundle(
        expected_stop - expected_start,
        RangeSource(1, 3),
        expected_start,
        expected_stop)
    restriction_tracker = (
        self.sdf_restriction_provider.create_tracker(
            iobase._SDFBoundedSourceRestriction(source_bundle)))
    self.assertTrue(
        isinstance(
            restriction_tracker, iobase._SDFBoundedSourceRestrictionTracker))
    self.assertEqual(expected_start, restriction_tracker.start_pos())
    self.assertEqual(expected_stop, restriction_tracker.stop_pos())

  def test_simple_source_split(self):
    element = self.initial_range_source
    restriction = (self.sdf_restriction_provider.initial_restriction(element))
    expect_splits = [(0, 2), (2, 4)]
    split_bundles = list(
        self.sdf_restriction_provider.split(element, restriction))
    self.assertTrue(
        all(
            isinstance(bundle._source_bundle, SourceBundle)
            for bundle in split_bundles))

    splits = ([(
        bundle._source_bundle.start_position,
        bundle._source_bundle.stop_position) for bundle in split_bundles])
    self.assertEqual(expect_splits, list(splits))

  def test_concat_source_split(self):
    element = self.initial_range_source
    initial_concat_source = ConcatSource([self.initial_range_source])
    sdf_concat_restriction_provider = (
        iobase._SDFBoundedSourceRestrictionProvider(desired_chunk_size=2))
    restriction = (self.sdf_restriction_provider.initial_restriction(element))
    expect_splits = [(0, 2), (2, 4)]
    split_bundles = list(
        sdf_concat_restriction_provider.split(
            initial_concat_source, restriction))
    self.assertTrue(
        all(
            isinstance(bundle._source_bundle, SourceBundle)
            for bundle in split_bundles))
    splits = ([(
        bundle._source_bundle.start_position,
        bundle._source_bundle.stop_position) for bundle in split_bundles])
    self.assertEqual(expect_splits, list(splits))

  def test_restriction_size(self):
    element = self.initial_range_source
    restriction = (self.sdf_restriction_provider.initial_restriction(element))
    split_1, split_2 = self.sdf_restriction_provider.split(element, restriction)
    split_1_size = self.sdf_restriction_provider.restriction_size(
        element, split_1)
    split_2_size = self.sdf_restriction_provider.restriction_size(
        element, split_2)
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
        iobase._SDFBoundedSourceRestrictionTracker(
            iobase._SDFBoundedSourceRestriction(source_bundle)))

  def test_current_restriction_before_split(self):
    current_restriction = (self.sdf_restriction_tracker.current_restriction())
    self.assertEqual(
        self.initial_start_pos,
        current_restriction._source_bundle.start_position)
    self.assertEqual(
        self.initial_stop_pos, current_restriction._source_bundle.stop_position)
    self.assertEqual(
        self.initial_start_pos,
        current_restriction._range_tracker.start_position())
    self.assertEqual(
        self.initial_stop_pos,
        current_restriction._range_tracker.stop_position())

  def test_current_restriction_after_split(self):
    fraction_of_remainder = 0.5
    self.sdf_restriction_tracker.try_claim(1)
    expected_restriction, _ = (
        self.sdf_restriction_tracker.try_split(fraction_of_remainder))
    current_restriction = self.sdf_restriction_tracker.current_restriction()
    self.assertEqual(
        expected_restriction._source_bundle, current_restriction._source_bundle)
    self.assertTrue(current_restriction._range_tracker)

  def test_try_split_at_remainder(self):
    fraction_of_remainder = 0.4
    expected_primary = (0, 2, 2.0)
    expected_residual = (2, 4, 2.0)
    self.sdf_restriction_tracker.try_claim(0)
    actual_primary, actual_residual = (
        self.sdf_restriction_tracker.try_split(fraction_of_remainder))
    self.assertEqual(
        expected_primary,
        (
            actual_primary._source_bundle.start_position,
            actual_primary._source_bundle.stop_position,
            actual_primary._source_bundle.weight))
    self.assertEqual(
        expected_residual,
        (
            actual_residual._source_bundle.start_position,
            actual_residual._source_bundle.stop_position,
            actual_residual._source_bundle.weight))
    self.assertEqual(
        actual_primary._source_bundle.weight,
        self.sdf_restriction_tracker.current_restriction().weight())

  def test_try_split_with_any_exception(self):
    source_bundle = SourceBundle(
        range_trackers.OffsetRangeTracker.OFFSET_INFINITY,
        RangeSource(0, range_trackers.OffsetRangeTracker.OFFSET_INFINITY),
        0,
        range_trackers.OffsetRangeTracker.OFFSET_INFINITY)
    self.sdf_restriction_tracker = (
        iobase._SDFBoundedSourceRestrictionTracker(
            iobase._SDFBoundedSourceRestriction(source_bundle)))
    self.sdf_restriction_tracker.try_claim(0)
    self.assertIsNone(self.sdf_restriction_tracker.try_split(0.5))


class UseSdfBoundedSourcesTests(unittest.TestCase):
  def _run_sdf_wrapper_pipeline(self, source, expected_values):
    with beam.Pipeline() as p:
      p._options.view_as(DebugOptions).add_experiment('beam_fn_api')

      actual = p | beam.io.Read(source)
      assert_that(actual, equal_to(expected_values))

  @mock.patch('apache_beam.io.iobase.SDFBoundedSourceReader.expand')
  def test_sdf_wrapper_overrides_read(self, sdf_wrapper_mock_expand):
    def _fake_wrapper_expand(pbegin):
      return pbegin | beam.Map(lambda x: 'fake')

    sdf_wrapper_mock_expand.side_effect = _fake_wrapper_expand
    self._run_sdf_wrapper_pipeline(RangeSource(0, 4), ['fake'])

  def test_sdf_wrap_range_source(self):
    self._run_sdf_wrapper_pipeline(RangeSource(0, 4), [0, 1, 2, 3])


class UseSdfUnboundedSourcesTests(unittest.TestCase):
  """Covers the UnboundedSource branch in
  ``iobase.Read.expand()``. Uses ``UnboundedCountingSource`` from
  ``unbounded_source_test`` as a finite fake source (no network).
  """
  def test_read_end_to_end_unbounded(self):
    from apache_beam.io.unbounded_source_test import UnboundedCountingSource
    with beam.Pipeline() as p:
      out = p | beam.io.Read(UnboundedCountingSource(5))
      assert_that(out, equal_to([0, 1, 2, 3, 4]))

  def test_read_unbounded_pcollection_is_unbounded(self):
    from apache_beam.io.unbounded_source_test import UnboundedCountingSource
    p = beam.Pipeline()
    out = p | beam.io.Read(UnboundedCountingSource(3))
    self.assertFalse(out.is_bounded)

  def test_read_unbounded_serializes_as_expanded_composite(self):
    from apache_beam.io.unbounded_source_test import UnboundedCountingSource
    p = beam.Pipeline()
    p | 'ReadIt' >> beam.io.Read(UnboundedCountingSource(3))

    proto = p.to_runner_api(use_fake_coders=True)
    transforms = proto.components.transforms.values()
    deprecated_reads = [
        transform.unique_name for transform in transforms
        if transform.spec.urn == common_urns.deprecated_primitives.READ.urn
    ]
    read_transforms = [
        transform for transform in proto.components.transforms.values()
        if transform.unique_name == 'ReadIt'
    ]

    self.assertEqual([], deprecated_reads)
    self.assertEqual(1, len(read_transforms))
    self.assertEqual(
        python_urns.GENERIC_COMPOSITE_TRANSFORM, read_transforms[0].spec.urn)
    self.assertTrue(read_transforms[0].subtransforms)


if __name__ == '__main__':
  unittest.main()
