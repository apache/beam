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

"""Tests for apache_beam.testing.synthetic_pipeline."""

# pytype: skip-file

import glob
import json
import logging
import tempfile
import time
import unittest

import apache_beam as beam
from apache_beam.io import source_test_utils
from apache_beam.io.restriction_trackers import OffsetRange
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  import numpy  # pylint: disable=unused-import
except ImportError:
  NP_INSTALLED = False
else:
  NP_INSTALLED = True


def input_spec(
    num_records,
    key_size,
    value_size,
    bundle_size_distribution_type='const',
    bundle_size_distribution_param=0,
    force_initial_num_bundles=0):
  return {
      'numRecords': num_records,
      'keySizeBytes': key_size,
      'valueSizeBytes': value_size,
      'bundleSizeDistribution': {
          'type': bundle_size_distribution_type,
          'param': bundle_size_distribution_param
      },
      'forceNumInitialBundles': force_initial_num_bundles,
  }


@unittest.skipIf(
    not NP_INSTALLED, 'Synthetic source dependencies are not installed')
class SyntheticPipelineTest(unittest.TestCase):

  # pylint: disable=expression-not-assigned

  def test_synthetic_step_multiplies_output_elements_count(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create(list(range(10))) | beam.ParDo(
          synthetic_pipeline.SyntheticStep(0, 0, 10))
      assert_that(pcoll | beam.combiners.Count.Globally(), equal_to([100]))

  def test_minimal_runtime_with_synthetic_step_delay(self):
    start = time.time()
    with beam.Pipeline() as p:
      p | beam.Create(list(range(10))) | beam.ParDo(
          synthetic_pipeline.SyntheticStep(0, 0.5, 10))

    elapsed = time.time() - start
    self.assertGreaterEqual(elapsed, 0.5, elapsed)

  def test_synthetic_sdf_step_multiplies_output_elements_count(self):
    with beam.Pipeline() as p:
      pcoll = p | beam.Create(list(range(10))) | beam.ParDo(
          synthetic_pipeline.get_synthetic_sdf_step(0, 0, 10))
      assert_that(pcoll | beam.combiners.Count.Globally(), equal_to([100]))

  def test_minimal_runtime_with_synthetic_sdf_step_bundle_delay(self):
    start = time.time()
    with beam.Pipeline() as p:
      p | beam.Create(list(range(10))) | beam.ParDo(
          synthetic_pipeline.get_synthetic_sdf_step(0, 0.5, 10))

    elapsed = time.time() - start
    self.assertGreaterEqual(elapsed, 0.5, elapsed)

  def test_synthetic_step_split_provider(self):
    provider = synthetic_pipeline.SyntheticSDFStepRestrictionProvider(
        5, 2, False, False, None)

    self.assertEqual(
        list(provider.split('ab', OffsetRange(2, 15))),
        [OffsetRange(2, 8), OffsetRange(8, 15)])
    self.assertEqual(
        list(provider.split('ab', OffsetRange(0, 8))),
        [OffsetRange(0, 4), OffsetRange(4, 8)])
    self.assertEqual(list(provider.split('ab', OffsetRange(0, 0))), [])
    self.assertEqual(
        list(provider.split('ab', OffsetRange(2, 3))), [OffsetRange(2, 3)])

    provider = synthetic_pipeline.SyntheticSDFStepRestrictionProvider(
        10, 1, False, False, None)
    self.assertEqual(
        list(provider.split('ab', OffsetRange(1, 10))), [OffsetRange(1, 10)])
    self.assertEqual(provider.restriction_size('ab', OffsetRange(1, 10)), 9 * 2)

    provider = synthetic_pipeline.SyntheticSDFStepRestrictionProvider(
        10, 3, False, False, None)
    self.assertEqual(
        list(provider.split('ab', OffsetRange(1, 10))),
        [OffsetRange(1, 4), OffsetRange(4, 7), OffsetRange(7, 10)])
    self.assertEqual(provider.initial_restriction('a'), OffsetRange(0, 10))

    provider = synthetic_pipeline.SyntheticSDFStepRestrictionProvider(
        10, 3, False, False, 45)
    self.assertEqual(provider.restriction_size('ab', OffsetRange(1, 3)), 45)

    tracker = provider.create_tracker(OffsetRange(1, 6))
    tracker.try_claim(1)  # Claim to allow splitting.
    self.assertEqual(
        tracker.try_split(.5), (OffsetRange(1, 3), OffsetRange(3, 6)))

  def verify_random_splits(self, provider, restriction, bundles):
    ranges = list(provider.split('ab', restriction))

    prior_stop = restriction.start
    for r in ranges:
      self.assertEqual(r.start, prior_stop)
      prior_stop = r.stop
    self.assertEqual(prior_stop, restriction.stop)
    self.assertEqual(len(ranges), bundles)

  def testSyntheticStepSplitProviderUnevenChunks(self):
    bundles = 4
    provider = synthetic_pipeline.SyntheticSDFStepRestrictionProvider(
        5, bundles, True, False, None)
    self.verify_random_splits(provider, OffsetRange(4, 10), bundles)
    self.verify_random_splits(provider, OffsetRange(4, 4), 0)
    self.verify_random_splits(provider, OffsetRange(0, 1), 1)
    self.verify_random_splits(provider, OffsetRange(0, bundles - 2), bundles)

  def test_synthetic_step_split_provider_no_liquid_sharding(self):
    # Verify Liquid Sharding Works
    provider = synthetic_pipeline.SyntheticSDFStepRestrictionProvider(
        5, 5, True, False, None)
    tracker = provider.create_tracker(OffsetRange(1, 6))
    tracker.try_claim(2)
    self.assertEqual(
        tracker.try_split(.5), (OffsetRange(1, 4), OffsetRange(4, 6)))

    # Verify No Liquid Sharding
    provider = synthetic_pipeline.SyntheticSDFStepRestrictionProvider(
        5, 5, True, True, None)
    tracker = provider.create_tracker(OffsetRange(1, 6))
    tracker.try_claim(2)
    self.assertEqual(tracker.try_split(3), None)

  def test_synthetic_source(self):
    def assert_size(element, expected_size):
      assert len(element) == expected_size

    with beam.Pipeline() as p:
      pcoll = (
          p | beam.io.Read(
              synthetic_pipeline.SyntheticSource(input_spec(300, 5, 15))))
      (pcoll | beam.Map(lambda elm: elm[0]) | 'key' >> beam.Map(assert_size, 5))
      (
          pcoll
          | beam.Map(lambda elm: elm[1]) | 'value' >> beam.Map(assert_size, 15))
      assert_that(pcoll | beam.combiners.Count.Globally(), equal_to([300]))

  def test_synthetic_source_split_even(self):
    source = synthetic_pipeline.SyntheticSource(
        input_spec(1000, 1, 1, 'const', 0))
    splits = source.split(100)
    sources_info = [(split.source, split.start_position, split.stop_position)
                    for split in splits]
    self.assertEqual(20, len(sources_info))
    source_test_utils.assert_sources_equal_reference_source(
        (source, None, None), sources_info)

  def test_synthetic_source_split_uneven(self):
    source = synthetic_pipeline.SyntheticSource(
        input_spec(1000, 1, 1, 'zipf', 3, 10))
    splits = source.split(100)
    sources_info = [(split.source, split.start_position, split.stop_position)
                    for split in splits]
    self.assertEqual(10, len(sources_info))
    source_test_utils.assert_sources_equal_reference_source(
        (source, None, None), sources_info)

  def test_split_at_fraction(self):
    source = synthetic_pipeline.SyntheticSource(input_spec(10, 1, 1))
    source_test_utils.assert_split_at_fraction_exhaustive(source)
    source_test_utils.assert_split_at_fraction_fails(source, 5, 0.3)
    source_test_utils.assert_split_at_fraction_succeeds_and_consistent(
        source, 1, 0.3)

  def run_pipeline(self, barrier, writes_output=True):
    steps = [{
        'per_element_delay': 1
    }, {
        'per_element_delay': 1, 'splittable': True
    }]
    args = [
        '--barrier=%s' % barrier,
        '--runner=DirectRunner',
        '--steps=%s' % json.dumps(steps),
        '--input=%s' % json.dumps(input_spec(10, 1, 1))
    ]
    if writes_output:
      output_location = tempfile.NamedTemporaryFile().name
      args.append('--output=%s' % output_location)

    synthetic_pipeline.run(args, save_main_session=False)

    # Verify output
    if writes_output:
      read_output = []
      for file_name in glob.glob(output_location + '*'):
        with open(file_name, 'rb') as f:
          read_output.extend(f.read().splitlines())

      self.assertEqual(10, len(read_output))

  def test_pipeline_shuffle(self):
    self.run_pipeline('shuffle')

  def test_pipeline_side_input(self):
    self.run_pipeline('side-input')

  def test_pipeline_expand_gbk(self):
    self.run_pipeline('expand-gbk', False)

  def test_pipeline_expand_side_output(self):
    self.run_pipeline('expand-second-output', False)

  def test_pipeline_merge_gbk(self):
    self.run_pipeline('merge-gbk')

  def test_pipeline_merge_side_input(self):
    self.run_pipeline('merge-side-input')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
