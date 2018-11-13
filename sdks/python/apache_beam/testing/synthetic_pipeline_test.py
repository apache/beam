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

from __future__ import absolute_import

import glob
import json
import logging
import tempfile
import time
import unittest

import apache_beam as beam
from apache_beam.io import source_test_utils
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  import numpy as np
except ImportError:
  np = None


def input_spec(num_records, key_size, value_size,
               bundle_size_distribution_type='const',
               bundle_size_distribution_param=0,
               force_initial_num_bundles=0):
  return {
      'numRecords': num_records,
      'keySizeBytes': key_size,
      'valueSizeBytes': value_size,
      'bundleSizeDistribution': {'type': bundle_size_distribution_type,
                                 'param': bundle_size_distribution_param},
      'forceNumInitialBundles': force_initial_num_bundles,
  }


@unittest.skipIf(np is None, 'Synthetic source dependencies are not installed')
class SyntheticPipelineTest(unittest.TestCase):

  # pylint: disable=expression-not-assigned

  def testSyntheticStep(self):
    start = time.time()
    with beam.Pipeline() as p:
      pcoll = p | beam.Create(list(range(10))) | beam.ParDo(
          synthetic_pipeline.SyntheticStep(0, 0.5, 10))
      assert_that(
          pcoll | beam.combiners.Count.Globally(), equal_to([100]))

    elapsed = time.time() - start
    # TODO(chamikaramj): Fix the flaky time based bounds.
    self.assertTrue(0.5 <= elapsed <= 3, elapsed)

  def testSyntheticSource(self):
    def assert_size(element, expected_size):
      assert len(element) == expected_size
    with beam.Pipeline() as p:
      pcoll = (
          p | beam.io.Read(
              synthetic_pipeline.SyntheticSource(input_spec(300, 5, 15))))
      (pcoll
       | beam.Map(lambda elm: elm[0]) | 'key' >> beam.Map(assert_size, 5))
      (pcoll
       | beam.Map(lambda elm: elm[1]) | 'value' >> beam.Map(assert_size, 15))
      assert_that(pcoll | beam.combiners.Count.Globally(),
                  equal_to([300]))

  def testSyntheticSourceSplitEven(self):
    source = synthetic_pipeline.SyntheticSource(
        input_spec(1000, 1, 1, 'const', 0))
    splits = source.split(100)
    sources_info = [(split.source, split.start_position, split.stop_position)
                    for split in splits]
    self.assertEquals(20, len(sources_info))
    source_test_utils.assert_sources_equal_reference_source(
        (source, None, None), sources_info)

  def testSyntheticSourceSplitUneven(self):
    source = synthetic_pipeline.SyntheticSource(
        input_spec(1000, 1, 1, 'zipf', 3, 10))
    splits = source.split(100)
    sources_info = [(split.source, split.start_position, split.stop_position)
                    for split in splits]
    self.assertEquals(10, len(sources_info))
    source_test_utils.assert_sources_equal_reference_source(
        (source, None, None), sources_info)

  def testSplitAtFraction(self):
    source = synthetic_pipeline.SyntheticSource(input_spec(10, 1, 1))
    source_test_utils.assert_split_at_fraction_exhaustive(source)
    source_test_utils.assert_split_at_fraction_fails(source, 5, 0.3)
    source_test_utils.assert_split_at_fraction_succeeds_and_consistent(
        source, 1, 0.3)

  def run_pipeline(self, barrier, writes_output=True):
    steps = [{'per_element_delay': 1}, {'per_element_delay': 1}]
    args = ['--barrier=%s' % barrier, '--runner=DirectRunner',
            '--steps=%s' % json.dumps(steps),
            '--input=%s' % json.dumps(input_spec(10, 1, 1))]
    if writes_output:
      output_location = tempfile.NamedTemporaryFile().name
      args.append('--output=%s' % output_location)

    synthetic_pipeline.run(args)

    # Verify output
    if writes_output:
      read_output = []
      for file_name in glob.glob(output_location + '*'):
        with open(file_name, 'rb') as f:
          read_output.extend(f.read().splitlines())

      self.assertEqual(10, len(read_output))

  def testPipelineShuffle(self):
    self.run_pipeline('shuffle')

  def testPipelineSideInput(self):
    self.run_pipeline('side-input')

  def testPipelineExpandGBK(self):
    self.run_pipeline('expand-gbk', False)

  def testPipelineExpandSideOutput(self):
    self.run_pipeline('expand-second-output', False)

  def testPipelineMergeGBK(self):
    self.run_pipeline('merge-gbk')

  def testPipelineMergeSideInput(self):
    self.run_pipeline('merge-side-input')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
