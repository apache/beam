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
"""
To run test on DirectRunner

python setup.py nosetests \
    --test-pipeline-options="--input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\":0}'
        --co_input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\":0}'" \
    --tests apache_beam.testing.load_tests.co_group_by_it_test

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\":0
        }'
        --co_input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\":0
        }'" \
    --tests apache_beam.testing.load_tests.co_group_by_it_test

"""

from __future__ import absolute_import

import json
import logging
import unittest

import apache_beam as beam
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.test_pipeline import TestPipeline

INPUT_TAG = 'pc1'
CO_INPUT_TAG = 'pc2'


class CoGroupByKeyTest(unittest.TestCase):

  def parseTestPipelineOptions(self, options):
    return {
        'numRecords': options.get('num_records'),
        'keySizeBytes': options.get('key_size'),
        'valueSizeBytes': options.get('value_size'),
        'bundleSizeDistribution': {
            'type': options.get(
                'bundle_size_distribution_type', 'const'
            ),
            'param': options.get('bundle_size_distribution_param', 0)
        },
        'forceNumInitialBundles': options.get(
            'force_initial_num_bundles', 0
        )
    }

  def setUp(self):
    self.pipeline = TestPipeline(is_integration_test=True)
    self.inputOptions = json.loads(self.pipeline.get_option('input_options'))
    self.coInputOptions = json.loads(
        self.pipeline.get_option('co_input_options'))

  class _Ungroup(beam.DoFn):
    def process(self, element):
      values = element[1]
      inputs = values.get(INPUT_TAG)
      co_inputs = values.get(CO_INPUT_TAG)
      for i in inputs:
        yield i
      for i in co_inputs:
        yield i

  def testCoGroupByKey(self):
    with self.pipeline as p:
      pc1 = (p
             | 'Read ' + INPUT_TAG >> beam.io.Read(
                 synthetic_pipeline.SyntheticSource(
                     self.parseTestPipelineOptions(self.inputOptions)))
             | 'Make ' + INPUT_TAG + ' iterable' >> beam.Map(lambda x: (x, x))
            )

      pc2 = (p
             | 'Read ' + CO_INPUT_TAG >> beam.io.Read(
                 synthetic_pipeline.SyntheticSource(
                     self.parseTestPipelineOptions(self.coInputOptions)))
             | 'Make ' + CO_INPUT_TAG + ' iterable' >> beam.Map(
                 lambda x: (x, x))
            )
      # pylint: disable=expression-not-assigned
      ({INPUT_TAG: pc1, CO_INPUT_TAG: pc2}
       | 'CoGroupByKey: ' >> beam.CoGroupByKey()
       | 'Consume Joined Collections' >> beam.ParDo(self._Ungroup())
       | 'Measure time' >> beam.ParDo(MeasureTime())
      )

      result = p.run()
      result.wait_until_finish()
      metrics = result.metrics().query()

      for dist in metrics['distributions']:
        logging.info("Distribution: %s", dist)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
