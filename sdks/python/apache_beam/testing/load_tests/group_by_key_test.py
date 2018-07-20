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
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'" \
    --tests apache_beam.testing.load_tests.group_by_it_test

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
        \"force_initial_num_bundles\": 0
        }'" \
    --tests apache_beam.testing.load_tests.group_by_it_test

"""

from __future__ import absolute_import

import json
import logging
import unittest

import apache_beam as beam
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.test_pipeline import TestPipeline


class GroupByKeyTest(unittest.TestCase):
  def parseTestPipelineOptions(self):
    return {
        'numRecords': self.inputOptions.get('num_records'),
        'keySizeBytes': self.inputOptions.get('key_size'),
        'valueSizeBytes': self.inputOptions.get('value_size'),
        'bundleSizeDistribution': {
            'type': self.inputOptions.get(
                'bundle_size_distribution_type', 'const'
            ),
            'param': self.inputOptions.get('bundle_size_distribution_param', 0)
        },
        'forceNumInitialBundles': self.inputOptions.get(
            'force_initial_num_bundles', 0
        )
    }

  def setUp(self):
    self.pipeline = TestPipeline(is_integration_test=True)
    self.inputOptions = json.loads(self.pipeline.get_option('input_options'))

  def testGroupByKey(self):
    with self.pipeline as p:
      # pylint: disable=expression-not-assigned
      (p
       | beam.io.Read(synthetic_pipeline.SyntheticSource(
           self.parseTestPipelineOptions()))
       | 'Measure time' >> beam.ParDo(MeasureTime())
       | 'GroupByKey' >> beam.GroupByKey()
       | 'Ungroup' >> beam.FlatMap(
           lambda elm: [(elm[0], v) for v in elm[1]])
      )

      result = p.run()
      result.wait_until_finish()
      metrics = result.metrics().query()
      for dist in metrics['distributions']:
        logging.info("Distribution: %s", dist)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
