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
This is Combine load test with Synthetic Source. Besides of the standard
input options there are additional options:
* fanout (optional) - number of GBK operations to run in parallel
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_namespace (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* input_options - options for Synthetic Sources.

Example test run on DirectRunner:

python setup.py nosetests \
    --test-pipeline-options="
    --project=big-query-project
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=combine
    --fanout=1
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'" \
    --tests apache_beam.testing.load_tests.combine_test

or:

./gradlew -PloadTest.args='
    --publish_to_big_query=true
    --project=...
    --metrics_dataset=python_load_test
    --metrics_table=combine
    --input_options=\'
      {"num_records": 1,
      "key_size": 1,
      "value_size":1,
      "bundle_size_distribution_type": "const",
      "bundle_size_distribution_param": 1,
      "force_initial_num_bundles": 1}\'
    --runner=DirectRunner
    --fanout=1' \
-PloadTest.mainClass=apache_beam.testing.load_tests.combine_test \
-Prunner=DirectRunner :beam-sdks-python-load-tests:run

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --fanout=1
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --publish_to_big_query=true
        --metrics_dataset=python_load_tests
        --metrics_table=combine
        --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\": 0
        }'" \
    --tests apache_beam.testing.load_tests.combine_test

or:

./gradlew -PloadTest.args='
    --publish_to_big_query=true
    --project=...
    --metrics_dataset=python_load_tests
    --metrics_table=combine
    --temp_location=gs://...
    --input_options=\'
      {"num_records": 1,
      "key_size": 1,
      "value_size":1,
      "bundle_size_distribution_type": "const",
      "bundle_size_distribution_param": 1,
      "force_initial_num_bundles": 1}\'
    --runner=TestDataflowRunner
    --fanout=1' \
-PloadTest.mainClass=
apache_beam.testing.load_tests.combine_test \
-Prunner=
TestDataflowRunner :beam-sdks-python-load-tests:run
"""

from __future__ import absolute_import

import logging
import os
import unittest

import apache_beam as beam
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime

load_test_enabled = False
if os.environ.get('LOAD_TEST_ENABLED') == 'true':
  load_test_enabled = True


@unittest.skipIf(not load_test_enabled, 'Enabled only for phrase triggering.')
class CombineTest(LoadTest):
  def setUp(self):
    super(CombineTest, self).setUp()
    self.fanout = self.pipeline.get_option('fanout')
    if self.fanout is None:
      self.fanout = 1
    else:
      self.fanout = int(self.fanout)

  class _GetElement(beam.DoFn):
    def process(self, element):
      yield element

  def testCombineGlobally(self):
    input = (self.pipeline
             | beam.io.Read(synthetic_pipeline.SyntheticSource(
                 self.parseTestPipelineOptions()))
             | 'Measure time: Start' >> beam.ParDo(
                 MeasureTime(self.metrics_namespace))
            )

    for branch in range(self.fanout):
      # pylint: disable=expression-not-assigned
      (input
       | 'Combine with Top %i' % branch >> beam.CombineGlobally(
           beam.combiners.TopCombineFn(1000))
       | 'Consume %i' % branch >> beam.ParDo(self._GetElement())
       | 'Measure time: End %i' % branch >> beam.ParDo(
           MeasureTime(self.metrics_namespace))
      )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
