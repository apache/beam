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
This is SideInput load test with Synthetic Source. Besides of the standard
input options there are additional options:
* number_of_counter_operations - number of pardo operations
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_namespace (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* input_options - options for Synthetic Sources.

To run test on DirectRunner

python setup.py nosetests \
    --test-pipeline-options="
    --project=big-query-project
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=side_input
    --number_of_counter_operations=1000
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'
   " \
    --tests apache_beam.testing.load_tests.sideinput_test

or:

./gradlew -PloadTest.args='
    --publish_to_big_query=true
    --project=...
    --metrics_dataset=python_load_tests
    --metrics_table=side_input
    --input_options=\'
      {"num_records": 1,
      "key_size": 1,
      "value_size":1,
      "bundle_size_distribution_type": "const",
      "bundle_size_distribution_param": 1,
      "force_initial_num_bundles": 1}\'
    --runner=DirectRunner' \
-PloadTest.mainClass=
apache_beam.testing.load_tests.sideinput_test \
-Prunner=DirectRunner :beam-sdks-python-load-tests:run

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --publish_to_big_query=true
        --metrics_dataset=python_load_tests
        --metrics_table=side_input
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --number_of_counter_operations=1000
        --input_options='{
        \"num_records\": 1,
        \"key_size\": 1,
        \"value_size\":1,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\": 0
        }'
        " \
    --tests apache_beam.testing.load_tests.sideinput_test

or:

./gradlew -PloadTest.args='
    --publish_to_big_query=true
    --project=...
    --metrics_dataset=python_load_tests
    --metrics_table=side_input
    --temp_location=gs://...
    --input_options=\'
      {"num_records": 1,
      "key_size": 1,
      "value_size":1,
      "bundle_size_distribution_type": "const",
      "bundle_size_distribution_param": 1,
      "force_initial_num_bundles": 1}\'
    --runner=TestDataflowRunner' \
-PloadTest.mainClass=
apache_beam.testing.load_tests.sideinput_test:SideInputTest.testSideInput \
-Prunner=TestDataflowRunner :beam-sdks-python-load-tests:run
"""

from __future__ import absolute_import

import logging
import os
import unittest

import apache_beam as beam
from apache_beam.pvalue import AsIter
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime

load_test_enabled = False
if os.environ.get('LOAD_TEST_ENABLED') == 'true':
  load_test_enabled = True


@unittest.skipIf(not load_test_enabled, 'Enabled only for phrase triggering.')
class SideInputTest(LoadTest):
  def _getSideInput(self):
    side_input = self.parseTestPipelineOptions()
    side_input['numRecords'] = side_input['numRecords']
    side_input['keySizeBytes'] = side_input['keySizeBytes']
    side_input['valueSizeBytes'] = side_input['valueSizeBytes']
    return side_input

  def setUp(self):
    super(SideInputTest, self).setUp()

    self.iterations = self.pipeline.get_option('number_of_counter_operations')
    if not self.iterations:
      self.iterations = 1
    self.iterations = int(self.iterations)

  def testSideInput(self):
    def join_fn(element, side_input, iterations):
      list = []
      for i in range(iterations):
        for key, value in side_input:
          if i == iterations - 1:
            list.append({key: element[1]+value})
      yield list

    main_input = (self.pipeline
                  | "Read pcoll 1" >> beam.io.Read(
                      synthetic_pipeline.SyntheticSource(
                          self.parseTestPipelineOptions()))
                  | 'Measure time: Start pcoll 1' >> beam.ParDo(
                      MeasureTime(self.metrics_namespace))
                 )

    side_input = (self.pipeline
                  | "Read pcoll 2" >> beam.io.Read(
                      synthetic_pipeline.SyntheticSource(
                          self._getSideInput()))
                  | 'Measure time: Start pcoll 2' >> beam.ParDo(
                      MeasureTime(self.metrics_namespace))
                 )
    # pylint: disable=expression-not-assigned
    (main_input
     | "Merge" >> beam.ParDo(
         join_fn,
         AsIter(side_input),
         self.iterations)
     | 'Measure time' >> beam.ParDo(MeasureTime(self.metrics_namespace))
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
