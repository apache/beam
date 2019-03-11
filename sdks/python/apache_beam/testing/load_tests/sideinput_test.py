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
    --project=big-query-project
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=side_input
    --test-pipeline-options="
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
apache_beam.testing.load_tests.sideinput_test:SideInputTest.test \
-Prunner=TestDataflowRunner :beam-sdks-python-load-tests:run
"""

from __future__ import absolute_import

import json
import logging
import os
import unittest

import apache_beam as beam
from apache_beam.pvalue import AsIter
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.load_tests.load_test_metrics_utils import MetricsMonitor
from apache_beam.testing.test_pipeline import TestPipeline

load_test_enabled = False
if os.environ.get('LOAD_TEST_ENABLED') == 'true':
  load_test_enabled = True


@unittest.skipIf(not load_test_enabled, 'Enabled only for phrase triggering.')
class SideInputTest(unittest.TestCase):
  def _parseTestPipelineOptions(self):
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

  def _getSideInput(self):
    side_input = self._parseTestPipelineOptions()
    side_input['numRecords'] = side_input['numRecords']
    side_input['keySizeBytes'] = side_input['keySizeBytes']
    side_input['valueSizeBytes'] = side_input['valueSizeBytes']
    return side_input

  def _getPerElementDelaySec(self):
    return self.syntheticStepOptions.get('per_element_delay_sec', 0)

  def _getPerBundleDelaySec(self):
    return self.syntheticStepOptions.get('per_bundle_delay_sec', 0)

  def _getOutputRecordsPerInputRecords(self):
    return self.syntheticStepOptions.get('output_records_per_input_records', 0)

  def setUp(self):
    self.pipeline = TestPipeline()
    self.inputOptions = json.loads(self.pipeline.get_option('input_options'))
    self.iterations = self.pipeline.get_option('number_of_counter_operations')
    if self.iterations is None:
      self.iterations = 1
    self.iterations = int(self.iterations)

    self.metrics_monitor = self.pipeline.get_option('publish_to_big_query')
    metrics_project_id = self.pipeline.get_option('project')
    self.metrics_namespace = self.pipeline.get_option('metrics_table')
    metrics_dataset = self.pipeline.get_option('metrics_dataset')

    check = metrics_project_id and self.metrics_namespace and metrics_dataset \
            is not None
    if not self.metrics_monitor:
      logging.info('Metrics will not be collected')
    elif check:
      self.metrics_monitor = MetricsMonitor(
          project_name=metrics_project_id,
          table=self.metrics_namespace,
          dataset=metrics_dataset,
      )
    else:
      raise ValueError('One or more of parameters for collecting metrics '
                       'are empty.')

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
                          self._parseTestPipelineOptions()))
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

    result = self.pipeline.run()
    result.wait_until_finish()

    if self.metrics_monitor is not None:
      self.metrics_monitor.send_metrics(result)

  if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
