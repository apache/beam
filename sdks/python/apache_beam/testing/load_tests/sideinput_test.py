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

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
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

"""

from __future__ import absolute_import

import json
import logging
import unittest

import apache_beam as beam
from apache_beam.pvalue import AsIter
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.test_pipeline import TestPipeline


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

  def testSideInput(self):
    def join_fn(element, side_input, iterations):
      list = []
      for i in range(iterations):
        for key, value in side_input:
          if i == iterations - 1:
            list.append({key: element[1]+value})
      yield list

    with self.pipeline as p:
      main_input = (p
                    | "Read pcoll 1" >> beam.io.Read(
                        synthetic_pipeline.SyntheticSource(
                            self._parseTestPipelineOptions()))
                   )

      side_input = (p
                    | "Read pcoll 2" >> beam.io.Read(
                        synthetic_pipeline.SyntheticSource(
                            self._getSideInput()))
                   )
      # pylint: disable=expression-not-assigned
      (main_input
       | "Merge" >> beam.ParDo(
           join_fn,
           AsIter(side_input),
           self.iterations)
       | 'Measure time' >> beam.ParDo(MeasureTime())
      )

      result = p.run()
      result.wait_until_finish()
      metrics = result.metrics().query()

      for dist in metrics['distributions']:
        logging.info("Distribution: %s", dist)

  if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    unittest.main()
