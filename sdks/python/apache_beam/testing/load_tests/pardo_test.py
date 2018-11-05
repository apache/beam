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
This is ParDo load test with Synthetic Source. Besides of the standard
input options there are additional options:
* number_of_counter_operations - number of pardo operations
* metrics_project_id - the gcp project in case of saving metrics in big query
* input_options - options for Synthetic Sources.

Example test run on DirectRunner:

python setup.py nosetests \
    --test-pipeline-options="
    --number_of_counter_operations=1000
    --metrics_project_id=big-query-project
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'" \
    --tests apache_beam.testing.load_tests.pardo_test

To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --output=gc
        --number_of_counter_operations=1000
        --metrics_project_id=big-query-project
        --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\": 0
        }'" \
    --tests apache_beam.testing.load_tests.pardo_test

"""

from __future__ import absolute_import

import json
import logging
import unittest

import apache_beam as beam
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test_metrics_utils import BigQueryMetricsCollector
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.load_tests.load_test_metrics_utils import count_metrics
from apache_beam.testing.test_pipeline import TestPipeline

NAMESPACE = 'pardo'
COUNTER_LABEL = "total_bytes_count"
RUNTIME_LABEL = 'runtime'


class ParDoTest(unittest.TestCase):

  def parseTestPipelineOptions(self):
    return {'numRecords': self.input_options.get('num_records'),
            'keySizeBytes': self.input_options.get('key_size'),
            'valueSizeBytes': self.input_options.get('value_size'),
            'bundleSizeDistribution': {
                'type': self.input_options.get(
                    'bundle_size_distribution_type', 'const'
                ),
                'param': self.input_options.get(
                    'bundle_size_distribution_param', 0
                )
            },
            'forceNumInitialBundles': self.input_options.get(
                'force_initial_num_bundles', 0
            )
           }

  def setUp(self):
    self.pipeline = TestPipeline(is_integration_test=True)
    self.output = self.pipeline.get_option('output')
    self.iterations = self.pipeline.get_option('number_of_counter_operations')
    self.input_options = json.loads(self.pipeline.get_option('input_options'))

    metrics_project_id = self.pipeline.get_option('metrics_project_id')
    self.bigQuery = None
    if metrics_project_id is not None:
      schema = [{'name': RUNTIME_LABEL, 'type': 'FLOAT', 'mode': 'REQUIRED'},
                {'name': COUNTER_LABEL, 'type': 'INTEGER', 'mode': 'REQUIRED'}]
      self.bigQuery = BigQueryMetricsCollector(
          metrics_project_id,
          NAMESPACE,
          schema
      )

  class _GetElement(beam.DoFn):
    @count_metrics(namespace=NAMESPACE, counter_name=COUNTER_LABEL)
    def process(self, element):
      yield element

  def testParDo(self):
    if self.iterations is None:
      num_runs = 1
    else:
      num_runs = int(self.iterations)

    with self.pipeline as p:
      pc = (p
            | 'Read synthetic' >> beam.io.Read(
                synthetic_pipeline.SyntheticSource(
                    self.parseTestPipelineOptions()
                ))
            | 'Measure time' >> beam.ParDo(MeasureTime(NAMESPACE))
           )

      for i in range(num_runs):
        label = 'Step: %d' % i
        pc = (pc
              | label >> beam.ParDo(self._GetElement()))

      if self.output is not None:
        # pylint: disable=expression-not-assigned
        (pc
         | "Write" >> beam.io.WriteToText(self.output)
        )

      result = p.run()
      result.wait_until_finish()

      if self.bigQuery is not None:
        self.bigQuery.save_metrics(result)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
