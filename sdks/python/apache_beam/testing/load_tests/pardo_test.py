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
* iterations - number of subsequent ParDo transforms to be performed,
* number_of_counters - number of counter metrics to be created for one ParDo
transform,
* number_of_counter_operations - number of operations on counters to be
performed in one ParDo,
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_dataset (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* input_options - options for Synthetic Sources.

Example test run on DirectRunner:

python setup.py nosetests \
    --test-pipeline-options="
    --iterations=1000
    --number_of_counters=1
    --number_of_counter_operations=1
    --project=big-query-project
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=pardo
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\":15,
    \"bundle_size_distribution_type\": \"const\",
    \"bundle_size_distribution_param\": 1,
    \"force_initial_num_bundles\": 0
    }'" \
    --tests apache_beam.testing.load_tests.pardo_test

or:

./gradlew -PloadTest.args='
    --publish_to_big_query=true
    --project=...
    --metrics_dataset=python_load_tests
    --metrics_table=pardo
    --input_options=\'
      {"num_records": 1,
      "key_size": 1,
      "value_size":1,
      "bundle_size_distribution_type": "const",
      "bundle_size_distribution_param": 1,
      "force_initial_num_bundles": 1}\'
    --runner=DirectRunner' \
-PloadTest.mainClass=apache_beam.testing.load_tests.pardo_test \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load-tests:run


To run test on other runner (ex. Dataflow):

python setup.py nosetests \
    --test-pipeline-options="
        --runner=TestDataflowRunner
        --project=...
        --staging_location=gs://...
        --temp_location=gs://...
        --sdk_location=./dist/apache-beam-x.x.x.dev0.tar.gz
        --iterations=1000
        --number_of_counters=1
        --number_of_counter_operations=1
        --publish_to_big_query=true
        --metrics_dataset=python_load_tests
        --metrics_table=pardo
        --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\":15,
        \"bundle_size_distribution_type\": \"const\",
        \"bundle_size_distribution_param\": 1,
        \"force_initial_num_bundles\": 0
        }'" \
    --tests apache_beam.testing.load_tests.pardo_test

or:

./gradlew -PloadTest.args='
    --publish_to_big_query=true
    --project=...
    --metrics_dataset=python_load_tests
    --metrics_table=pardo
    --temp_location=gs://...
    --input_options=\'
      {"num_records": 1,
      "key_size": 1,
      "value_size":1,
      "bundle_size_distribution_type": "const",
      "bundle_size_distribution_param": 1,
      "force_initial_num_bundles": 1}\'
    --runner=TestDataflowRunner' \
-PloadTest.mainClass=apache_beam.testing.load_tests.pardo_test \
-Prunner=TestDataflowRunner :sdks:python:apache_beam:testing:load-tests:run
"""

# pytype: skip-file

from __future__ import absolute_import

import logging
import os
import unittest

import apache_beam as beam
from apache_beam.metrics import Metrics
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime

load_test_enabled = False
if os.environ.get('LOAD_TEST_ENABLED') == 'true':
  load_test_enabled = True


@unittest.skipIf(not load_test_enabled, 'Enabled only for phrase triggering.')
class ParDoTest(LoadTest):
  def setUp(self):
    super(ParDoTest, self).setUp()
    self.iterations = self.get_option_or_default('iterations')
    self.number_of_counters = self.get_option_or_default('number_of_counters')
    self.number_of_operations = self.get_option_or_default(
        'number_of_counter_operations')

  def testParDo(self):
    class CounterOperation(beam.DoFn):
      def __init__(self, number_of_counters, number_of_operations):
        self.number_of_operations = number_of_operations
        self.counters = []
        for i in range(number_of_counters):
          self.counters.append(Metrics.counter('do-not-publish',
                                               'name-{}'.format(i)))

      def process(self, element):
        for _ in range(self.number_of_operations):
          for counter in self.counters:
            counter.inc()
        yield element

    pc = (self.pipeline
          | 'Read synthetic' >> beam.io.Read(
              synthetic_pipeline.SyntheticSource(
                  self.parseTestPipelineOptions()
              ))
          | 'Measure time: Start' >> beam.ParDo(
              MeasureTime(self.metrics_namespace))
         )

    for i in range(self.iterations):
      pc = (pc
            | 'Step: %d' % i >> beam.ParDo(
                CounterOperation(self.number_of_counters,
                                 self.number_of_operations))
           )

    # pylint: disable=expression-not-assigned
    (pc
     | 'Measure time: End' >> beam.ParDo(MeasureTime(self.metrics_namespace))
    )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
