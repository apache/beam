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
This is CoGroupByKey load test with Synthetic Source. Besides of the standard
input options there are additional options:
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_namespace (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* input_options - options for Synthetic Sources,
* co_input_options - options for Synthetic Sources,
* iterations - number of reiterations over per-key-grouped values to perform
(default: 1).

Example test run:

python -m apache_beam.testing.load_tests.co_group_by_key_test \
    --test-pipeline-options="
      --project=big-query-project
      --region=...
      --publish_to_big_query=true
      --metrics_dataset=python_load_tests
      --metrics_table=co_gbk
      --iterations=1
      --input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\": 15
        }'
      --co_input_options='{
        \"num_records\": 1000,
        \"key_size\": 5,
        \"value_size\": 15
        }'"

or:

./gradlew -PloadTest.args="
    --publish_to_big_query=true
    --project=...
    --region=...
    --metrics_dataset=python_load_tests
    --metrics_table=co_gbk
    --iterations=1
    --input_options='{
      \"num_records\": 1,
      \"key_size\": 1,
      \"value_size\": 1}'
    --co_input_options='{
      \"num_records\": 1,
      \"key_size\": 1,
      \"value_size\": 1}'
    --runner=DirectRunner" \
-PloadTest.mainClass=apache_beam.testing.load_tests.co_group_by_key_test \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load_tests:run
"""

# pytype: skip-file

from __future__ import absolute_import

import json
import logging

import apache_beam as beam
from apache_beam.testing import synthetic_pipeline
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime


class CoGroupByKeyTest(LoadTest):
  INPUT_TAG = 'pc1'
  CO_INPUT_TAG = 'pc2'

  def __init__(self):
    super(CoGroupByKeyTest, self).__init__()
    self.co_input_options = json.loads(
        self.pipeline.get_option('co_input_options'))
    self.iterations = self.get_option_or_default('iterations', 1)

  class _UngroupAndReiterate(beam.DoFn):
    def __init__(self, input_tag, co_input_tag):
      self.input_tag = input_tag
      self.co_input_tag = co_input_tag

    def process(self, element, iterations):
      values = element[1]
      inputs = values.get(self.input_tag)
      co_inputs = values.get(self.co_input_tag)
      for i in range(iterations):
        for value in inputs:
          if i == iterations - 1:
            yield value
        for value in co_inputs:
          if i == iterations - 1:
            yield value

  def test(self):
    pc1 = (
        self.pipeline
        | 'Read ' + self.INPUT_TAG >> beam.io.Read(
            synthetic_pipeline.SyntheticSource(
                self.parse_synthetic_source_options()))
        | 'Measure time: Start pc1' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))

    pc2 = (
        self.pipeline
        | 'Read ' + self.CO_INPUT_TAG >> beam.io.Read(
            synthetic_pipeline.SyntheticSource(
                self.parse_synthetic_source_options(self.co_input_options)))
        | 'Measure time: Start pc2' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))
    # pylint: disable=expression-not-assigned
    ({
        self.INPUT_TAG: pc1, self.CO_INPUT_TAG: pc2
    }
     | 'CoGroupByKey ' >> beam.CoGroupByKey()
     | 'Consume Joined Collections' >> beam.ParDo(
         self._UngroupAndReiterate(self.INPUT_TAG, self.CO_INPUT_TAG),
         self.iterations)
     | 'Measure time: End' >> beam.ParDo(MeasureTime(self.metrics_namespace)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  CoGroupByKeyTest().run()
