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
This is GroupByKey load test with Synthetic Source. Besides of the standard
input options there are additional options:
* fanout (optional) - number of GBK operations to run in parallel
* iterations (optional) - number of reiteraations over per-key-grouped
values to perform
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_namespace (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* input_options - options for Synthetic Sources.

Example test run:

python -m apache_beam.testing.load_tests.group_by_key_test \
    --test-pipeline-options="
    --project=big-query-project
    --region=...
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=gbk
    --fanout=1
    --iterations=1
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"

or:

./gradlew -PloadTest.args="
    --publish_to_big_query=true
    --project=...
    --region=...
    --metrics_dataset=python_load_tests
    --metrics_table=gbk
    --fanout=1
    --iterations=1
    --input_options='{
      \"num_records\": 1,
      \"key_size\": 1,
      \"value_size\": 1}'
    --runner=DirectRunner" \
-PloadTest.mainClass=apache_beam.testing.load_tests.group_by_key_test \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load_tests:run
"""

# pytype: skip-file

from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import AssignTimestamps
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureLatency
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource


class GroupByKeyTest(LoadTest):
  def __init__(self):
    super(GroupByKeyTest, self).__init__()
    self.fanout = self.get_option_or_default('fanout', 1)
    self.iterations = self.get_option_or_default('iterations', 1)

  class _UngroupAndReiterate(beam.DoFn):
    def process(self, element, iterations):
      key, value = element
      for i in range(iterations):
        for v in value:
          if i == iterations - 1:
            return key, v

  def test(self):
    pc = (
        self.pipeline
        | beam.io.Read(SyntheticSource(self.parse_synthetic_source_options()))
        | 'Measure time: Start' >> beam.ParDo(
            MeasureTime(self.metrics_namespace))
        | 'Assign timestamps' >> beam.ParDo(AssignTimestamps()))

    for branch in range(self.fanout):
      (  # pylint: disable=expression-not-assigned
          pc
          | 'GroupByKey %i' % branch >> beam.GroupByKey()
          | 'Ungroup %i' % branch >> beam.ParDo(
              self._UngroupAndReiterate(), self.iterations)
          | 'Measure latency' >> beam.ParDo(
              MeasureLatency(self.metrics_namespace))
          | 'Measure time: End %i' % branch >> beam.ParDo(
              MeasureTime(self.metrics_namespace)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  GroupByKeyTest().run()
