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

Example test run:

python -m apache_beam.testing.load_tests.sideinput_test \
    --test-pipeline-options="
    --project=big-query-project
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=side_input
    --number_of_counter_operations=1000
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"

or:

./gradlew -PloadTest.args="
    --publish_to_big_query=true
    --project=...
    --metrics_dataset=python_load_tests
    --metrics_table=side_input
    --input_options='{
      \"num_records\": 1,
      \"key_size\": 1,
      \"value_size\": 1}'
    --runner=DirectRunner" \
-PloadTest.mainClass=apache_beam.testing.load_tests.sideinput_test \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load_tests:run
"""

# pytype: skip-file

from __future__ import absolute_import

import logging

import apache_beam as beam
from apache_beam.pvalue import AsIter
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource


class SideInputTest(LoadTest):
  def __init__(self):
    super(SideInputTest, self).__init__()
    self.iterations = self.get_option_or_default(
        'number_of_counter_operations', 1)

  def test(self):
    def join_fn(element, side_input, iterations):
      result = []
      for i in range(iterations):
        for key, value in side_input:
          if i == iterations - 1:
            result.append({key: element[1] + value})
      yield result

    main_input = (
        self.pipeline
        | "Read pcoll 1" >> beam.io.Read(
            SyntheticSource(self.parse_synthetic_source_options()))
        | 'Measure time: Start pcoll 1' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))

    side_input = (
        self.pipeline
        | "Read pcoll 2" >> beam.io.Read(
            SyntheticSource(self.parse_synthetic_source_options()))
        | 'Measure time: Start pcoll 2' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))
    # pylint: disable=expression-not-assigned
    (
        main_input
        | "Merge" >> beam.ParDo(join_fn, AsIter(side_input), self.iterations)
        | 'Measure time' >> beam.ParDo(MeasureTime(self.metrics_namespace)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  SideInputTest().run()
