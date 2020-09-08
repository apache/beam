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
* top_count - an arguments passed to the Top combiner.
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

python -m apache_beam.testing.load_tests.combine_test \
    --test-pipeline-options="
    --project=big-query-project
    --region=...
    --publish_to_big_query=true
    --metrics_dataset=python_load_tests
    --metrics_table=combine
    --fanout=1
    --top_count=1000
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
    --metrics_table=combine
    --top_count=1000
    --fanout=1
    --input_options='{
      \"num_records\": 1,
      \"key_size\": 1,
      \"value_size\": 1}'
    --runner=DirectRunner" \
-PloadTest.mainClass=apache_beam.testing.load_tests.combine_test \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load_tests:run
"""

# pytype: skip-file

from __future__ import absolute_import

import logging
import sys

import apache_beam as beam
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import AssignTimestamps
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import StatefulLoadGenerator
from apache_beam.testing.synthetic_pipeline import SyntheticSource
from apache_beam.transforms.combiners import window


class CombineTest(LoadTest):
  def __init__(self):
    super(CombineTest, self).__init__()
    self.fanout = self.get_option_or_default('fanout', 1)
    try:
      self.top_count = int(self.pipeline.get_option('top_count'))
    except (TypeError, ValueError):
      logging.error(
          'You should set \"--top_count\" option to use TOP '
          'combiners')
      sys.exit(1)

  class _GetElement(beam.DoFn):
    def process(self, element):
      yield element

  def test(self):
    if self.get_option_or_default('use_stateful_load_generator', False):
      source = (
          self.pipeline
          | 'LoadGenerator' >> StatefulLoadGenerator(self.input_options)
          | beam.ParDo(AssignTimestamps())
          | beam.WindowInto(window.FixedWindows(20)))
    else:
      source = (
          self.pipeline
          | 'Read synthetic' >> beam.io.Read(
              SyntheticSource(self.parse_synthetic_source_options())))

    pc = (
        source
        | 'Measure time: Start' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))

    for branch in range(self.fanout):
      (  # pylint: disable=expression-not-assigned
          pc
          | 'Combine with Top %i' % branch >> beam.CombineGlobally(
              beam.combiners.TopCombineFn(self.top_count)).without_defaults()
          | 'Consume %i' % branch >> beam.ParDo(self._GetElement())
          | 'Measure time: End %i' % branch >> beam.ParDo(
              MeasureTime(self.metrics_namespace)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  CombineTest().run()
