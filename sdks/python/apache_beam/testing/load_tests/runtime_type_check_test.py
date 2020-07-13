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
This is runtime type=checking load test with Synthetic Source.
Besides of the standard input options there are additional options:
* runtime_type_check (optional) - if it's enabled for the pipeline
* nested_typehint (optional) - if the typehint on the DoFn is nested or simple
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

python -m apache_beam.testing.load_tests.runtime_type_check_test \
    --test-pipeline-options="
    --project=apache-beam-testing
    --region=us-centrall
    --publish_to_big_query=true
    --metrics_dataset=saavan_python_load_tests
    --metrics_table=gbk
    --nested_typehint=0
    --fanout=10
    --iterations=10
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"
"""

# pytype: skip-file

from __future__ import absolute_import

import logging
from typing import Iterable, Tuple

import apache_beam as beam
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource


class RunTimeTypeCheckTest(LoadTest):
  def __init__(self):
    super(RunTimeTypeCheckTest, self).__init__()
    self.fanout = self.get_option_or_default('fanout', 1)
    self.iterations = self.get_option_or_default('iterations', 1)
    self.nested_typehint = self.get_option_or_default('nested_typehint', 0)

  class SimpleInput(beam.DoFn):
    def process(self, element: Tuple[bytes, ...], iterations):
      for _ in range(iterations):
        yield element

  class SimpleOutput(beam.DoFn):
    def process(self, element, iterations) -> Iterable[Tuple[bytes, ...]]:
      for _ in range(iterations):
        yield element

  class NestedInput(beam.DoFn):
    def process(
        self, element: Tuple[int, str, bytes, Iterable[int]], iterations):
      for _ in range(iterations):
        yield element

  class NestedOutput(beam.DoFn):
    def process(self, element,
                iterations) -> Iterable[Tuple[int, str, bytes, Iterable[int]]]:
      for _ in range(iterations):
        yield 1, 'a', element, [0]

  def test(self):
    pc = (
        self.pipeline
        | beam.io.Read(SyntheticSource(self.parse_synthetic_source_options()))
        | 'Measure time: Start' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))

    if self.nested_typehint:
      input_transform = self.NestedInput
      output_transform = self.NestedOutput
    else:
      input_transform = self.SimpleInput
      output_transform = self.SimpleOutput

    # Perform the output DoFn before input DoFn so the annotations are valid
    for branch in range(self.fanout):
      (  # pylint: disable=expression-not-assigned
              pc
              | 'Output Transform %i' % branch >>
                beam.ParDo(output_transform(), self.iterations)
              | 'Input Transform %i' % branch >>
                beam.ParDo(input_transform(), self.iterations)
              | 'Measure time: End %i' % branch >>
                beam.ParDo(MeasureTime(self.metrics_namespace)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  RunTimeTypeCheckTest().run()
