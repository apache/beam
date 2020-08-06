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
This is runtime type-checking load test base class.

It has two children:
- runtime_type_check_on_test_py3
- runtime_type_check_off_test_py3

Besides of the standard input options there are additional options:
* runtime_type_check (optional) - if it's enabled for the pipeline
* nested_typehint (optional) - if the typehint on the DoFn is nested or simple
* num_records (optional) - number of elements to process
* fanout (optional) - number of operations to run in parallel
* project (optional) - the gcp project in case of saving
metrics in Big Query (in case of Dataflow Runner
it is required to specify project of runner),
* publish_to_big_query - if metrics should be published in big query,
* metrics_namespace (optional) - name of BigQuery dataset where metrics
will be stored,
* metrics_table (optional) - name of BigQuery table where metrics
will be stored,
* input_options - these are not used, but must be specified to run the test
"""

# pytype: skip-file

from __future__ import absolute_import

from typing import Iterable
from typing import Tuple
from typing import Union

import apache_beam as beam
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime


class BaseRunTimeTypeCheckTest(LoadTest):
  def __init__(self):
    runtime_type_check = getattr(self, 'runtime_type_check', False)
    super(BaseRunTimeTypeCheckTest,
          self).__init__(runtime_type_check=runtime_type_check)
    self.fanout = self.get_option_or_default('fanout', 1)
    self.num_records = self.get_option_or_default('num_records', 1000)
    self.nested_typehint = self.get_option_or_default('nested_typehint', 0)

  @beam.typehints.with_input_types(Tuple[int, ...])
  class SimpleInput(beam.DoFn):
    def process(self, element, *args, **kwargs):
      yield element

  @beam.typehints.with_output_types(Tuple[int, ...])
  class SimpleOutput(beam.DoFn):
    def process(self, element, *args, **kwargs):
      yield element

  @beam.typehints.with_input_types(
      Tuple[int, str, Tuple[float], Iterable[int], Union[str, int]])
  class NestedInput(beam.DoFn):
    def process(self, element, *args, **kwargs):
      yield element

  @beam.typehints.with_output_types(
      Tuple[int, str, Tuple[float], Iterable[int], Union[str, int]])
  class NestedOutput(beam.DoFn):
    def process(self, element, *args, **kwargs):
      yield element

  def test(self):
    if self.nested_typehint:
      input_transform = self.NestedInput
      output_transform = self.NestedOutput
      records = [(1, '2', (3.0, ), [4], '5') for _ in range(self.num_records)]
    else:
      input_transform = self.SimpleInput
      output_transform = self.SimpleOutput
      records = [(1, 2, 3, 4, 5) for _ in range(self.num_records)]

    pc = (
        self.pipeline
        | beam.Create(records)
        | 'Measure time: Start' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))

    # Perform the output DoFn before input DoFn so the annotations are valid
    for branch in range(self.fanout):
      (  # pylint: disable=expression-not-assigned
              pc
              | 'Output Transform %i' % branch >>
                beam.ParDo(output_transform())
              | 'Input Transform %i' % branch >>
                beam.ParDo(input_transform())
              | 'Measure time: End %i' % branch >>
                beam.ParDo(MeasureTime(self.metrics_namespace)))
