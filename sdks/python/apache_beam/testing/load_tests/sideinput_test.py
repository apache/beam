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
Load test for operations involving side inputs.

The purpose of this test is to measure the cost of materialization and
accessing side inputs. The test uses synthetic source which can be
parametrized to generate records with various sizes of keys and values,
impose delays in the pipeline and simulate other performance challenges.

This test can accept the following parameters:
  * side_input_type (str) - Required. Specifies how the side input will be
    materialized in ParDo operation. Choose from (dict, iter, list).
  * window_count (int) - The number of fixed sized windows to subdivide the
    side input into. By default, a global window will be used.
  * access_percentage (int) - Specifies the percentage of elements in the side
    input to be accessed. By default, all elements will be accessed.

Example test run:

python -m apache_beam.testing.load_tests.sideinput_test \
    --test-pipeline-options="
    --side_input_type=iter
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"

or:

./gradlew -PloadTest.args="
    --side_input_type=iter
    --input_options='{
      \"num_records\": 300,
      \"key_size\": 5,
      \"value_size\": 15}'" \
-PloadTest.mainClass=apache_beam.testing.load_tests.sideinput_test \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load_tests:run
"""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division

import logging
from typing import Any
from typing import Dict
from typing import Iterable
from typing import Tuple
from typing import Union

import apache_beam as beam
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSDFAsSource
from apache_beam.transforms import window


class SideInputTest(LoadTest):
  SIDE_INPUT_TYPES = {
      'iter': beam.pvalue.AsIter,
      'list': beam.pvalue.AsList,
      'dict': beam.pvalue.AsDict,
  }
  SDF_INITIAL_ELEMENTS = 1000

  def __init__(self):
    super(SideInputTest, self).__init__()
    self.windows = self.get_option_or_default('window_count', default=1)

    self.access_percentage = self.get_option_or_default(
        'access_percentage', default=100)
    if self.access_percentage < 0 or self.access_percentage > 100:
      raise ValueError(
          'access_percentage: Invalid value. Should be in range '
          'from 0 to 100, got {} instead'.format(self.access_percentage))

    self.elements_per_window = self.input_options['num_records'] // self.windows

    self.side_input_type = self.pipeline.get_option('side_input_type')
    if self.side_input_type is None:
      raise ValueError(
          'side_input_type is required. Please provide one of '
          'these: {}'.format(list(self.SIDE_INPUT_TYPES.keys())))

  def materialize_as(self):
    try:
      return self.SIDE_INPUT_TYPES[self.side_input_type]
    except KeyError:
      raise ValueError(
          'Unknown side input type. Please provide one of '
          'these: {}'.format(list(self.SIDE_INPUT_TYPES.keys())))

  def test(self):
    class SequenceSideInputTestDoFn(beam.DoFn):
      """Iterate over first n side_input elements."""
      def __init__(self, first_n: int):
        self._first_n = first_n

      def process(  # type: ignore[override]
          self, element: Any, side_input: Iterable[Tuple[bytes,
                                                         bytes]]) -> None:
        i = 0
        it = iter(side_input)
        while i < self._first_n:
          i += 1
          try:
            # No-op. We only make sure that the element is accessed.
            next(it)
          except StopIteration:
            break

    class MappingSideInputTestDoFn(beam.DoFn):
      """Iterates over first n keys in the dictionary and checks the value."""
      def __init__(self, first_n: int):
        self._first_n = first_n

      def process(  # type: ignore[override]
          self, element: Any, dict_side_input: Dict[bytes, bytes]) -> None:
        i = 0
        for key in dict_side_input:
          if i == self._first_n:
            break
          # No-op. We only make sure that the element is accessed.
          dict_side_input[key]
          i += 1

    class AssignTimestamps(beam.DoFn):
      """Produces timestamped values. Timestamps are equal to the value of the
      element."""
      def __init__(self):
        # Avoid having to use save_main_session
        self.window = window

      def process(self, element: int) -> Iterable[window.TimestampedValue]:  # type: ignore[override]
        yield self.window.TimestampedValue(element, element)

    class GetSyntheticSDFOptions(beam.DoFn):
      def __init__(
          self, elements_per_record: int, key_size: int, value_size: int):
        self.elements_per_record = elements_per_record
        self.key_size = key_size
        self.value_size = value_size

      def process(self, element: Any) -> Iterable[Dict[str, Union[int, str]]]:  # type: ignore[override]
        yield {
            'num_records': self.elements_per_record,
            'key_size': self.key_size,
            'value_size': self.value_size,
            'initial_splitting_num_bundles': 0,
            'initial_splitting_desired_bundle_size': 0,
            'sleep_per_input_record_sec': 0,
            'initial_splitting': 'const'
        }

    main_input = self.pipeline | 'Create' >> beam.Create(range(self.windows))

    initial_elements = self.SDF_INITIAL_ELEMENTS
    if self.windows > 1:
      main_input = (
          main_input
          | 'Assign timestamps' >> beam.ParDo(AssignTimestamps())
          | 'Apply windows' >> beam.WindowInto(window.FixedWindows(1)))
      side_input = main_input
      initial_elements = self.windows
    else:
      side_input = self.pipeline | 'Side input: create' >> beam.Create(
          range(initial_elements))

    side_input = (
        side_input
        | 'Get synthetic SDF options' >> beam.ParDo(
            GetSyntheticSDFOptions(
                self.input_options['num_records'] // initial_elements,
                self.input_options['key_size'],
                self.input_options['value_size']))
        | 'Generate input' >> beam.ParDo(SyntheticSDFAsSource()))
    main_input |= 'Collect start time metrics' >> beam.ParDo(
        MeasureTime(self.metrics_namespace))

    side_input_type = self.materialize_as()
    elements_to_access = self.elements_per_window * \
                         self.access_percentage // 100
    logging.info(
        '%s out of %s total elements in each window will be accessed.',
        elements_to_access,
        self.elements_per_window)
    if side_input_type is beam.pvalue.AsDict:
      dofn = MappingSideInputTestDoFn(elements_to_access)
    else:
      dofn = SequenceSideInputTestDoFn(elements_to_access)

    _ = (
        main_input
        | beam.ParDo(dofn, side_input_type(side_input))
        | 'Collect end time metrics' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  SideInputTest().run()
