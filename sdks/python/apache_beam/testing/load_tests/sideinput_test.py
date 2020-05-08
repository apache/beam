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
    side input into. By default, no windows will be used.
  * side_input_size (int) - The size of the side input. Must be equal to or
    lower than the size of the main input. If lower, the side input will be
    created by applying a :class:`beam.combiners.Sample
    <apache_beam.transforms.combiners.Sample>` transform.
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

import apache_beam as beam
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.testing.load_tests.load_test_metrics_utils import MeasureTime
from apache_beam.testing.synthetic_pipeline import SyntheticSource


class SideInputTest(LoadTest):
  SIDE_INPUT_TYPES = {
      'iter': beam.pvalue.AsIter,
      'list': beam.pvalue.AsList,
      'dict': beam.pvalue.AsDict,
  }

  def __init__(self):
    super(SideInputTest, self).__init__()
    self.windows = self.get_option_or_default('window_count', default=0)
    self.access_percentage = self.get_option_or_default(
        'access_percentage', default=100)
    if self.access_percentage < 0 or self.access_percentage > 100:
      raise ValueError(
          'access_percentage: Invalid value. Should be in range '
          'from 0 to 100, got {} instead'.format(self.access_percentage))

    self.side_input_size = self.get_option_or_default(
        'side_input_size', default=0)
    if self.side_input_size == 0:
      self.side_input_size = self.input_options.get('num_records')

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
      def __init__(self, first_n):
        self._first_n = first_n

      def process(self, unused_element, side_input):
        i = 0
        it = iter(side_input)
        while i < self._first_n:
          i += 1
          try:
            # No-op. We only make sure that the element is accessed.
            next(it)
          except StopIteration:
            return

    class MappingSideInputTestDoFn(beam.DoFn):
      """Take a sequence of keys as an additional side input and for each
      key in the sequence checks the value for key in the dictionary."""
      def process(self, unused_element, dict_side_input, keys_to_check):
        for key in keys_to_check:
          # No-op. We only make sure that the element is accessed.
          dict_side_input[key]

    class GetRandomKeys(beam.DoFn):
      def __init__(self, n):
        self._n = n

      def process(self, unused_element, dict_side_input):
        import random
        n = min(self._n, len(dict_side_input))
        return random.sample(dict_side_input.keys(), n)

    class AddEventTimestamps(beam.DoFn):
      """Assign timestamp to each element of PCollection."""
      def setup(self):
        self._timestamp = 0

      def process(self, element):
        from apache_beam.transforms.combiners import window
        yield window.TimestampedValue(element, self._timestamp)
        self._timestamp += 1

    input_pc = (
        self.pipeline
        | 'Read synthetic' >> beam.io.Read(
            SyntheticSource(self.parse_synthetic_source_options()))
        | 'Collect start time metrics' >> beam.ParDo(
            MeasureTime(self.metrics_namespace)))

    if self.side_input_size != self.input_options.get('num_records'):
      side_input = (
          input_pc
          | 'Sample {} elements'.format(self.side_input_size) >>
          beam.combiners.Sample.FixedSizeGlobally(self.side_input_size)
          | 'Flatten a sequence' >> beam.FlatMap(lambda x: x))
    else:
      side_input = input_pc

    if self.windows > 0:
      window_size = self.side_input_size / self.windows
      logging.info('Fixed windows of %s seconds will be applied', window_size)
      side_input = (
          side_input
          | 'Add event timestamps' >> beam.ParDo(AddEventTimestamps())
          | 'Apply windows' >> beam.WindowInto(
              beam.combiners.window.FixedWindows(window_size)))

    side_input_type = self.materialize_as()
    elements_to_access = self.side_input_size * self.access_percentage // 100
    logging.info(
        '%s out of %s total elements in the side input will be '
        'accessed.',
        elements_to_access,
        self.side_input_size)
    if side_input_type is beam.pvalue.AsDict:
      random_keys = (
          self.pipeline
          | beam.Impulse()
          | 'Get random keys' >> beam.ParDo(
              GetRandomKeys(elements_to_access), beam.pvalue.AsDict(side_input))
      )
      pc = input_pc | beam.ParDo(
          MappingSideInputTestDoFn(),
          side_input_type(side_input),
          beam.pvalue.AsList(random_keys))
    else:
      pc = input_pc | beam.ParDo(
          SequenceSideInputTestDoFn(elements_to_access),
          side_input_type(side_input))

    _ = pc | 'Collect end time metrics' >> beam.ParDo(
        MeasureTime(self.metrics_namespace))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  SideInputTest().run()
