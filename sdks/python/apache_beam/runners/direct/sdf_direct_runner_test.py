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

"""Unit tests for SDF implementation for DirectRunner."""

from __future__ import absolute_import
from __future__ import division

import logging
import os
import unittest
from builtins import range

import apache_beam as beam
from apache_beam import Create
from apache_beam import DoFn
from apache_beam.io import filebasedsource_test
from apache_beam.io.restriction_trackers import OffsetRestrictionTracker
from apache_beam.pvalue import AsList
from apache_beam.pvalue import AsSingleton
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.core import RestrictionProvider
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.window import SlidingWindows
from apache_beam.transforms.window import TimestampedValue


class ReadFilesProvider(RestrictionProvider):

  def initial_restriction(self, element):
    size = os.path.getsize(element)
    return (0, size)

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(*restriction)


class ReadFiles(DoFn):

  def __init__(self, resume_count=None):
    self._resume_count = resume_count

  def process(
      self, element, restriction_tracker=ReadFilesProvider(), *args, **kwargs):
    file_name = element
    assert isinstance(restriction_tracker, OffsetRestrictionTracker)

    with open(file_name, 'rb') as file:
      pos = restriction_tracker.start_position()
      if restriction_tracker.start_position() > 0:
        file.seek(restriction_tracker.start_position() - 1)
        line = file.readline()
        pos = pos - 1 + len(line)

      output_count = 0
      while restriction_tracker.try_claim(pos):
        line = file.readline()
        len_line = len(line)
        line = line.strip()
        if not line:
          break

        if line is None:
          break
        yield line
        output_count += 1

        if self._resume_count and output_count == self._resume_count:
          restriction_tracker.defer_remainder()
          break

        pos += len_line


class ExpandStringsProvider(RestrictionProvider):

  def initial_restriction(self, element):
    return (0, len(element[0]))

  def create_tracker(self, restriction):
    return OffsetRestrictionTracker(restriction[0], restriction[1])

  def split(self, element, restriction):
    return [restriction,]


class ExpandStrings(DoFn):

  def __init__(self, record_window=False):
    self._record_window = record_window

  def process(
      self, element, side1, side2, side3, window=beam.DoFn.WindowParam,
      restriction_tracker=ExpandStringsProvider(),
      *args, **kwargs):
    side = []
    side.extend(side1)
    side.extend(side2)
    side.extend(side3)
    assert isinstance(restriction_tracker, OffsetRestrictionTracker)
    side = list(side)
    for i in range(restriction_tracker.start_position(),
                   restriction_tracker.stop_position()):
      if restriction_tracker.try_claim(i):
        if not side:
          yield (
              element[0] + ':' + str(element[1]) + ':' + str(int(window.start))
              if self._record_window else element)
        else:
          for val in side:
            ret = (
                element[0] + ':' + str(element[1]) + ':' +
                str(int(window.start)) if self._record_window else element)
            yield ret + ':' + val
      else:
        break


class SDFDirectRunnerTest(unittest.TestCase):

  def setUp(self):
    super(SDFDirectRunnerTest, self).setUp()
    # Importing following for DirectRunner SDF implemenation for testing.
    from apache_beam.runners.direct import transform_evaluator
    self._default_max_num_outputs = (
        transform_evaluator._ProcessElementsEvaluator.DEFAULT_MAX_NUM_OUTPUTS)

  def run_sdf_read_pipeline(
      self, num_files, num_records_per_file, resume_count=None):
    expected_data = []
    file_names = []
    for _ in range(num_files):
      new_file_name, new_expected_data = filebasedsource_test.write_data(
          num_records_per_file)
      assert len(new_expected_data) == num_records_per_file
      file_names.append(new_file_name)
      expected_data.extend(new_expected_data)

    assert len(expected_data) > 0

    with TestPipeline() as p:
      pc1 = (p
             | 'Create1' >> beam.Create(file_names)
             | 'SDF' >> beam.ParDo(ReadFiles(resume_count)))

      assert_that(pc1, equal_to(expected_data))

      # TODO(chamikara: verify the number of times process method was invoked
      # using a side output once SDFs supports producing side outputs.

  def test_sdf_no_checkpoint_single_element(self):
    self.run_sdf_read_pipeline(
        1,
        self._default_max_num_outputs - 1)

  def test_sdf_one_checkpoint_single_element(self):
    self.run_sdf_read_pipeline(
        1,
        int(self._default_max_num_outputs + 1))

  def test_sdf_multiple_checkpoints_single_element(self):
    self.run_sdf_read_pipeline(
        1,
        int(self._default_max_num_outputs * 3))

  def test_sdf_no_checkpoint_multiple_element(self):
    self.run_sdf_read_pipeline(
        5,
        int(self._default_max_num_outputs - 1))

  def test_sdf_one_checkpoint_multiple_element(self):
    self.run_sdf_read_pipeline(
        5,
        int(self._default_max_num_outputs + 1))

  def test_sdf_multiple_checkpoints_multiple_element(self):
    self.run_sdf_read_pipeline(
        5,
        int(self._default_max_num_outputs * 3))

  def test_sdf_with_resume_single_element(self):
    resume_count = self._default_max_num_outputs // 10
    # Makes sure that resume_count is not trivial.
    assert resume_count > 0

    self.run_sdf_read_pipeline(
        1,
        self._default_max_num_outputs - 1,
        resume_count)

  def test_sdf_with_resume_multiple_elements(self):
    resume_count = self._default_max_num_outputs // 10
    assert resume_count > 0

    self.run_sdf_read_pipeline(
        5,
        int(self._default_max_num_outputs - 1),
        resume_count)

  def test_sdf_with_windowed_timestamped_input(self):
    with TestPipeline() as p:
      result = (p
                | beam.Create([1, 3, 5, 10])
                | beam.FlatMap(lambda t: [TimestampedValue(('A', t), t),
                                          TimestampedValue(('B', t), t)])
                | beam.WindowInto(SlidingWindows(10, 5),
                                  accumulation_mode=AccumulationMode.DISCARDING)
                | beam.ParDo(ExpandStrings(record_window=True), [], [], []))

      expected_result = [
          'A:1:-5', 'A:1:0', 'A:3:-5', 'A:3:0', 'A:5:0', 'A:5:5', 'A:10:5',
          'A:10:10', 'B:1:-5', 'B:1:0', 'B:3:-5', 'B:3:0', 'B:5:0', 'B:5:5',
          'B:10:5', 'B:10:10',]
      assert_that(result, equal_to(expected_result))

  def test_sdf_with_side_inputs(self):
    with TestPipeline() as p:
      side1 = p | 'Create1' >> Create(['1', '2'])
      side2 = p | 'Create2' >> Create(['3', '4'])
      side3 = p | 'Create3' >> Create(['5'])
      result = (p
                | 'create_main' >> beam.Create(['a', 'b', 'c'])
                | beam.ParDo(ExpandStrings(), AsList(side1), AsList(side2),
                             AsSingleton(side3)))

      expected_result = []
      for c in ['a', 'b', 'c']:
        for i in range(5):
          expected_result.append(c + ':' + str(i+1))
      assert_that(result, equal_to(expected_result))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
