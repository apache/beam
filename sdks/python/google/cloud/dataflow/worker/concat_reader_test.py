# Copyright 2016 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Unit tests for ConcatReader."""

import logging
import random
import unittest

from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.worker import concat_reader


class TestSource(iobase.Source):

  def __init__(
      self, elements, index_to_fail_reading=-1, fail_reader_at_close=False):
    self.elements = elements
    self.index_to_fail_reading = index_to_fail_reading
    self.fail_reader_at_close = fail_reader_at_close

  def reader(self):
    return TestReader(self.elements, self.index_to_fail_reading,
                      self.fail_reader_at_close)


class TestReader(iobase.SourceReader):

  def __init__(self, elements, index_to_fail_reading, fail_reader_at_close):
    self.elements = elements
    self.index_to_fail_reading = index_to_fail_reading
    self.fail_reader_at_close = fail_reader_at_close
    self.current_index = 0

  def __exit__(self, exception_type, exception_value, traceback):
    if self.fail_reader_at_close:
      raise ValueError

  def __enter__(self):
    return self

  def __iter__(self):

    for element in self.elements:
      if self.current_index == self.index_to_fail_reading:
        raise ValueError
      yield element
      self.current_index += 1

  def get_progress(self):
    return iobase.ReaderProgress(
        position=iobase.ReaderPosition(record_index=self.current_index))


class ConcatReaderTest(unittest.TestCase):

  def create_data(self, sizes):
    all_data = []
    for size in sizes:
      data = []
      for _ in range(size):
        next_data = random.randint(0, 10000)
        data.append(next_data)
      all_data.append(data)
    return all_data

  def _create_concat_source(self, sub_source_sizes, output_record,
                            index_of_source_to_fail=-1,
                            index_to_fail_reading=-1,
                            fail_reader_at_close=False):
    sub_sources = []
    all_data = self.create_data(sub_source_sizes)
    for data in all_data:
      output_record.extend(data)

    for index, data in enumerate(all_data):
      if index == index_of_source_to_fail:
        sub_sources.append(
            TestSource(data, index_to_fail_reading, fail_reader_at_close))
      else:
        sub_sources.append(TestSource(data, -1, False))
    return concat_reader.ConcatSource(sub_sources)

  def test_create_from_null(self):
    all_data = []
    with concat_reader.ConcatSource(None).reader() as reader:
      for data in reader:
        all_data.append(data)

    self.assertEqual(0, len(all_data))

  def test_read_empty_list(self):
    all_data = []
    with concat_reader.ConcatSource([]).reader() as reader:
      for data in reader:
        all_data.append(data)

    self.assertEqual(0, len(all_data))

  def test_read_one(self):
    expected_output = []
    received_output = []
    with self._create_concat_source([10], expected_output).reader() as reader:
      for data in reader:
        received_output.append(data)

    self.assertEqual(10, len(expected_output))
    self.assertEqual(10, len(received_output))
    self.assertEqual(expected_output.sort(), received_output.sort())

  def test_read_multi_same_size(self):
    expected_output = []
    received_output = []
    source = self._create_concat_source([10, 10, 10], expected_output)
    with source.reader() as reader:
      for data in reader:
        received_output.append(data)

    self.assertEqual(30, len(expected_output))
    self.assertEqual(30, len(received_output))
    self.assertEqual(expected_output.sort(), received_output.sort())

  def test_read_multi_different_sizes(self):
    expected_output = []
    received_output = []
    source = self._create_concat_source([10, 30, 40, 20], expected_output)
    with source.reader() as reader:
      for data in reader:
        received_output.append(data)

    self.assertEqual(100, len(expected_output))
    self.assertEqual(100, len(received_output))
    self.assertEqual(expected_output.sort(), received_output.sort())

  def test_last_reader_empty(self):
    expected_output = []
    received_output = []
    source = self._create_concat_source([10, 30, 40, 0], expected_output)
    with source.reader() as reader:
      for data in reader:
        received_output.append(data)

    self.assertEqual(80, len(expected_output))
    self.assertEqual(80, len(received_output))
    self.assertEqual(expected_output.sort(), received_output.sort())

  def test_empty_reader_before_non_empty_reader(self):
    expected_output = []
    received_output = []
    source = self._create_concat_source([10, 0, 40, 20], expected_output)
    with source.reader() as reader:
      for data in reader:
        received_output.append(data)

    self.assertEqual(70, len(expected_output))
    self.assertEqual(70, len(received_output))
    self.assertEqual(expected_output.sort(), received_output.sort())

  def test_multiple_readers_are_empty(self):
    expected_output = []
    received_output = []
    source = self._create_concat_source([10, 0, 0, 20, 0, 30], expected_output)
    with source.reader() as reader:
      for data in reader:
        received_output.append(data)

    self.assertEqual(60, len(expected_output))
    self.assertEqual(60, len(received_output))
    self.assertEqual(expected_output.sort(), received_output.sort())

  def test_a_reader_fails_at_read(self):
    all_data = []
    received_output = []

    try:
      source = self._create_concat_source(
          [10, 30, 40, 20], all_data, index_of_source_to_fail=2,
          index_to_fail_reading=15, fail_reader_at_close=False)
      with source.reader() as reader:
        for data in reader:
          received_output.append(data)
    except ValueError:
      self.assertEqual(100, len(all_data))
      self.assertEqual(55, len(received_output))
      self.assertEqual(all_data[:55].sort(), received_output.sort())
    else:
      # reading should have produced a ValueError. Failing test. since it
      # didn't.
      raise ValueError

  def test_a_reader_fails_at_close(self):
    all_data = []
    received_output = []

    try:
      source = self._create_concat_source(
          [10, 30, 40, 20], all_data, index_of_source_to_fail=2,
          index_to_fail_reading=-1, fail_reader_at_close=True)
      with source.reader() as reader:
        for data in reader:
          received_output.append(data)
    except ValueError:
      self.assertEqual(100, len(all_data))
      self.assertEqual(80, len(received_output))
      self.assertEqual(all_data[:80].sort(), received_output.sort())
    else:
      # reading should have produced a ValueError. Failing test. since it
      # didn't.
      raise ValueError

  def _test_progress_reporting(self, sizes):
    with self._create_concat_source(sizes, []).reader() as reader:
      reader_iter = iter(reader)
      for reader_index in range(0, len(sizes)):
        for record_index in range(0, sizes[reader_index]):
          reader_iter.next()
          progress = reader.get_progress()
          self.assertIsNotNone(progress)
          self.assertIsNotNone(progress.position)
          self.assertIsNotNone(progress.position.concat_position)
          self.assertTrue(isinstance(progress.position.concat_position,
                                     iobase.ConcatPosition))
          self.assertEqual(reader_index,
                           progress.position.concat_position.index)
          self.assertEqual(
              record_index,
              progress.position.concat_position.position.record_index)

  def test_get_progress_single(self):
    self._test_progress_reporting([10])

  def test_get_progress_same_size(self):
    self._test_progress_reporting([10, 10, 10])

  def test_get_progress_multiple_sizes(self):
    self._test_progress_reporting([20, 10, 30])


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
