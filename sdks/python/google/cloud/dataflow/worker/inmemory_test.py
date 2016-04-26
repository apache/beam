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

"""Tests for in-memory input source."""

import logging
import unittest

from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.worker import inmemory


class FakeCoder(object):

  def decode(self, value):
    return value + 10


class InMemoryIO(unittest.TestCase):

  def test_inmemory(self):
    source = inmemory.InMemorySource([1, 2, 3, 4, 5], FakeCoder(), 1, 3)
    with source.reader() as reader:
      self.assertItemsEqual([12, 13], [i for i in reader])

  def test_norange(self):
    source = inmemory.InMemorySource([1, 2, 3, 4, 5], coder=FakeCoder())
    with source.reader() as reader:
      self.assertItemsEqual([11, 12, 13, 14, 15], [i for i in reader])

  def test_in_memory_source_updates_progress_none(self):
    source = inmemory.InMemorySource([], coder=FakeCoder())
    with source.reader() as reader:
      self.assertEqual(None, reader.get_progress())

  def test_in_memory_source_updates_progress_one(self):
    source = inmemory.InMemorySource([1], coder=FakeCoder())
    with source.reader() as reader:
      self.assertEqual(None, reader.get_progress())
      i = 0
      for item in reader:
        self.assertEqual(i, reader.get_progress().position.record_index)
        self.assertEqual(11, item)
        i += 1
      self.assertEqual(1, i)
      self.assertEqual(0, reader.get_progress().position.record_index)

  def test_in_memory_source_updates_progress_many(self):
    source = inmemory.InMemorySource([1, 2, 3, 4, 5], coder=FakeCoder())
    with source.reader() as reader:
      self.assertEqual(None, reader.get_progress())
      i = 0
      for item in reader:
        self.assertEqual(i, reader.get_progress().position.record_index)
        self.assertEqual(11 + i, item)
        i += 1
      self.assertEqual(5, i)
      self.assertEqual(4, reader.get_progress().position.record_index)

  def try_splitting_reader_at(self, reader, split_request, expected_response):
    actual_response = reader.request_dynamic_split(split_request)

    if expected_response is None:
      self.assertIsNone(actual_response)
    else:
      self.assertIsNotNone(actual_response.stop_position)
      self.assertIsInstance(actual_response.stop_position,
                            iobase.ReaderPosition)
      self.assertIsNotNone(actual_response.stop_position.record_index)
      self.assertEqual(expected_response.stop_position.record_index,
                       actual_response.stop_position.record_index)

  def test_in_memory_source_dynamic_split(self):
    source = inmemory.InMemorySource([10, 20, 30, 40, 50, 60],
                                     coder=FakeCoder())

    # Unstarted reader
    with source.reader() as reader:
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(
              iobase.ReaderProgress(
                  position=iobase.ReaderPosition(record_index=2))),
          None)

    # Proposed split position out of range
    with source.reader() as reader:
      reader_iter = iter(reader)
      next(reader_iter)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(
              iobase.ReaderProgress(
                  position=iobase.ReaderPosition(record_index=-1))),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(
              iobase.ReaderProgress(
                  position=iobase.ReaderPosition(record_index=10))),
          None)

    # Already read past proposed split position
    with source.reader() as reader:
      reader_iter = iter(reader)
      next(reader_iter)
      next(reader_iter)
      next(reader_iter)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(
              iobase.ReaderProgress(
                  position=iobase.ReaderPosition(record_index=1))),
          None)

      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(
              iobase.ReaderProgress(
                  position=iobase.ReaderPosition(record_index=2))),
          None)

    # Successful split
    with source.reader() as reader:
      reader_iter = iter(reader)
      next(reader_iter)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(
              iobase.ReaderProgress(
                  position=iobase.ReaderPosition(record_index=4))),
          iobase.DynamicSplitResultWithPosition(
              stop_position=iobase.ReaderPosition(record_index=4)))

      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(
              iobase.ReaderProgress(
                  position=iobase.ReaderPosition(record_index=2))),
          iobase.DynamicSplitResultWithPosition(
              stop_position=iobase.ReaderPosition(record_index=2)))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
