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
      self.assertEqual(1, reader.get_progress().percent_complete)

  def test_in_memory_source_updates_progress_one(self):
    source = inmemory.InMemorySource([1], coder=FakeCoder())
    with source.reader() as reader:
      self.assertEqual(0, reader.get_progress().percent_complete)
      i = 0
      for item in reader:
        i += 1
        self.assertEqual(11, item)
        self.assertEqual(1, reader.get_progress().percent_complete)
      self.assertEqual(1, i)
      self.assertEqual(1, reader.get_progress().percent_complete)

  def test_in_memory_source_updates_progress_many(self):
    source = inmemory.InMemorySource([1, 2, 3, 4, 5], coder=FakeCoder())
    with source.reader() as reader:
      self.assertEqual(0, reader.get_progress().percent_complete)
      i = 0
      for item in reader:
        i += 1
        self.assertEqual(i + 10, item)
        self.assertEqual(float(i) / 5, reader.get_progress().percent_complete)
      self.assertEqual(5, i)
      self.assertEqual(1, reader.get_progress().percent_complete)

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
