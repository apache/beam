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

"""Unit tests for shuffle sources and sinks."""

import base64
import cStringIO as StringIO
import logging
import unittest

from google.cloud.dataflow.io import iobase
from google.cloud.dataflow.worker.shuffle import GroupedShuffleSource
from google.cloud.dataflow.worker.shuffle import ShuffleEntry
from google.cloud.dataflow.worker.shuffle import ShuffleSink
from google.cloud.dataflow.worker.shuffle import UngroupedShuffleSource


class Base64Coder(object):
  """Simple base64 coder used throughout the tests."""

  def decode(self, o):
    return base64.b64decode(o)

  def encode(self, o):
    return base64.b64encode(o)


class FakeShuffleReader(object):
  """A fake shuffle reader returning a known set of shuffle chunks.

  This is not an implementation of a shuffler. The class simply returns the
  shuffle chunks as specified and is injected as a dependency when a shuffle
  reader gets created.

  There are several properties of the sequence of shuffle entries in a chunk
  that are expected to be true. Tests for the real shuffler make sure that this
  is the case. For instance:
  - keys appear in lexicographic order
  """

  def __init__(self, chunk_descriptors):
    """Initializes the fake shuffle from a list of lists of (k,v) pairs."""
    self.all_vals = []
    self.chunk_starts = []
    last_index = 0
    for chunk in chunk_descriptors:
      self.all_vals.extend(chunk)
      last_index += len(chunk)
      self.chunk_starts.append(last_index)

  def _make_chunk(self, descriptor, start_index):
    """Returns an encoded shuffle chunk from a list of (k,v) pairs."""
    stream = StringIO.StringIO()
    coder = Base64Coder()
    position = start_index
    for key, value in descriptor:
      ShuffleEntry(
          coder.encode(key),
          coder.encode('2nd-%s' % key),
          coder.encode(value),
          position=str(position)).to_bytes(stream)
      position += 1
    value = stream.getvalue()
    stream.close()
    return value

  def _get_first_chunk_key_values(self, start, end):
    result = [self.all_vals[start]]
    index = start + 1
    while index < end:
      if index in self.chunk_starts:
        return result, index
      result.append(self.all_vals[index])
      index += 1
    return result, end

  def Read(self, first, last):  # pylint: disable=invalid-name
    first = 0 if not first else int(first)
    last = len(self.all_vals) if not last else int(last)
    key_values, next_position = self._get_first_chunk_key_values(first, last)
    return (
        self._make_chunk(key_values, next_position - len(key_values)),
        str(next_position) if next_position < last else '')


class FakeShuffleWriter(object):
  """A fake shuffle writter recording what entries were written."""

  def __init__(self):
    # The list of (key, 2nd-key, value) tuples written. The attribute will
    # get its real value only when close() is called.
    self.values = []
    self._entries = []

  def Write(self, entries):  # pylint: disable=invalid-name
    stream = StringIO.StringIO(entries)
    # TODO(silviuc): Find a better way to detect EOF for a string stream.
    while stream.tell() < len(stream.getvalue()):
      self._entries.append(
          ShuffleEntry.from_stream(stream, with_position=False))

  def Close(self):  # pylint: disable=invalid-name
    coder = Base64Coder()
    for entry in self._entries:
      self.values.append((
          coder.decode(entry.key),
          coder.decode(entry.secondary_key),
          coder.decode(entry.value)))


class TestShuffleEntry(unittest.TestCase):

  def test_basics(self):
    entry = ShuffleEntry('abc', 'xyz123', '0123456789', position='zyx')
    stream = StringIO.StringIO()
    entry.to_bytes(stream)
    self.assertEqual(
        ShuffleEntry.from_stream(StringIO.StringIO(stream.getvalue())),
        entry)

  def test_size(self):
    """Test that the computed size property returns expected values."""
    params = ['abc', 'xyz123', '0123456789', 'zyx']
    entry = ShuffleEntry(params[0], params[1], params[2], position=params[3])
    expected_size = 4 * len(params) + sum(len(x) for x in params)
    stream = StringIO.StringIO()
    entry.to_bytes(stream)
    self.assertEqual(entry.size, expected_size)
    self.assertEqual(
        ShuffleEntry.from_stream(StringIO.StringIO(stream.getvalue())).size,
        expected_size)

  def test_big_endian(self):
    """Tests that lengths are written as big endian ints."""
    entry = ShuffleEntry('abc', 'xyz123', '0123456789', position='zyx')
    stream = StringIO.StringIO()
    entry.to_bytes(stream)
    entry_bytes = stream.getvalue()
    # The length of 'abc' is represented in big endian form.
    self.assertEqual(entry_bytes[0], '\x00')
    self.assertEqual(entry_bytes[1], '\x00')
    self.assertEqual(entry_bytes[2], '\x00')
    self.assertEqual(entry_bytes[3], '\x03')


TEST_CHUNK1 = [('a', '1'), ('b', '0'), ('b', '1'), ('c', '0')]
TEST_CHUNK2 = [('c', '1'), ('c', '2'), ('c', '3'), ('c', '4')]


class TestGroupedShuffleSource(unittest.TestCase):

  def test_basics(self):
    result = []
    source = GroupedShuffleSource(
        config_bytes='not used', coder=Base64Coder())

    chunks = [TEST_CHUNK1, TEST_CHUNK2]
    with source.reader(test_reader=FakeShuffleReader(chunks)) as reader:
      for key, key_values in reader:
        for value in key_values:
          result.append((key, value))
    self.assertEqual(TEST_CHUNK1 + TEST_CHUNK2, result)

  def test_progress_reporting(self):
    result = []
    progress_record = []
    source = GroupedShuffleSource(
        config_bytes='not used', coder=Base64Coder())

    chunks = [TEST_CHUNK1, TEST_CHUNK2]

    expected_progress_record = []
    expected_progress_record += 1 * [base64.urlsafe_b64encode('0')]
    expected_progress_record += 2 * [base64.urlsafe_b64encode('1')]
    expected_progress_record += 5 * [base64.urlsafe_b64encode('3')]

    with source.reader(test_reader=FakeShuffleReader(chunks)) as reader:
      for key, key_values in reader:
        for value in key_values:
          result.append((key, value))
          progress_record.append(
              reader.get_progress().position.shuffle_position)
    self.assertEqual(TEST_CHUNK1 + TEST_CHUNK2, result)
    self.assertEqual(expected_progress_record, progress_record)

  def try_splitting_reader_at(self, reader, split_request, expected_response):
    actual_response = reader.request_dynamic_split(split_request)

    if expected_response is None:
      self.assertIsNone(actual_response)
    else:
      self.assertIsNotNone(actual_response.stop_position)
      self.assertIsInstance(actual_response.stop_position,
                            iobase.ReaderPosition)
      self.assertIsNotNone(actual_response.stop_position.shuffle_position)
      self.assertEqual(expected_response.stop_position.shuffle_position,
                       actual_response.stop_position.shuffle_position)

  def test_dynamic_splitting(self):
    source = GroupedShuffleSource(
        config_bytes='not used', coder=Base64Coder())

    chunks = [TEST_CHUNK1, TEST_CHUNK2]

    with source.reader(test_reader=FakeShuffleReader(chunks)) as reader:
      # Cannot split an unstarted reader
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('1')))),
          None)

      reader_iter = iter(reader)
      next(reader_iter)
      next(reader_iter)
      # Cannot split since the provided split position is smaller than or equal
      # to the current position '1'.
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('0')))),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('1')))),
          None)

      # Successful split.
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('3')))),
          iobase.DynamicSplitResultWithPosition(iobase.ReaderPosition(
              shuffle_position=base64.urlsafe_b64encode('3'))))

  def test_dynamic_splitting_with_range(self):
    source = GroupedShuffleSource(
        config_bytes='not used',
        coder=Base64Coder(),
        start_position=base64.urlsafe_b64encode('0'),
        end_position=base64.urlsafe_b64encode('3'))

    chunks = [TEST_CHUNK1, TEST_CHUNK2]

    with source.reader(test_reader=FakeShuffleReader(chunks)) as reader:
      reader_iter = iter(reader)
      next(reader_iter)

      # Cannot split if split request is out of range
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('0')))),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('3')))),
          None)
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('4')))),
          None)

      # Successful split.
      self.try_splitting_reader_at(
          reader,
          iobase.DynamicSplitRequest(iobase.ReaderProgress(
              position=iobase.ReaderPosition(
                  shuffle_position=base64.urlsafe_b64encode('2')))),
          iobase.DynamicSplitResultWithPosition(iobase.ReaderPosition(
              shuffle_position=base64.urlsafe_b64encode('2'))))

  def test_reiteration(self):
    """Tests that key values iterators can be iterated repeatedly."""
    source = GroupedShuffleSource(
        config_bytes='not used', coder=Base64Coder())

    chunks = [TEST_CHUNK1, TEST_CHUNK2]
    saved_iterators = {}
    with source.reader(test_reader=FakeShuffleReader(chunks)) as reader:
      for key, key_values in reader:
        saved_iterators[key] = key_values
    # Iterate once.
    self.assertEqual(list(saved_iterators['a']), ['1'])
    self.assertEqual(list(saved_iterators['b']), ['0', '1'])
    self.assertEqual(list(saved_iterators['c']), ['0', '1', '2', '3', '4'])

    # Iterate twice.
    self.assertEqual(list(saved_iterators['a']), ['1'])
    self.assertEqual(list(saved_iterators['b']), ['0', '1'])
    self.assertEqual(list(saved_iterators['c']), ['0', '1', '2', '3', '4'])

  def test_iterator_drained(self):
    result = []
    source = GroupedShuffleSource(
        config_bytes='not used', coder=Base64Coder())

    chunks = [TEST_CHUNK1, TEST_CHUNK2]
    with source.reader(test_reader=FakeShuffleReader(chunks)) as reader:
      for key, key_values in reader:
        for value in key_values:
          result.append((key, value))
          # We stop after getting the first shuffle entry for eack key.
          # We need to check that the iterator is properly drained and the
          # rest of the entries for the same key are discarded.
          break
    # We expect only the first entry for each key to show up.
    self.assertEqual([('a', '1'), ('b', '0'), ('c', '0')], result)


class TestUngroupedShuffleSource(unittest.TestCase):

  def test_basics(self):
    result = []
    source = UngroupedShuffleSource(
        config_bytes='not used', coder=Base64Coder())

    chunks = [TEST_CHUNK1, TEST_CHUNK2]
    with source.reader(test_reader=FakeShuffleReader(chunks)) as reader:
      for v in reader:
        result.append(v)
    # We get only the values from the (k, 2nd-k, v) tuples.
    self.assertEqual([e[1] for e in TEST_CHUNK1 + TEST_CHUNK2], result)


class TestShuffleSink(unittest.TestCase):

  def test_basics(self):
    source = ShuffleSink(config_bytes='not used', coder=Base64Coder())
    entries = [('a', '2nd-a', '1'), ('b', '2nd-b', '0'), ('b', '2nd-b', '1')]
    fake_writer = FakeShuffleWriter()
    with source.writer(test_writer=fake_writer) as writer:
      for entry in entries:
        writer.Write(*entry)
    self.assertEqual(entries, fake_writer.values)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()

