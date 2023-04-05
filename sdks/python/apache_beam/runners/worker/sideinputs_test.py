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

"""Tests for side input utilities."""

# pytype: skip-file

import logging
import time
import unittest

import mock

from apache_beam.coders import observable
from apache_beam.runners.worker import sideinputs


def strip_windows(iterator):
  return [wv.value for wv in iterator]


class FakeSource(object):
  def __init__(self, items, notify_observers=False):
    self.items = items
    self._should_notify_observers = notify_observers

  def reader(self):
    return FakeSourceReader(self.items, self._should_notify_observers)


class FakeSourceReader(observable.ObservableMixin):
  def __init__(self, items, notify_observers=False):
    super().__init__()
    self.items = items
    self.entered = False
    self.exited = False
    self._should_notify_observers = notify_observers

  def __iter__(self):
    if self._should_notify_observers:
      self.notify_observers(len(self.items), is_record_size=True)
    for item in self.items:
      yield item

  def __enter__(self):
    self.entered = True
    return self

  def __exit__(self, exception_type, exception_value, traceback):
    self.exited = True

  @property
  def returns_windowed_values(self):
    return False


class PrefetchingSourceIteratorTest(unittest.TestCase):
  def test_single_source_iterator_fn(self):
    sources = [
        FakeSource([0, 1, 2, 3, 4, 5]),
    ]
    iterator_fn = sideinputs.get_iterator_fn_for_sources(
        sources, max_reader_threads=2)
    assert list(strip_windows(iterator_fn())) == list(range(6))

  def test_bytes_read_are_reported(self):
    mock_read_counter = mock.MagicMock()
    source_records = ['a', 'b', 'c', 'd']
    sources = [
        FakeSource(source_records, notify_observers=True),
    ]
    iterator_fn = sideinputs.get_iterator_fn_for_sources(
        sources, max_reader_threads=3, read_counter=mock_read_counter)
    assert list(strip_windows(iterator_fn())) == source_records
    mock_read_counter.add_bytes_read.assert_called_with(4)

  def test_multiple_sources_iterator_fn(self):
    sources = [
        FakeSource([0]),
        FakeSource([1, 2, 3, 4, 5]),
        FakeSource([]),
        FakeSource([6, 7, 8, 9, 10]),
    ]
    iterator_fn = sideinputs.get_iterator_fn_for_sources(
        sources, max_reader_threads=3)
    assert sorted(strip_windows(iterator_fn())) == list(range(11))

  def test_multiple_sources_single_reader_iterator_fn(self):
    sources = [
        FakeSource([0]),
        FakeSource([1, 2, 3, 4, 5]),
        FakeSource([]),
        FakeSource([6, 7, 8, 9, 10]),
    ]
    iterator_fn = sideinputs.get_iterator_fn_for_sources(
        sources, max_reader_threads=1)
    assert list(strip_windows(iterator_fn())) == list(range(11))

  def test_source_iterator_single_source_exception(self):
    class MyException(Exception):
      pass

    def exception_generator():
      yield 0
      raise MyException('I am an exception!')

    sources = [
        FakeSource(exception_generator()),
    ]
    iterator_fn = sideinputs.get_iterator_fn_for_sources(sources)
    seen = set()
    with self.assertRaises(MyException):
      for value in iterator_fn():
        seen.add(value.value)
    self.assertEqual(sorted(seen), [0])

  def test_source_iterator_fn_exception(self):
    class MyException(Exception):
      pass

    def exception_generator():
      yield 0
      time.sleep(0.1)
      raise MyException('I am an exception!')

    def perpetual_generator(value):
      while True:
        yield value
        time.sleep(0.1)

    sources = [
        FakeSource(perpetual_generator(1)),
        FakeSource(perpetual_generator(2)),
        FakeSource(perpetual_generator(3)),
        FakeSource(perpetual_generator(4)),
        FakeSource(exception_generator()),
    ]
    iterator_fn = sideinputs.get_iterator_fn_for_sources(sources)
    seen = set()
    with self.assertRaises(MyException):
      for value in iterator_fn():
        seen.add(value.value)
    self.assertEqual(sorted(seen), list(range(5)))


class EmulatedCollectionsTest(unittest.TestCase):
  def test_emulated_iterable(self):
    def _iterable_fn():
      for i in range(10):
        yield i

    iterable = sideinputs.EmulatedIterable(_iterable_fn)
    # Check that multiple iterations are supported.
    for _ in range(0, 5):
      for i, j in enumerate(iterable):
        self.assertEqual(i, j)

  def test_large_iterable_values(self):
    # Here, we create a large collection that would be too big for memory-
    # constained test environments, but should be under the memory limit if
    # materialized one at a time.
    def _iterable_fn():
      for i in range(10):
        yield ('%d' % i) * (200 * 1024 * 1024)

    iterable = sideinputs.EmulatedIterable(_iterable_fn)
    # Check that multiple iterations are supported.
    for _ in range(0, 3):
      for i, j in enumerate(iterable):
        self.assertEqual(('%d' % i) * (200 * 1024 * 1024), j)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
