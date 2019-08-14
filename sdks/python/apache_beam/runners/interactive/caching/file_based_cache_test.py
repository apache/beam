# -*- coding: utf-8 -*-
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
from __future__ import absolute_import

import gc
import itertools
import logging
import tempfile
import time
import unittest
import uuid

import mock
import numpy as np

from apache_beam import coders
from apache_beam.io.filesystems import FileSystems
from apache_beam.pipeline import Pipeline
from apache_beam.pvalue import PCollection
from apache_beam.runners.interactive.caching import file_based_cache
from apache_beam.testing import datatype_inference

GENERIC_TEST_DATA = [
    [],
    [None],
    [None, None, None],
    ["ABC", "DeF"],
    [u"ABC", u"±♠Ωℑ"],
    [b"abc123", b"aAaAa"],
    [1.5],
    [100, -123.456, 78.9],
    ["ABC", 1.2, 100, 0, -10, None, b"abc123"],
    [()],
    [("a", "b", "c")],
    [("a", 1, 1.2), ("b", 2, 5.5)],
    [("a", 1, 1.2), (2.5, "c", None)],
    [{}],
    [{"col1": "a", "col2": 1, "col3": 1.5}],
    [{"col1": "a", "col2": 1, "col3": 1.5},
     {"col1": "b", "col2": 2, "col3": 4.5}],
    [{"col1": "a", "col2": 1, "col3": u"±♠Ω"}, {4: 1, 5: 3.4, (6, 7): "a"}],
    [("a", "b", "c"), ["d", 1], {"col1": 1, 202: 1.234}, None, "abc", b"def",
     100, (1, 2, 3, "b")],
    [np.zeros((3, 6)), np.ones((10, 22))],
]

GENERIC_ELEMENT_TYPES = {
    datatype_inference.infer_element_type(data) for data in GENERIC_TEST_DATA
}


def read_directly(cache, limit=None, timeout=None):
  return list(itertools.islice(cache.read(), limit))


def write_directly(cache, data_in, timeout=None):
  """Write elements to cache using the cache API."""
  cache.write(data_in)


def write_through_pipeline(cache, data_in, timeout=None):
  """Write elements to cache using a Beam pipeline."""
  import apache_beam as beam

  with beam.Pipeline() as p:
    _ = (p | "Create" >> beam.Create(data_in) | "Write" >> cache.writer())


class FileBasedCacheTest(unittest.TestCase):

  def cache_class(self, location, *args, **kwargs):

    class MockedFileBasedCache(file_based_cache.FileBasedCache):

      _reader_class = mock.MagicMock()
      _writer_class = mock.MagicMock()
      _reader_passthrough_arguments = {}
      requires_coder = False

    return MockedFileBasedCache(location, *args, **kwargs)

  def setUp(self):
    self.temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self.temp_dir, self.cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self.temp_dir])

  def create_dummy_file(self, location):
    """Create a dummy file with `location` as the filepath prefix."""
    filename = location + "-" + uuid.uuid4().hex
    while FileSystems.exists(filename):
      filename = location + "-" + uuid.uuid4().hex
    with open(filename, "wb") as fout:
      fout.write(b"dummy data")
    return filename

  def test_init(self):
    # cache needs to be kept so that resources are not gc-ed immediately.
    cache = self.cache_class(self.location)  # pylint: disable=unused-variable

    with self.assertRaises(ValueError):
      _ = self.cache_class(self.location, mode=None)

    with self.assertRaises(ValueError):
      _ = self.cache_class(self.location, mode="be happy")

  def test_overwrite_cache_0(self):
    # cache needs to be kept so that resources are not gc-ed immediately.
    cache = self.cache_class(self.location)  # pylint: disable=unused-variable
    # Refuse to create a cache with the same data storage location
    with self.assertRaises(IOError):
      _ = self.cache_class(self.location)

  def test_overwrite_cache_1(self):
    # cache needs to be kept so that resources are not gc-ed immediately.
    cache = self.cache_class(self.location)  # pylint: disable=unused-variable
    # Refuse to create a cache with the same data storage location
    _ = self.create_dummy_file(self.location)
    with self.assertRaises(IOError):
      _ = self.cache_class(self.location)

  def test_overwrite_cache_2(self):
    # cache needs to be kept so that resources are not gc-ed immediately.
    cache = self.cache_class(self.location)  # pylint: disable=unused-variable
    # OK to overwrite cache when in "overwrite" mode
    _ = self.cache_class(self.location, mode="overwrite")

  def test_overwrite_cache_3(self):
    cache = self.cache_class(self.location)
    # OK to overwrite cleared cache
    cache.remove()
    _ = self.cache_class(self.location)

  def test_persist_false_0(self):
    cache = self.cache_class(self.location, persist=False)
    _ = self.create_dummy_file(self.location)
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)
    cache.remove()
    files = self._glob_files(self.location + "**")
    self.assertEqual(len(files), 0)

  def test_persist_false_1(self):
    cache = self.cache_class(self.location, persist=False)
    _ = self.create_dummy_file(self.location)
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)
    del cache
    gc.collect()
    files = self._glob_files(self.location + "**")
    self.assertEqual(len(files), 0)

  def test_persist_setter_false(self):
    cache = self.cache_class(self.location, persist=True)
    _ = self.create_dummy_file(self.location)
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)
    cache.persist = False
    del cache
    gc.collect()
    files = self._glob_files(self.location + "**")
    self.assertEqual(len(files), 0)

  def test_persist_true_0(self):
    cache = self.cache_class(self.location, persist=True)
    _ = self.create_dummy_file(self.location)
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)
    cache.remove()
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)

  def test_persist_true_1(self):
    cache = self.cache_class(self.location, persist=True)
    _ = self.create_dummy_file(self.location)
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)
    del cache
    gc.collect()
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)

  def test_persist_setter_true(self):
    cache = self.cache_class(self.location, persist=False)
    _ = self.create_dummy_file(self.location)
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)
    cache.persist = True
    del cache
    gc.collect()
    files = self._glob_files(self.location + "**")
    self.assertGreater(len(files), 0)

  def _glob_files(self, pattern):
    files = [
        metadata.path for match in FileSystems.match([self.location + "**"])
        for metadata in match.metadata_list
    ]
    return files

  def test_timestamp(self):
    # Seems to always pass when delay=0.01 but set higher to prevent flakiness
    cache = self.cache_class(self.location)
    timestamp0 = cache.timestamp
    time.sleep(0.1)
    _ = self.create_dummy_file(self.location)
    timestamp1 = cache.timestamp
    self.assertGreater(timestamp1, timestamp0)

  def test_writer_arguments(self):
    kwargs = {"a": 10, "b": "hello"}
    cache = self.cache_class(self.location, **kwargs)
    cache._reader_passthrough_arguments = set()
    cache.writer().expand(DummyPColl(element_type=None))
    _, kwargs_out = list(cache._writer_class.call_args)
    self.assertEqual(kwargs_out, kwargs)

  def test_reader_arguments(self):

    def check_reader_passthrough_kwargs(kwargs, passthrough):
      cache = self.cache_class(self.location, mode="overwrite", **kwargs)
      cache._reader_passthrough_arguments = passthrough
      cache.reader()
      _, kwargs_out = list(cache._reader_class.call_args)
      self.assertEqual(kwargs_out, {k: kwargs[k] for k in passthrough})

    check_reader_passthrough_kwargs({"a": 10, "b": "hello world"}, {})
    check_reader_passthrough_kwargs({"a": 10, "b": "hello world"}, {"b"})

  def test_infer_element_type_with_write(self):
    cache = self.cache_class(self.location)
    self.assertEqual(cache.element_type, None)
    for data in GENERIC_TEST_DATA:
      element_type = datatype_inference.infer_element_type(data)
      cache.truncate()
      self.assertEqual(cache.element_type, None)
      cache.write(data)
      self.assertEqual(cache.element_type, element_type)

  def test_infer_element_type_with_writer(self):
    cache = self.cache_class(self.location)
    self.assertEqual(cache.element_type, None)
    for data in GENERIC_TEST_DATA:
      element_type = datatype_inference.infer_element_type(data)
      cache.truncate()
      self.assertEqual(cache.element_type, None)
      cache.writer().expand(DummyPColl(element_type=element_type))
      self.assertEqual(cache.element_type, element_type)

  def test_default_coder(self):
    cache = self.cache_class(
        self.location, coder=file_based_cache.SafeFastPrimitivesCoder)
    cache._reader_passthrough_arguments = {"coder"}
    for element_type in GENERIC_ELEMENT_TYPES:
      cache.truncate()
      cache.element_type = element_type
      cache.writer().expand(PCollection(Pipeline(), element_type=None))
      _, kwargs_out = list(cache._writer_class.call_args)
      self.assertEqual(
          kwargs_out.get("coder"), file_based_cache.SafeFastPrimitivesCoder)

  def test_inferred_coder(self):
    cache = self.cache_class(self.location)
    cache._reader_passthrough_arguments = {"coder"}
    for element_type in GENERIC_ELEMENT_TYPES:
      cache.truncate()
      cache.element_type = element_type
      cache.writer().expand(PCollection(Pipeline(), element_type=None))
      _, kwargs_out = list(cache._writer_class.call_args)
      self.assertEqual(
          kwargs_out.get("coder"), coders.registry.get_coder(element_type))

  def test_writer(self):
    cache = self.cache_class(self.location)
    self.assertEqual(cache._writer_class.call_count, 0)
    cache.writer().expand(DummyPColl(element_type=None))
    self.assertEqual(cache._writer_class.call_count, 1)
    cache.writer().expand(DummyPColl(element_type=None))
    self.assertEqual(cache._writer_class.call_count, 2)

  def test_reader(self):
    cache = self.cache_class(self.location)
    self.assertEqual(cache._reader_class.call_count, 0)
    cache.reader()
    self.assertEqual(cache._reader_class.call_count, 1)
    cache.reader()
    self.assertEqual(cache._reader_class.call_count, 2)

  def test_write(self):
    cache = self.cache_class(self.location)
    self.assertEqual(cache._writer_class()._sink.open.call_count, 0)
    self.assertEqual(cache._writer_class()._sink.write_record.call_count, 0)
    self.assertEqual(cache._writer_class()._sink.close.call_count, 0)

    cache.write((i for i in range(11)))
    self.assertEqual(cache._writer_class()._sink.open.call_count, 1)
    self.assertEqual(cache._writer_class()._sink.write_record.call_count, 11)
    self.assertEqual(cache._writer_class()._sink.close.call_count, 1)

    cache.write((i for i in range(5)))
    self.assertEqual(cache._writer_class()._sink.open.call_count, 2)
    self.assertEqual(cache._writer_class()._sink.write_record.call_count, 16)
    self.assertEqual(cache._writer_class()._sink.close.call_count, 2)

    class DummyError(Exception):
      pass

    cache._writer_class()._sink.write_record.side_effect = DummyError
    with self.assertRaises(DummyError):
      cache.write((i for i in range(5)))
    self.assertEqual(cache._writer_class()._sink.open.call_count, 3)
    self.assertEqual(cache._writer_class()._sink.write_record.call_count, 17)
    self.assertEqual(cache._writer_class()._sink.close.call_count, 3)

  def test_read(self):
    cache = self.cache_class(self.location)
    _ = self.create_dummy_file(self.location)
    self.assertEqual(cache._reader_class()._source.read.call_count, 0)
    cache.read()
    # ._read does not get called unless we get items from iterator
    self.assertEqual(cache._reader_class()._source.read.call_count, 0)
    list(cache.read())
    self.assertEqual(cache._reader_class()._source.read.call_count, 1)
    list(cache.read())
    self.assertEqual(cache._reader_class()._source.read.call_count, 2)

  def test_truncate(self):
    cache = self.cache_class(self.location)
    self.assertEqual(
        len(list(file_based_cache.glob_files(cache.file_pattern))), 1)
    _ = self.create_dummy_file(self.location)
    self.assertEqual(
        len(list(file_based_cache.glob_files(cache.file_pattern))), 2)
    cache.truncate()
    self.assertEqual(
        len(list(file_based_cache.glob_files(cache.file_pattern))), 1)

  def test_clear_data(self):
    cache = self.cache_class(self.location)
    self.assertEqual(
        len(list(file_based_cache.glob_files(cache.file_pattern))), 1)
    _ = self.create_dummy_file(self.location)
    self.assertEqual(
        len(list(file_based_cache.glob_files(cache.file_pattern))), 2)
    cache.remove()
    self.assertEqual(
        len(list(file_based_cache.glob_files(cache.file_pattern))), 0)


class DummyPColl(object):

  def __init__(self, element_type):
    self.element_type = element_type

  def __or__(self, unused_other):
    return self


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
