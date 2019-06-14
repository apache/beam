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

import os
import pickle
import shutil
import sys
import tempfile
import unittest
import uuid
import dill
import numpy as np
from parameterized import parameterized
from past.builtins import unicode

from apache_beam import coders
from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.interactive.caching.datatype_inference import \
    infer_avro_schema
from apache_beam.runners.interactive.caching.datatype_inference import \
    infer_element_type
from apache_beam.runners.interactive.caching.datatype_inference import \
    infer_pyarrow_schema
from apache_beam.runners.interactive.caching.filebasedcache import *
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms import Create

if sys.version_info > (3,):
  long = int

# yapf: disable
GENERIC_TEST_DATA = [
    #
    ("empty_0", []),
    ("none_0", [None]),
    ("none_1", [None, None, None]),
    ("strings_0", ["ABC"]),
    ("strings_1", ["ABC", "DeF"]),
    ("strings_2", [u"ABC", u"±♠Ωℑ"]),
    ("numbers_0", [1.5]),
    ("numbers_1", [100, -123.456, 78.9]),
    ("numbers_2", [b"abc123"]),
    ("bytes_0", [b"abc123", b"aAaAa"]),
    ("bytes_1", ["ABC", 1.2, 100, 0, -10, None, b"abc123"]),
    ("mixed_primitive_types_0", [("a", "b", "c")]),
    ("tuples_0", [("a", 1, 1.2), ("b", 2, 5.5)]),
    ("tuples_1", [("a", 1, 1.2), (2.5, "c", None)]),
    ("tuples_2", [{"col1": "a", "col2": 1, "col3": 1.5}]),
    ("dictionaries_0", [{}]),
    ("dictionaries_1", [
        {"col1": "a", "col2": 1, "col3": 1.5},
        {"col1": "b", "col2": 2, "col3": 4.5}]),
    ("dictionaries_2", [
        {"col1": "a", "col2": 1, "col3": 1.5},
        {4: 1, 5: 3.4, (6, 7): "a"}]),
    ("dictionaries_3", [
        {"col1": "a", "col2": 1.5},
        {"col1": 1, "col3": u"±♠Ω"},
    ]),
    ("dictionaries_4", [{"a": 10}]),
    ("mixed_compound_types_0", [("a", "b", "c"), ["d", 1], {
        "col1": 1,
        202: 1.234
    }, None, "abc", b"def", 100, (1, 2, 3, "b")])
]
# yapf: enable

# yapf: disable
DATAFRAME_TEST_DATA = [
    ("empty_0", []),
    ("empty_1", [{}]),
    ("empty_2", [{}, {}, {}]),
    ("missing_columns_0", [{"col1": "abc", "col2": "def"}, {"col1": "hello"}]),
    ("string_0", [
        {"col1": "abc", "col2": "def"},
        {"col1": "hello", "col2": "good bye"}]),
    ("string_1", [
        {"col1": b"abc", "col2": "def"},
        {"col1": b"hello", "col2": "good bye"}]),
    ("string_2", [
        {"col1": u"abc", "col2": u"±♠Ω"},
        {"col1": u"hello", "col2": u"Ωℑ"}]),
    ("numeric_0", [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}]),
    ("numeric_1", [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}]),
    ("array_0", [{"x": np.array([1, 2])}, {"x": np.array([3, 4, 5])}]),
]
# yapf: enable


def read_through_pipeline(cache):
  """Read elements from cache using a Beam pipeline."""
  temp_dir = tempfile.mkdtemp()
  temp_cache = SafeTextBasedCache(os.path.join(temp_dir, uuid.uuid1().hex))
  try:
    with TestPipeline() as p:
      _ = (p | "Read" >> cache.reader() | "Write" >> temp_cache.writer())
    return list(temp_cache.read())
  finally:
    shutil.rmtree(temp_dir)


def write_through_pipeline(cache, data_in):
  """Write elements to cache using a Beam pipeline."""
  with TestPipeline() as p:
    _ = (p | "Create" >> Create(data_in) | "Write" >> cache.writer())


def read_directly(cache):
  """Read elements from cache using the cache API."""
  return list(cache.read())


def write_directly(cache, data_in):
  """Write elements to cache using the cache API."""
  cache.write(data_in)


class ExtraAssertions(object):

  def assertElementsEqual(self, data1, data2):
    try:
      iter(data1)
    except TypeError:
      self.assertEqual(data1, data2)
      return

    # self.assertEqual(type(data1), type(data2))
    self.assertEqual(len(data1), len(data2))

    if isinstance(data1, (str, bytes, unicode)):
      self.assertEqual(data1, data2)
      return

    if isinstance(data1, dict):
      self.assertEqual(sorted(data1.keys()), sorted(data2.keys()))
      for key in data1:
        self.assertElementsEqual(data1[key], data2[key])
      return

    if isinstance(data1, np.ndarray):
      self.assertTrue(np.allclose(data1, data2))
      return

    for value1, value2 in zip(data1, data2):
      self.assertElementsEqual(value1, value2)


class FunctionalTestBase(ExtraAssertions):

  _cache_class = None

  def _get_writer_kwargs(self, data):
    return {}

  def setUp(self):
    self.temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self.temp_dir, self._cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self.temp_dir])

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  def test_serde_empty(self, _, serializer):
    test_data = [{"a": 11, "b": "XXX"}, {"a": 20, "b": "YYY"}]
    cache = self._cache_class(self.location,
                              **self._get_writer_kwargs(test_data))
    with self.assertRaises(IOError):
      _ = list(cache.read())
    cache_out = serializer.loads(serializer.dumps(cache))
    with self.assertRaises(IOError):
      _ = list(cache_out.read())
    cache_out.write(test_data)
    self.assertEqual(list(cache_out.read()), test_data)

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  def test_serde_filled(self, _, serializer):
    test_data = [{"a": 11, "b": "XXX"}, {"a": 20, "b": "YYY"}]
    cache = self._cache_class(self.location,
                              **self._get_writer_kwargs(test_data))
    cache.write(test_data)
    self.assertEqual(list(cache.read()), test_data)
    cache_out = serializer.loads(serializer.dumps(cache))
    self.assertEqual(list(cache_out.read()), test_data)

  def check_roundtrip(self, write_fn, read_fn, data):
    cache = self._cache_class(self.location, **self._get_writer_kwargs(data))
    write_fn(cache, data)
    data_out = read_fn(cache)
    self.assertElementsEqual(data_out, data)
    write_fn(cache, data)
    data_out = read_fn(cache)
    self.assertElementsEqual(data_out, data * 2)
    cache.clear()
    with self.assertRaises(IOError):
      data_out = read_fn(cache)


class GenericRoundtripTestBase(FunctionalTestBase):

  @parameterized.expand([
      ("{}-{}-{}".format(
          data_name,
          write_fn.__name__,
          read_fn.__name__,
      ), write_fn, read_fn, data) for data_name, data in GENERIC_TEST_DATA
      for write_fn in [write_directly, write_through_pipeline]
      for read_fn in [read_directly, read_through_pipeline]
  ])
  def test_roundtrip(self, _, write_fn, read_fn, data):
    return self.check_roundtrip(write_fn, read_fn, data)


class DataframeRoundtripTestBase(FunctionalTestBase):

  @parameterized.expand([
      ("{}-{}-{}".format(
          data_name,
          write_fn.__name__,
          read_fn.__name__,
      ), write_fn, read_fn, data) for data_name, data in DATAFRAME_TEST_DATA
      for write_fn in [write_directly, write_through_pipeline]
      for read_fn in [read_directly, read_through_pipeline]
  ])
  def test_roundtrip(self, _, write_fn, read_fn, data):
    return self.check_roundtrip(write_fn, read_fn, data)


class CoderTestBase(FunctionalTestBase):
  """Make sure that the coder gets set correctly when we write data to cache."""
  _default_coder = None

  @parameterized.expand([
      ("{}-{}".format(data_name, write_fn.__name__), write_fn, data)
      for data_name, data in GENERIC_TEST_DATA
      for write_fn in [write_directly, write_through_pipeline]
  ])
  def test_coder(self, _, write_fn, data):
    inferred_coder = self._default_coder or coders.registry.get_coder(
        infer_element_type(data))
    cache = self._cache_class(self.location, **self._get_writer_kwargs(data))
    self.assertEqual(cache._writer_kwargs.get("coder"), self._default_coder)
    write_fn(cache, data)
    self.assertEqual(cache._writer_kwargs.get("coder"), inferred_coder)
    cache.clear()
    self.assertEqual(cache._writer_kwargs.get("coder"), self._default_coder)


# TextBasedCache


class TextBasedCacheRoundtripTest(GenericRoundtripTestBase, CoderTestBase,
                                  unittest.TestCase):

  _cache_class = TextBasedCache

  def check_roundtrip(self, write_fn, read_fn, data):
    if data == [{"a": 10}]:
      self.skipTest(
          "TextBasedCache crashes for this particular case. "
          "One of the reasons why it should not be used in production.")
    return super(TextBasedCacheRoundtripTest,
                 self).check_roundtrip(write_fn, read_fn, data)


# SafeTextBasedCache


class SafeTextBasedCacheRoundtripTest(GenericRoundtripTestBase, CoderTestBase,
                                      unittest.TestCase):

  _cache_class = SafeTextBasedCache
  _default_coder = SafeFastPrimitivesCoder()


# TFRecordBasedCache


class TFRecordBasedCacheRoundtripTest(GenericRoundtripTestBase, CoderTestBase,
                                      unittest.TestCase):

  _cache_class = TFRecordBasedCache


## AvroBasedCache


class AvroBasedCacheRoundtripBase(DataframeRoundtripTestBase):

  _cache_class = AvroBasedCache

  def check_roundtrip(self, write_fn, read_fn, data):
    if not data:
      pass
    elif not all(
        (sorted(data[0].keys()) == sorted(d.keys()) for d in data[1:])):
      self.skipTest("Rows with missing columns are not supported.")
    elif any((isinstance(v, np.ndarray) for v in data[0].values())):
      self.skipTest("Array data are not supported.")
    return super(AvroBasedCacheRoundtripBase,
                 self).check_roundtrip(write_fn, read_fn, data)


class AvroBasedCacheRoundtripTest(AvroBasedCacheRoundtripBase,
                                  unittest.TestCase):

  def _get_writer_kwargs(self, data):
    if sys.version > (3,):
      self.skipTest("Only fastavro is supported on Python 3.")
    use_fastavro = False
    schema = infer_avro_schema(data, use_fastavro=use_fastavro)
    return dict(schema=schema, use_fastavro=use_fastavro)


class FastAvroBasedCacheRoundtripTest(AvroBasedCacheRoundtripBase,
                                      unittest.TestCase):

  def _get_writer_kwargs(self, data):
    use_fastavro = True
    schema = infer_avro_schema(data, use_fastavro=use_fastavro)
    return dict(schema=schema, use_fastavro=use_fastavro)


## ParquetBasedCache


class ParquetBasedCacheRoundtripTest(DataframeRoundtripTestBase,
                                     unittest.TestCase):

  _cache_class = ParquetBasedCache

  def _get_writer_kwargs(self, data):
    schema = infer_pyarrow_schema(data)
    return dict(schema=schema)

  def check_roundtrip(self, write_fn, read_fn, data):
    if not data:
      self.skipTest("Empty PCollections are not supported.")
    elif not data[0]:
      self.skipTest("PCollections with no columns are not supported.")
    elif not all(
        (sorted(data[0].keys()) == sorted(d.keys()) for d in data[1:])):
      self.skipTest("Rows with missing columns are not supported.")
    return super(ParquetBasedCacheRoundtripTest,
                 self).check_roundtrip(write_fn, read_fn, data)
