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

import array
import json
import os
import shutil
import sys
import tempfile
import unittest
import uuid

import avro.schema
import numpy as np
import pyarrow as pa
from nose.plugins.attrib import attr
from parameterized import parameterized
from past.builtins import unicode
from apache_beam.testing.test_pipeline import TestPipeline

from apache_beam import coders
from apache_beam.io.filesystems import FileSystems
from apache_beam.runners import DirectRunner
from apache_beam.runners.interactive.caching.filebasedcache import *
from apache_beam.transforms import Create
from apache_beam.typehints import trivial_inference
from apache_beam.typehints import typehints

if sys.version_info > (3,):
  long = int

# yapf: disable
DATASET_TEST_DATA = [
    #
    ("empty", []),
    ("none_1", [None]),
    ("none_2", [None, None, None]),
    ("strings_1", ["ABC"]),
    ("strings_2", ["ABC", "DeF"]),
    ("strings_3", [u"ABC", u"±♠Ωℑ"]),
    ("numbers_1", [1.5]),
    ("numbers_2", [100, -123.456, 78.9]),
    ("numbers_3", [b"abc123"]),
    ("bytes_1", [b"abc123", b"aAaAa"]),
    ("bytes_2", ["ABC", 1.2, 100, 0, -10, None, b"abc123"]),
    ("mixed_primitive_types", [("a", "b", "c")]),
    ("tuples_1", [("a", 1, 1.2), ("b", 2, 5.5)]),
    ("tuples_2", [("a", 1, 1.2), (2.5, "c", None)]),
    ("tuples_3", [{"col1": "a", "col2": 1, "col3": 1.5}]),
    ("dictionaries_1", [
        {"col1": "a", "col2": 1, "col3": 1.5},
        {"col1": "b", "col2": 2, "col3": 4.5}]),
    ("dictionaries_2", [
        {"col1": "a", "col2": 1, "col3": 1.5},
        {4: 1, 5: 3.4, (6, 7): "a"}]),
    ("dictionaries_3", [{
        "col1": "a",
        "col2": 1,
        "col3": 1.5
    }, {
        "col1": 1,
        "col2": 3.4,
        "col3": "a",
    }]),
    ("roundtrip_mixed_compound_types", [("a", "b", "c"), ["d", 1], {
        "col1": 1,
        202: 1.234
    }, None, "abc", b"def", 100, (1, 2, 3, "b")])
]
# yapf: enable

# yapf: disable
AVRO_TEST_DATA = [
    # TODO(ostrokach): Empty PCollections not supported by Arrow.
    ("empty_1", []),
    ("empty_2", [{}]),
    ("empty_3", [{}, {}, {}]),
    # TODO(ostrokach): Rows with missing columns not supported by Arrow / Avro.
    # ("string_1", [{"col1": "abc", "col2": "def"}, {"col1": "hello"}]),
    ("string_1", [
        {"col1": "abc", "col2": "def"},
        {"col1": "hello", "col2": "good bye"}]),
    ("string_2", [
        {"col1": b"abc", "col2": "def"},
        {"col1": b"hello", "col2": "good bye"}]),
    ("string_3", [
        {"col1": u"abc", "col2": u"±♠Ω"},
        {"col1": u"hello", "col2": u"Ωℑ"}]),
    ("numeric_1", [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}]),
    ("numeric_2", [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}]),
    # TODO(ostrokach): Arrays are not supported (but not difficult to implement)
    # ("array_1", [{"x": np.array([1,2])}, {"x": np.array([3,4, 5])}]),
]
# yapf: enable

# yapf: disable
PARQUET_TEST_DATA = [
    # TODO(ostrokach): Empty PCollections not supported by PyArrow.
    # ("empty_1", []),
    # ("empty_2", [{}]),
    # ("empty_3", [{}, {}, {}]),
    # TODO(ostrokach): Rows with missing columns not supported by Arrow / Avro.
    # ("string_1", [{"col1": "abc", "col2": "def"}, {"col1": "hello"}]),
    ("string_1", [
        {"col1": "abc", "col2": "def"},
        {"col1": "hello", "col2": "good bye"}]),
    ("string_2", [
        {"col1": b"abc", "col2": "def"},
        {"col1": b"hello", "col2": "good bye"}]),
    ("string_3", [
        {"col1": u"abc", "col2": u"±♠Ω"},
        {"col1": u"hello", "col2": u"Ωℑ"}]),
    ("numeric_1", [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}]),
    ("numeric_2", [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}]),
    # TODO(ostrokach): Arrays are not supported (but not difficult to implement)
    ("array_1", [{"x": np.array([1,2])}, {"x": np.array([3,4, 5])}]),
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


def infer_column_coders(data):
  column_data = {}
  for row in data:
    for key, value in row.items():
      column_data.setdefault(key, []).append(value)
  column_coders = {
      key:
      typehints.Union[[trivial_inference.instance_to_type(v) for v in value]]
      for key, value in column_data.items()
  }
  return column_coders


def infer_avro_schema(data):
  _typehint_to_avro_type = {
      typehints.Union[[int]]: "int",
      typehints.Union[[int, None]]: ["int", "null"],
      typehints.Union[[long]]: "long",
      typehints.Union[[long, None]]: ["long", "null"],
      typehints.Union[[float]]: "double",
      typehints.Union[[float, None]]: ["double", "null"],
      typehints.Union[[str]]: "string",
      typehints.Union[[str, None]]: ["string", "null"],
      typehints.Union[[unicode]]: "string",
      typehints.Union[[unicode, None]]: ["string", "null"],
      typehints.Union[[np.ndarray]]: "bytes",
      typehints.Union[[np.ndarray, None]]: ["bytes", "null"],
      typehints.Union[[array.array]]: "bytes",
      typehints.Union[[array.array, None]]: ["bytes", "null"],
  }

  column_coders = infer_column_coders(data)
  avro_fields = [{
      "name": str(key),
      "type": _typehint_to_avro_type[value]
  } for key, value in column_coders.items()]
  schema_dict = {
      "namespace": "example.avro",
      "type": "record",
      "name": "User",
      "fields": avro_fields
  }
  return avro.schema.parse(json.dumps(schema_dict))


def infer_parquet_schema(data):
  column_data = {}
  for row in data:
    for key, value in row.items():
      column_data.setdefault(key, []).append(value)
  column_types = {
      key: pa.array(value).type for key, value in column_data.items()
  }
  return pa.schema(list(column_types.items()))


class CheckCoder(object):

  _default_coder = None

  def setUp(self):
    self.temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self.temp_dir, self._cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self.temp_dir])

  @parameterized.expand([
      ("{}-{}".format(write_fn.__name__, data_name), write_fn, data)
      for write_fn in [write_directly, write_through_pipeline]
      for data_name, data in DATASET_TEST_DATA
  ])
  @attr('IT')
  def test_coder(self, _, write_fn, data):
    element_type = typehints.Union[[
        trivial_inference.instance_to_type(v) for v in data
    ]]
    coder = coders.registry.get_coder(element_type)

    cache = self._cache_class(self.location)
    self.assertEqual(cache._writer_kwargs.get("coder"), self._default_coder)
    write_fn(cache, data)
    self.assertEqual(cache._writer_kwargs.get("coder"), coder)
    cache.clear()
    self.assertEqual(cache._writer_kwargs.get("coder"), self._default_coder)


class CheckRoundtripDataset(object):

  def setUp(self):
    self.temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self.temp_dir, self._cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self.temp_dir])

  @parameterized.expand([
      ("{}-{}-{}".format(write_fn.__name__, read_fn.__name__,
                         data_name), write_fn, read_fn, data)
      for write_fn in [write_directly, write_through_pipeline]
      for read_fn in [read_directly, read_through_pipeline]
      for data_name, data in DATASET_TEST_DATA
  ])
  @attr('IT')
  def test_roundtrip(self, _, write_fn, read_fn, data):
    cache = self._cache_class(self.location)
    write_fn(cache, data)
    data_out = read_fn(cache)
    self.assertEqual(data_out, data)
    cache.clear()
    with self.assertRaises(IOError):
      data_out = read_fn(cache)


# TextBasedCache


class TextBasedCacheCoderTest(CheckCoder, unittest.TestCase):

  _cache_class = TextBasedCache


class TextBasedCacheRoundtripTest(CheckRoundtripDataset, unittest.TestCase):

  _cache_class = TextBasedCache


# SafeTextBasedCache


class SafeTextBasedCacheCoderTest(CheckCoder):

  _cache_class = SafeTextBasedCache
  _default_coder = SafeFastPrimitivesCoder()


class SafeTextBasedCacheRoundtripTest(CheckRoundtripDataset, unittest.TestCase):

  _cache_class = SafeTextBasedCache


# TFRecordBasedCache


class TFRecordBasedCacheCoderTest(CheckCoder, unittest.TestCase):

  _cache_class = TFRecordBasedCache


class TFRecordBasedCacheRoundtripTest(CheckRoundtripDataset, unittest.TestCase):

  _cache_class = TFRecordBasedCache


## AvroBasedCache


class AvroBasedCacheRoundtripTest(unittest.TestCase):

  _cache_class = AvroBasedCache
  _schema_gen = staticmethod(infer_avro_schema)

  def setUp(self):
    self.temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self.temp_dir, self._cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self.temp_dir])

  @parameterized.expand([
      ("{}-{}-{}".format(write_fn.__name__, read_fn.__name__,
                         data_name), write_fn, read_fn, data)
      for write_fn in [write_directly, write_through_pipeline]
      for read_fn in [read_directly, read_through_pipeline]
      for data_name, data in AVRO_TEST_DATA
  ])
  @attr('IT')
  def test_roundtrip(self, _, write_fn, read_fn, data):
    schema = self._schema_gen(data)
    cache = self._cache_class(self.location, schema=schema)
    write_fn(cache, data)
    data_out = read_fn(cache)
    self.assertEqual(data_out, data)
    cache.clear()
    with self.assertRaises(IOError):
      data_out = read_fn(cache)


## ParquetBasedCache


class ParquetBasedCacheRoundtripTest(unittest.TestCase):

  _cache_class = ParquetBasedCache
  _schema_gen = staticmethod(infer_parquet_schema)

  def setUp(self):
    self.temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self.temp_dir, self._cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self.temp_dir])

  @parameterized.expand([
      ("{}-{}-{}".format(write_fn.__name__, read_fn.__name__,
                         data_name), write_fn, read_fn, data)
      for write_fn in [write_directly, write_through_pipeline]
      for read_fn in [read_directly, read_through_pipeline]
      for data_name, data in PARQUET_TEST_DATA
  ])
  @attr('IT')
  def test_roundtrip(self, _, write_fn, read_fn, data):
    schema = self._schema_gen(data)
    cache = self._cache_class(self.location, schema=schema)
    write_fn(cache, data)
    data_out = read_fn(cache)
    self.assert_elements_equal(data_out, data)
    cache.clear()
    with self.assertRaises(IOError):
      data_out = read_fn(cache)

  def assert_elements_equal(self, data1, data2):
    self.assertEqual(len(data1), len(data2))
    for row1, row2 in zip(data1, data2):
      self.assertEqual(sorted(row1), sorted(row2))
      for c in row1:
        if isinstance(row1[c], (list, np.ndarray)):
          self.assertTrue(isinstance(row2[c], np.ndarray))
          self.assertTrue(np.allclose(row1[c], row2[c]))
        else:
          self.assertEqual(row1[c], row2[c])
