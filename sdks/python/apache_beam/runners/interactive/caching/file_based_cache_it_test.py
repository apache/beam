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

import logging
import pickle
import sys
import tempfile
import unittest

import dill
import numpy as np
from parameterized import parameterized

from apache_beam.io.filesystems import FileSystems
from apache_beam.runners.interactive.caching import file_based_cache
from apache_beam.runners.interactive.caching import file_based_cache_test
from apache_beam.testing.extra_assertions import ExtraAssertionsMixin
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that


class Validators(unittest.TestCase, ExtraAssertionsMixin):

  def validate_directly(self, cache, expected):
    actual = list(cache.read())
    self.assertUnhashableCountEqual(actual, expected)

  def validate_through_pipeline(self, cache, expected):

    def equal_to_expected(actual):
      self.assertUnhashableCountEqual(actual, expected)

    p = TestPipeline()
    pcoll = p | "Read" >> cache.reader()
    assert_that(pcoll, equal_to_expected)
    p.run()

  if sys.version_info < (3,):

    def runTest(self):
      pass


DATAFRAME_TEST_DATA = [
    [],
    [{}],
    [{}, {}, {}],
    [{"col1": "abc", "col2": "def"}, {"col1": "hello"}],
    [{"col1": "abc", "col2": "def"}, {"col1": "hello", "col2": "good bye"}],
    [{"col1": b"abc", "col2": "def"}, {"col1": b"hello", "col2": "good bye"}],
    [{"col1": u"abc", "col2": u"±♠Ω"}, {"col1": u"hello", "col2": u"Ωℑ"}],
    [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}],
    [{"x": 123, "y": 5.55}, {"x": 555, "y": 6.63}],
    [{"x": np.array([1, 2])}, {"x": np.array([3, 4, 5])}],
]


class TestCase(ExtraAssertionsMixin, unittest.TestCase):
  pass


# #############################################################################
# Serialization
# #############################################################################


class SerializationTestBase(object):

  # Attributes to be set by child classes.
  cache_class = None
  location = None

  def get_writer_kwargs(self, data=None):
    return {}

  test_data = [{"a": 11, "b": "XXX"}, {"a": 20, "b": "YYY"}]

  def check_serialize_deserialize_empty(self, write_fn, read_fn, serializer):
    cache = self.cache_class(self.location,
                             **self.get_writer_kwargs(self.test_data))
    cache_out = serializer.loads(serializer.dumps(cache))
    write_fn(cache_out, self.test_data)
    data_out = list(read_fn(cache_out, limit=len(self.test_data)))
    self.assertEqual(data_out, self.test_data)

  def check_serialize_deserialize_filled(self, write_fn, read_fn, serializer):
    cache = self.cache_class(self.location,
                             **self.get_writer_kwargs(self.test_data))
    write_fn(cache, self.test_data)
    cache_out = serializer.loads(serializer.dumps(cache))
    data_out = list(read_fn(cache_out, limit=len(self.test_data)))
    self.assertEqual(data_out, self.test_data)


class FileSerializationTestBase(SerializationTestBase):

  # Attributes to be set by child classes.
  cache_class = None

  def setUp(self):
    self._temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self._temp_dir, self.cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self._temp_dir])

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  def test_serialize_deserialize_empty(self, _, serializer):
    self.check_serialize_deserialize_empty(file_based_cache_test.write_directly,
                                           file_based_cache_test.read_directly,
                                           serializer)

  @parameterized.expand([("pickle", pickle), ("dill", dill)])
  def test_serialize_deserialize_filled(self, _, serializer):
    self.check_serialize_deserialize_filled(
        file_based_cache_test.write_directly,
        file_based_cache_test.read_directly, serializer)


class TextBasedCacheSerializationTest(FileSerializationTestBase, TestCase):

  cache_class = file_based_cache.TextBasedCache


class TFRecordBasedCacheSerializationTest(FileSerializationTestBase, TestCase):

  cache_class = file_based_cache.TFRecordBasedCache


# #############################################################################
# Roundtrip
# #############################################################################


class RoundtripTestBase(object):

  # Attributes to be set by child classes.
  cache_class = None
  location = None

  def get_writer_kwargs(self, data=None):
    return {}

  def check_roundtrip(self, write_fn, validate_fn, dataset):
    """Make sure that data can be correctly written using the write_fn function
    and read using the validate_fn function.
    """
    cache = self.cache_class(self.location, **self.get_writer_kwargs(None))
    for data in dataset:
      cache._writer_kwargs.update(self.get_writer_kwargs(data))
      write_fn(cache, data)
      validate_fn(cache, data)
      write_fn(cache, data)
      validate_fn(cache, data * 2)
      cache.truncate()
      validate_fn(cache, [])
    cache.remove()


class FileRoundtripTestBase(RoundtripTestBase):

  # Attributes to be set by child classes.
  cache_class = None
  dataset = None

  def get_writer_kwargs(self, data=None):
    return {}

  def setUp(self):
    self._temp_dir = tempfile.mkdtemp()
    self.location = FileSystems.join(self._temp_dir, self.cache_class.__name__)

  def tearDown(self):
    FileSystems.delete([self._temp_dir])

  @parameterized.expand([
      ("{}-{}".format(write_fn.__name__,
                      validate_fn.__name__), write_fn, validate_fn)
      for write_fn in [
          file_based_cache_test.write_directly,
          file_based_cache_test.write_through_pipeline
      ] for validate_fn in
      [Validators().validate_directly,
       Validators().validate_through_pipeline]
  ])
  def test_roundtrip(self, _, write_fn, validate_fn):
    return self.check_roundtrip(write_fn, validate_fn, dataset=self.dataset)


class TextBasedCacheRoundtripTest(FileRoundtripTestBase, TestCase):

  cache_class = file_based_cache.TextBasedCache
  dataset = file_based_cache_test.GENERIC_TEST_DATA


class TFRecordBasedCacheRoundtripTest(FileRoundtripTestBase, TestCase):

  cache_class = file_based_cache.TFRecordBasedCache
  dataset = file_based_cache_test.GENERIC_TEST_DATA


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
