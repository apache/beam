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
"""Tests for schemas."""

from __future__ import absolute_import

from apache_beam.schemas import Schema, ListType, MapType
import unittest
import numpy as np
import itertools


class SchemaTest(object):

  def test_create_schema_validates(self):
    self.create_schema()

  def test_eq(self):
    assert self.create_schema() == self.create_schema()

  def test_proto_roundtrip(self):
    schema = self.create_schema()
    converted = Schema.from_schema_api(schema.to_schema_api())

    assert converted == schema


class EmptySchema(SchemaTest, unittest.TestCase):

  @staticmethod
  def create_schema():
    return Schema({})


ALL_PRIMITIVE_TYPES = [
    ('byte', np.byte),
    ('int16', np.int16),
    ('int32', np.int32),
    ('int64', np.int64),
    ('float32', np.float32),
    ('float64', np.float64),
    ('unicode', np.unicode),
    ('bytes', np.bytes_),
]


class AllPrimitivesSchemaTest(SchemaTest, unittest.TestCase):

  @staticmethod
  def create_schema():
    return Schema(
        names=(item[0] for item in ALL_PRIMITIVE_TYPES),
        types=(item[1] for item in ALL_PRIMITIVE_TYPES))


class AllPrimitivesListSchemaTest(SchemaTest, unittest.TestCase):

  @staticmethod
  def create_schema():
    return Schema(
        names=(item[0] for item in ALL_PRIMITIVE_TYPES),
        types=(ListType(item[1]) for item in ALL_PRIMITIVE_TYPES))

class AllPrimitivesMapSchemaTest(SchemaTest, unittest.TestCase):

  @staticmethod
  def create_schema():
    primitive_type_pairs = list(itertools.product(ALL_PRIMITIVE_TYPES, ALL_PRIMITIVE_TYPES))
    return Schema(
        names=("{0}_{1}".format(key_item[0], value_item[0]) for key_item, value_item in primitive_type_pairs),
        types=(MapType(key_item[1], value_item[1]) for key_item, value_item in primitive_type_pairs))


class AdditionalSchemaTest(unittest.TestCase):

  def test_different_schemas_eq_returns_false(self):
    assert not Schema({
        'foo': np.int16,
        'bar': np.int32
    }) == Schema({'baz': np.unicode})

  def test_out_of_order_schemas_eq_returns_false(self):
    assert not Schema(
        names=['foo', 'bar'], types=[np.int16, np.int32]) == Schema(
            names=['bar', 'foo'], types=[np.int32, np.int16])


if __name__ == '__main__':
  unittest.main()
