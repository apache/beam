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

"""Unit tests for the Beam Row typing functionality."""

import typing
import unittest
from dataclasses import dataclass

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints import row_type
from apache_beam.typehints import schemas


class RowTypeTest(unittest.TestCase):
  @staticmethod
  def _check_key_type_and_count(x) -> int:
    key_type = type(x[0])
    if not row_type._user_type_is_generated(key_type):
      raise RuntimeError("Expect type after GBK to be generated user type")

    return len(x[1])

  def test_group_by_key_namedtuple(self):
    MyNamedTuple = typing.NamedTuple(
        "MyNamedTuple", [("id", int), ("name", str)])

    beam.coders.typecoders.registry.register_coder(
        MyNamedTuple, beam.coders.RowCoder)

    def generate(num: int):
      for i in range(100):
        yield (MyNamedTuple(i, 'a'), num)

    pipeline = TestPipeline(is_integration_test=False)

    with pipeline as p:
      result = (
          p
          | 'Create' >> beam.Create([i for i in range(10)])
          | 'Generate' >> beam.ParDo(generate).with_output_types(
              tuple[MyNamedTuple, int])
          | 'GBK' >> beam.GroupByKey()
          | 'Count Elements' >> beam.Map(self._check_key_type_and_count))
      assert_that(result, equal_to([10] * 100))

  def test_group_by_key_dataclass(self):
    @dataclass
    class MyDataClass:
      id: int
      name: str

    beam.coders.typecoders.registry.register_coder(
        MyDataClass, beam.coders.RowCoder)

    def generate(num: int):
      for i in range(100):
        yield (MyDataClass(i, 'a'), num)

    pipeline = TestPipeline(is_integration_test=False)

    with pipeline as p:
      result = (
          p
          | 'Create' >> beam.Create([i for i in range(10)])
          | 'Generate' >> beam.ParDo(generate).with_output_types(
              tuple[MyDataClass, int])
          | 'GBK' >> beam.GroupByKey()
          | 'Count Elements' >> beam.Map(self._check_key_type_and_count))
      assert_that(result, equal_to([10] * 100))

  def test_group_by_key_namedtuple_union(self):
    Tuple1 = typing.NamedTuple("Tuple1", [("id", int)])

    Tuple2 = typing.NamedTuple("Tuple2", [("id", int), ("name", str)])

    def generate(num: int):
      for i in range(2):
        yield (Tuple1(i), num)
        yield (Tuple2(i, 'a'), num)

    pipeline = TestPipeline(is_integration_test=False)

    with pipeline as p:
      result = (
          p
          | 'Create' >> beam.Create([i for i in range(2)])
          | 'Generate' >> beam.ParDo(generate).with_output_types(
              tuple[(Tuple1 | Tuple2), int])
          | 'GBK' >> beam.GroupByKey()
          | 'Count' >> beam.Map(lambda x: len(x[1])))
      assert_that(result, equal_to([2] * 4))

  # Union of dataclasses as type hint currently result in FastPrimitiveCoder
  # fails at GBK
  @unittest.skip("https://github.com/apache/beam/issues/22085")
  def test_group_by_key_inherited_dataclass_union(self):
    @dataclass
    class DataClassInt:
      id: int

    @dataclass
    class DataClassStr(DataClassInt):
      name: str

    beam.coders.typecoders.registry.register_coder(
        DataClassInt, beam.coders.RowCoder)
    beam.coders.typecoders.registry.register_coder(
        DataClassStr, beam.coders.RowCoder)

    def generate(num: int):
      for i in range(10):
        yield (DataClassInt(i), num)
        yield (DataClassStr(i, 'a'), num)

    pipeline = TestPipeline(is_integration_test=False)

    with pipeline as p:
      result = (
          p
          | 'Create' >> beam.Create([i for i in range(2)])
          | 'Generate' >> beam.ParDo(generate).with_output_types(
              tuple[(DataClassInt | DataClassStr), int])
          | 'GBK' >> beam.GroupByKey()
          | 'Count Elements' >> beam.Map(self._check_key_type_and_count))
      assert_that(result, equal_to([2] * 4))

  def test_derived_dataclass_schema_id(self):
    @dataclass
    class BaseDataClass:
      id: int

    @dataclass
    class DerivedDataClass(BaseDataClass):
      name: str

    self.assertFalse(hasattr(BaseDataClass, row_type._BEAM_SCHEMA_ID))
    schema_for_base = schemas.schema_from_element_type(BaseDataClass)
    self.assertTrue(hasattr(BaseDataClass, row_type._BEAM_SCHEMA_ID))
    self.assertEqual(
        schema_for_base.id, getattr(BaseDataClass, row_type._BEAM_SCHEMA_ID))

    # Getting the schema for BaseDataClass sets the _beam_schema_id
    schemas.typing_to_runner_api(
        BaseDataClass, schema_registry=schemas.SchemaTypeRegistry())

    # We create a RowTypeConstraint from DerivedDataClass.
    # It should not inherit the _beam_schema_id from BaseDataClass!
    derived_row_type = row_type.RowTypeConstraint.from_user_type(
        DerivedDataClass)
    self.assertIsNone(derived_row_type._schema_id)

    schema_for_derived = schemas.schema_from_element_type(DerivedDataClass)
    self.assertTrue(hasattr(DerivedDataClass, row_type._BEAM_SCHEMA_ID))
    self.assertEqual(
        schema_for_derived.id,
        getattr(DerivedDataClass, row_type._BEAM_SCHEMA_ID))
    self.assertNotEqual(schema_for_derived.id, schema_for_base.id)


if __name__ == '__main__':
  unittest.main()
