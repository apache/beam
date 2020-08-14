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
# pytype: skip-file

from __future__ import absolute_import

import logging
import typing
import unittest
from itertools import chain

import numpy as np
from past.builtins import unicode

import apache_beam as beam
from apache_beam.coders import RowCoder
from apache_beam.coders.typecoders import registry as coders_registry
from apache_beam.internal import pickler
from apache_beam.portability.api import schema_pb2
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.utils.timestamp import Timestamp

Person = typing.NamedTuple(
    "Person",
    [
        ("name", unicode),
        ("age", np.int32),
        ("address", typing.Optional[unicode]),
        ("aliases", typing.List[unicode]),
        ("knows_javascript", bool),
        # TODO(BEAM-7372): Use bytes instead of ByteString
        ("payload", typing.Optional[typing.ByteString]),
        ("custom_metadata", typing.Mapping[unicode, int]),
        ("favorite_time", Timestamp),
    ])

coders_registry.register_coder(Person, RowCoder)


class RowCoderTest(unittest.TestCase):
  JON_SNOW = Person(
      name="Jon Snow",
      age=23,
      address=None,
      aliases=["crow", "wildling"],
      knows_javascript=False,
      payload=None,
      custom_metadata={},
      favorite_time=Timestamp.from_rfc3339('2016-03-18T23:22:59.123Z'),
  )
  PEOPLE = [
      JON_SNOW,
      Person(
          "Daenerys Targaryen",
          25,
          "Westeros",
          ["Mother of Dragons"],
          False,
          None,
          {"dragons": 3},
          Timestamp.from_rfc3339('1970-04-26T17:46:40Z'),
      ),
      Person(
          "Michael Bluth",
          30,
          None, [],
          True,
          b"I've made a huge mistake", {},
          Timestamp.from_rfc3339('2020-08-12T15:51:00.032Z'))
  ]

  def test_create_row_coder_from_named_tuple(self):
    expected_coder = RowCoder(typing_to_runner_api(Person).row_type.schema)
    real_coder = coders_registry.get_coder(Person)

    for test_case in self.PEOPLE:
      self.assertEqual(
          expected_coder.encode(test_case), real_coder.encode(test_case))

      self.assertEqual(
          test_case, real_coder.decode(real_coder.encode(test_case)))

  def test_create_row_coder_from_schema(self):
    schema = schema_pb2.Schema(
        id="person",
        fields=[
            schema_pb2.Field(
                name="name",
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name="age",
                type=schema_pb2.FieldType(atomic_type=schema_pb2.INT32)),
            schema_pb2.Field(
                name="address",
                type=schema_pb2.FieldType(
                    atomic_type=schema_pb2.STRING, nullable=True)),
            schema_pb2.Field(
                name="aliases",
                type=schema_pb2.FieldType(
                    array_type=schema_pb2.ArrayType(
                        element_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING)))),
            schema_pb2.Field(
                name="knows_javascript",
                type=schema_pb2.FieldType(atomic_type=schema_pb2.BOOLEAN)),
            schema_pb2.Field(
                name="payload",
                type=schema_pb2.FieldType(
                    atomic_type=schema_pb2.BYTES, nullable=True)),
            schema_pb2.Field(
                name="custom_metadata",
                type=schema_pb2.FieldType(
                    map_type=schema_pb2.MapType(
                        key_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING),
                        value_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.INT64),
                    ))),
            schema_pb2.Field(
                name="favorite_time",
                type=schema_pb2.FieldType(
                    logical_type=schema_pb2.LogicalType(
                        urn="beam:logical_type:millis_instant:v1",
                        representation=schema_pb2.FieldType(
                            atomic_type=schema_pb2.INT64)))),
        ])
    coder = RowCoder(schema)

    for test_case in self.PEOPLE:
      self.assertEqual(test_case, coder.decode(coder.encode(test_case)))

  @unittest.skip(
      "BEAM-8030 - Overflow behavior in VarIntCoder is currently inconsistent")
  def test_overflows(self):
    IntTester = typing.NamedTuple(
        'IntTester',
        [
            # TODO(BEAM-7996): Test int8 and int16 here as well when those
            # types are supported
            # ('i8', typing.Optional[np.int8]),
            # ('i16', typing.Optional[np.int16]),
            ('i32', typing.Optional[np.int32]),
            ('i64', typing.Optional[np.int64]),
        ])

    c = RowCoder.from_type_hint(IntTester, None)

    no_overflow = chain(
        (IntTester(i32=i, i64=None) for i in (-2**31, 2**31 - 1)),
        (IntTester(i32=None, i64=i) for i in (-2**63, 2**63 - 1)),
    )

    # Encode max/min ints to make sure they don't throw any error
    for case in no_overflow:
      c.encode(case)

    overflow = chain(
        (IntTester(i32=i, i64=None) for i in (-2**31 - 1, 2**31)),
        (IntTester(i32=None, i64=i) for i in (-2**63 - 1, 2**63)),
    )

    # Encode max+1/min-1 ints to make sure they DO throw an error
    for case in overflow:
      self.assertRaises(OverflowError, lambda: c.encode(case))

  def test_none_in_non_nullable_field_throws(self):
    Test = typing.NamedTuple('Test', [('foo', unicode)])

    c = RowCoder.from_type_hint(Test, None)
    self.assertRaises(ValueError, lambda: c.encode(Test(foo=None)))

  def test_schema_remove_column(self):
    fields = [("field1", unicode), ("field2", unicode)]
    # new schema is missing one field that was in the old schema
    Old = typing.NamedTuple('Old', fields)
    New = typing.NamedTuple('New', fields[:-1])

    old_coder = RowCoder.from_type_hint(Old, None)
    new_coder = RowCoder.from_type_hint(New, None)

    self.assertEqual(
        New("foo"), new_coder.decode(old_coder.encode(Old("foo", "bar"))))

  def test_schema_add_column(self):
    fields = [("field1", unicode), ("field2", typing.Optional[unicode])]
    # new schema has one (optional) field that didn't exist in the old schema
    Old = typing.NamedTuple('Old', fields[:-1])
    New = typing.NamedTuple('New', fields)

    old_coder = RowCoder.from_type_hint(Old, None)
    new_coder = RowCoder.from_type_hint(New, None)

    self.assertEqual(
        New("bar", None), new_coder.decode(old_coder.encode(Old("bar"))))

  def test_schema_add_column_with_null_value(self):
    fields = [("field1", typing.Optional[unicode]), ("field2", unicode),
              ("field3", typing.Optional[unicode])]
    # new schema has one (optional) field that didn't exist in the old schema
    Old = typing.NamedTuple('Old', fields[:-1])
    New = typing.NamedTuple('New', fields)

    old_coder = RowCoder.from_type_hint(Old, None)
    new_coder = RowCoder.from_type_hint(New, None)

    self.assertEqual(
        New(None, "baz", None),
        new_coder.decode(old_coder.encode(Old(None, "baz"))))

  def test_row_coder_picklable(self):
    # occasionally coders can get pickled, RowCoder should be able to handle it
    coder = coders_registry.get_coder(Person)
    roundtripped = pickler.loads(pickler.dumps(coder))

    self.assertEqual(roundtripped, coder)

  def test_row_coder_in_pipeine(self):
    with TestPipeline() as p:
      res = (
          p
          | beam.Create(self.PEOPLE)
          | beam.Filter(lambda person: person.name == "Jon Snow"))
      assert_that(res, equal_to([self.JON_SNOW]))

  def test_row_coder_nested_struct(self):
    Pair = typing.NamedTuple('Pair', [('left', Person), ('right', Person)])

    value = Pair(self.PEOPLE[0], self.PEOPLE[1])
    coder = RowCoder(typing_to_runner_api(Pair).row_type.schema)

    self.assertEqual(value, coder.decode(coder.encode(value)))


if __name__ == "__main__":
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
