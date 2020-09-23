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

# pytype: skip-file

from __future__ import absolute_import

import itertools
import unittest
from typing import ByteString
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence

import numpy as np
from future.moves import pickle
from past.builtins import unicode

from apache_beam.portability.api import schema_pb2
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.typehints.schemas import typing_from_runner_api
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.utils.timestamp import Timestamp


class SchemaTest(unittest.TestCase):
  """ Tests for Runner API Schema proto to/from typing conversions

  There are two main tests: test_typing_survives_proto_roundtrip, and
  test_proto_survives_typing_roundtrip. These are both necessary because Schemas
  are cached by ID, so performing just one of them wouldn't necessarily exercise
  all code paths.
  """
  def test_typing_survives_proto_roundtrip(self):
    all_nonoptional_primitives = [
        np.int8,
        np.int16,
        np.int32,
        np.int64,
        np.float32,
        np.float64,
        unicode,
        bool,
        bytes,
        str,
    ]

    all_optional_primitives = [
        Optional[typ] for typ in all_nonoptional_primitives
    ]

    all_primitives = all_nonoptional_primitives + all_optional_primitives

    basic_array_types = [Sequence[typ] for typ in all_primitives]

    basic_map_types = [
        Mapping[key_type, value_type] for key_type,
        value_type in itertools.product(all_primitives, all_primitives)
    ]

    selected_schemas = [
        NamedTuple(
            'AllPrimitives',
            [('field%d' % i, typ) for i, typ in enumerate(all_primitives)]),
        NamedTuple(
            'ComplexSchema',
            [
                ('id', np.int64),
                ('name', unicode),
                (
                    'optional_map',
                    Optional[Mapping[unicode, Optional[np.float64]]]),
                ('optional_array', Optional[Sequence[np.float32]]),
                ('array_optional', Sequence[Optional[bool]]),
                ('timestamp', Timestamp),
            ])
    ]

    test_cases = all_primitives + \
                 basic_array_types + \
                 basic_map_types + \
                 selected_schemas

    for test_case in test_cases:
      self.assertEqual(
          test_case, typing_from_runner_api(typing_to_runner_api(test_case)))

  def test_proto_survives_typing_roundtrip(self):
    all_nonoptional_primitives = [
        schema_pb2.FieldType(atomic_type=typ)
        for typ in schema_pb2.AtomicType.values()
        if typ is not schema_pb2.UNSPECIFIED
    ]

    all_optional_primitives = [
        schema_pb2.FieldType(nullable=True, atomic_type=typ)
        for typ in schema_pb2.AtomicType.values()
        if typ is not schema_pb2.UNSPECIFIED
    ]

    all_primitives = all_nonoptional_primitives + all_optional_primitives

    basic_array_types = [
        schema_pb2.FieldType(array_type=schema_pb2.ArrayType(element_type=typ))
        for typ in all_primitives
    ]

    basic_map_types = [
        schema_pb2.FieldType(
            map_type=schema_pb2.MapType(
                key_type=key_type, value_type=value_type)) for key_type,
        value_type in itertools.product(all_primitives, all_primitives)
    ]

    selected_schemas = [
        schema_pb2.FieldType(
            row_type=schema_pb2.RowType(
                schema=schema_pb2.Schema(
                    id='32497414-85e8-46b7-9c90-9a9cc62fe390',
                    fields=[
                        schema_pb2.Field(name='field%d' % i, type=typ) for i,
                        typ in enumerate(all_primitives)
                    ]))),
        schema_pb2.FieldType(
            row_type=schema_pb2.RowType(
                schema=schema_pb2.Schema(
                    id='dead1637-3204-4bcb-acf8-99675f338600',
                    fields=[
                        schema_pb2.Field(
                            name='id',
                            type=schema_pb2.FieldType(
                                atomic_type=schema_pb2.INT64)),
                        schema_pb2.Field(
                            name='name',
                            type=schema_pb2.FieldType(
                                atomic_type=schema_pb2.STRING)),
                        schema_pb2.Field(
                            name='optional_map',
                            type=schema_pb2.FieldType(
                                nullable=True,
                                map_type=schema_pb2.MapType(
                                    key_type=schema_pb2.FieldType(
                                        atomic_type=schema_pb2.STRING),
                                    value_type=schema_pb2.FieldType(
                                        atomic_type=schema_pb2.DOUBLE)))),
                        schema_pb2.Field(
                            name='optional_array',
                            type=schema_pb2.FieldType(
                                nullable=True,
                                array_type=schema_pb2.ArrayType(
                                    element_type=schema_pb2.FieldType(
                                        atomic_type=schema_pb2.FLOAT)))),
                        schema_pb2.Field(
                            name='array_optional',
                            type=schema_pb2.FieldType(
                                array_type=schema_pb2.ArrayType(
                                    element_type=schema_pb2.FieldType(
                                        nullable=True,
                                        atomic_type=schema_pb2.BYTES)))),
                    ]))),
    ]

    test_cases = all_primitives + \
                 basic_array_types + \
                 basic_map_types + \
                 selected_schemas

    for test_case in test_cases:
      self.assertEqual(
          test_case, typing_to_runner_api(typing_from_runner_api(test_case)))

  def test_unknown_primitive_raise_valueerror(self):
    self.assertRaises(ValueError, lambda: typing_to_runner_api(np.uint32))

  def test_unknown_atomic_raise_valueerror(self):
    self.assertRaises(
        ValueError,
        lambda: typing_from_runner_api(
            schema_pb2.FieldType(atomic_type=schema_pb2.UNSPECIFIED)))

  def test_int_maps_to_int64(self):
    self.assertEqual(
        schema_pb2.FieldType(atomic_type=schema_pb2.INT64),
        typing_to_runner_api(int))

  def test_float_maps_to_float64(self):
    self.assertEqual(
        schema_pb2.FieldType(atomic_type=schema_pb2.DOUBLE),
        typing_to_runner_api(float))

  def test_trivial_example(self):
    MyCuteClass = NamedTuple(
        'MyCuteClass',
        [
            ('name', unicode),
            ('age', Optional[int]),
            ('interests', List[unicode]),
            ('height', float),
            ('blob', ByteString),
        ])

    expected = schema_pb2.FieldType(
        row_type=schema_pb2.RowType(
            schema=schema_pb2.Schema(
                fields=[
                    schema_pb2.Field(
                        name='name',
                        type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING),
                    ),
                    schema_pb2.Field(
                        name='age',
                        type=schema_pb2.FieldType(
                            nullable=True, atomic_type=schema_pb2.INT64)),
                    schema_pb2.Field(
                        name='interests',
                        type=schema_pb2.FieldType(
                            array_type=schema_pb2.ArrayType(
                                element_type=schema_pb2.FieldType(
                                    atomic_type=schema_pb2.STRING)))),
                    schema_pb2.Field(
                        name='height',
                        type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.DOUBLE)),
                    schema_pb2.Field(
                        name='blob',
                        type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.BYTES)),
                ])))

    # Only test that the fields are equal. If we attempt to test the entire type
    # or the entire schema, the generated id will break equality.
    self.assertEqual(
        expected.row_type.schema.fields,
        typing_to_runner_api(MyCuteClass).row_type.schema.fields)

  def test_generated_class_pickle(self):
    schema = schema_pb2.Schema(
        id="some-uuid",
        fields=[
            schema_pb2.Field(
                name='name',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING),
            )
        ])
    user_type = named_tuple_from_schema(schema)
    instance = user_type(name="test")

    self.assertEqual(instance, pickle.loads(pickle.dumps(instance)))

  def test_user_type_annotated_with_id_after_conversion(self):
    MyCuteClass = NamedTuple('MyCuteClass', [
        ('name', unicode),
    ])
    self.assertFalse(hasattr(MyCuteClass, '_beam_schema_id'))

    schema = named_tuple_to_schema(MyCuteClass)
    self.assertTrue(hasattr(MyCuteClass, '_beam_schema_id'))
    self.assertEqual(MyCuteClass._beam_schema_id, schema.id)


if __name__ == '__main__':
  unittest.main()
