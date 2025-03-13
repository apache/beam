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

import itertools
import pickle
import unittest
from typing import Any
from typing import ByteString
from typing import List
from typing import Mapping
from typing import NamedTuple
from typing import Optional
from typing import Sequence

import cloudpickle
import dill
import numpy as np
from hypothesis import given
from hypothesis import settings
from parameterized import parameterized
from parameterized import parameterized_class

from apache_beam.portability import common_urns
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import row_type
from apache_beam.typehints import typehints
from apache_beam.typehints.native_type_compatibility import match_is_named_tuple
from apache_beam.typehints.schemas import SchemaTypeRegistry
from apache_beam.typehints.schemas import named_fields_from_element_type
from apache_beam.typehints.schemas import named_tuple_from_schema
from apache_beam.typehints.schemas import named_tuple_to_schema
from apache_beam.typehints.schemas import typing_from_runner_api
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.typehints.testing.strategies import named_fields
from apache_beam.utils.timestamp import Timestamp

all_nonoptional_primitives = [
    np.int8,
    np.int16,
    np.int32,
    np.int64,
    np.float32,
    np.float64,
    bool,
    bytes,
    str,
]

all_optional_primitives = [Optional[typ] for typ in all_nonoptional_primitives]

all_primitives = all_nonoptional_primitives + all_optional_primitives

basic_array_types = [Sequence[typ] for typ in all_primitives]

basic_map_types = [
    Mapping[key_type, value_type] for key_type,
    value_type in itertools.product(all_primitives, all_primitives)
]


class AllPrimitives(NamedTuple):
  field_int8: np.int8
  field_int16: np.int16
  field_int32: np.int32
  field_int64: np.int64
  field_float32: np.float32
  field_float64: np.float64
  field_bool: bool
  field_bytes: bytes
  field_str: str
  field_optional_int8: Optional[np.int8]
  field_optional_int16: Optional[np.int16]
  field_optional_int32: Optional[np.int32]
  field_optional_int64: Optional[np.int64]
  field_optional_float32: Optional[np.float32]
  field_optional_float64: Optional[np.float64]
  field_optional_bool: Optional[bool]
  field_optional_bytes: Optional[bytes]
  field_optional_str: Optional[str]


class ComplexSchema(NamedTuple):
  id: np.int64
  name: str
  optional_map: Optional[Mapping[str, Optional[np.float64]]]
  optional_array: Optional[Sequence[np.float32]]
  array_optional: Sequence[Optional[bool]]
  timestamp: Timestamp


def get_test_beam_fieldtype_protos():
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
          map_type=schema_pb2.MapType(key_type=key_type, value_type=value_type))
      for key_type,
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
      schema_pb2.FieldType(
          row_type=schema_pb2.RowType(
              schema=schema_pb2.Schema(
                  id='a-schema-with-options',
                  fields=[
                      schema_pb2.Field(name='field%d' % i, type=typ) for i,
                      typ in enumerate(all_primitives)
                  ],
                  options=[
                      schema_pb2.Option(name='a_flag'),
                      schema_pb2.Option(
                          name='a_byte',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.BYTE),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  byte=127))),
                      schema_pb2.Option(
                          name='a_int16',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.INT16),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  int16=255))),
                      schema_pb2.Option(
                          name='a_int32',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.INT32),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  int32=255))),
                      schema_pb2.Option(
                          name='a_int64',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.INT64),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  int64=255))),
                      schema_pb2.Option(
                          name='a_float',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.FLOAT),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  float=3.14))),
                      schema_pb2.Option(
                          name='a_double',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.DOUBLE),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  double=2.718))),
                      schema_pb2.Option(
                          name='a_str',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.STRING),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  string='str'))),
                      schema_pb2.Option(
                          name='a_bool',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.BOOLEAN),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  boolean=True))),
                      schema_pb2.Option(
                          name='a_bytes',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.BYTES),
                          value=schema_pb2.FieldValue(
                              atomic_value=schema_pb2.AtomicTypeValue(
                                  bytes=b'bytes!'))),
                  ]))),
      schema_pb2.FieldType(
          row_type=schema_pb2.RowType(
              schema=schema_pb2.Schema(
                  id='a-schema-with-field-options',
                  fields=[
                      schema_pb2.Field(
                          name='field%d' % i,
                          type=typ,
                          options=[
                              schema_pb2.Option(name='a_flag'),
                              schema_pb2.Option(
                                  name='a_str',
                                  type=schema_pb2.FieldType(
                                      atomic_type=schema_pb2.STRING),
                                  value=schema_pb2.FieldValue(
                                      atomic_value=schema_pb2.AtomicTypeValue(
                                          string='str'))),
                          ]) for i,
                      typ in enumerate(all_primitives)
                  ] + [
                      schema_pb2.Field(
                          name='nested',
                          type=schema_pb2.FieldType(
                              row_type=schema_pb2.RowType(
                                  schema=schema_pb2.Schema(
                                      fields=[
                                          schema_pb2.Field(
                                              name='nested_field',
                                              type=schema_pb2.FieldType(
                                                  atomic_type=schema_pb2.INT64,
                                              ),
                                              options=[
                                                  schema_pb2.Option(
                                                      name='a_nested_field_flag'
                                                  ),
                                              ]),
                                      ],
                                      options=[
                                          schema_pb2.Option(
                                              name='a_nested_schema_flag'),
                                          schema_pb2.Option(
                                              name='a_str',
                                              type=schema_pb2.FieldType(
                                                  atomic_type=schema_pb2.STRING
                                              ),
                                              value=schema_pb2.FieldValue(
                                                  atomic_value=schema_pb2.
                                                  AtomicTypeValue(
                                                      string='str'))),
                                      ],
                                  ))),
                      ),
                  ]))),
      schema_pb2.FieldType(
          row_type=schema_pb2.RowType(
              schema=schema_pb2.Schema(
                  id='a-schema-with-optional-nested-struct',
                  fields=[
                      schema_pb2.Field(
                          name='id',
                          type=schema_pb2.FieldType(
                              atomic_type=schema_pb2.INT64)),
                      schema_pb2.Field(
                          name='nested_row',
                          type=schema_pb2.FieldType(
                              nullable=True,
                              row_type=schema_pb2.RowType(
                                  schema=schema_pb2.Schema(
                                      id='the-nested-schema',
                                      fields=[
                                          schema_pb2.Field(
                                              name='name',
                                              type=schema_pb2.FieldType(
                                                  atomic_type=schema_pb2.STRING)
                                          ),
                                          schema_pb2.Field(
                                              name='optional_map',
                                              type=schema_pb2.FieldType(
                                                  nullable=True,
                                                  map_type=schema_pb2.MapType(
                                                      key_type=schema_pb2.
                                                      FieldType(
                                                          atomic_type=schema_pb2
                                                          .STRING),
                                                      value_type=schema_pb2.
                                                      FieldType(
                                                          atomic_type=schema_pb2
                                                          .DOUBLE)))),
                                      ]))))
                  ]))),
  ]

  return all_primitives + \
      basic_array_types + \
      basic_map_types + \
      selected_schemas


def get_test_beam_schemas_protos():
  return [
      fieldtype.row_type.schema
      for fieldtype in get_test_beam_fieldtype_protos()
      if fieldtype.WhichOneof('type_info') == 'row_type'
  ]


class SchemaTest(unittest.TestCase):
  """ Tests for Runner API Schema proto to/from typing conversions

  There are two main tests: test_typing_survives_proto_roundtrip, and
  test_proto_survives_typing_roundtrip. These are both necessary because Schemas
  are cached by ID, so performing just one of them wouldn't necessarily exercise
  all code paths.
  """
  @parameterized.expand([(user_type,) for user_type in
      all_primitives + \
      basic_array_types + \
      basic_map_types]
                        )
  def test_typing_survives_proto_roundtrip(self, user_type):
    self.assertEqual(
        user_type,
        typing_from_runner_api(
            typing_to_runner_api(
                user_type, schema_registry=SchemaTypeRegistry()),
            schema_registry=SchemaTypeRegistry()))

  @parameterized.expand([(AllPrimitives, ), (ComplexSchema, )])
  def test_namedtuple_roundtrip(self, user_type):
    roundtripped = typing_from_runner_api(
        typing_to_runner_api(user_type, schema_registry=SchemaTypeRegistry()),
        schema_registry=SchemaTypeRegistry())

    self.assertIsInstance(roundtripped, row_type.RowTypeConstraint)
    self.assert_namedtuple_equivalent(roundtripped.user_type, user_type)

  def test_row_type_constraint_to_schema(self):
    result_type = typing_to_runner_api(
        row_type.RowTypeConstraint.from_fields([
            ('foo', np.int8),
            ('bar', float),
            ('baz', bytes),
        ]))

    self.assertIsInstance(result_type, schema_pb2.FieldType)
    self.assertEqual(result_type.WhichOneof("type_info"), "row_type")

    schema = result_type.row_type.schema

    self.assertIsNotNone(schema.id)
    expected = [
        schema_pb2.Field(
            name='foo', type=schema_pb2.FieldType(atomic_type=schema_pb2.BYTE)),
        schema_pb2.Field(
            name='bar',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.DOUBLE)),
        schema_pb2.Field(
            name='baz',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.BYTES)),
    ]
    self.assertEqual(list(schema.fields), expected)

  def test_row_type_constraint_to_schema_with_options(self):
    row_type_with_options = row_type.RowTypeConstraint.from_fields(
        [
            ('foo', np.int8),
            ('bar', float),
            ('baz', bytes),
        ],
        schema_options=[
            ('some_metadata', 'foo'),
            ('some_other_metadata', 'baz'),
            ('some_metadata', 'bar'),
            ('an_integer_option', np.int32(123456)),
        ])
    result_type = typing_to_runner_api(row_type_with_options)

    self.assertIsInstance(result_type, schema_pb2.FieldType)
    self.assertEqual(result_type.WhichOneof("type_info"), "row_type")

    schema = result_type.row_type.schema

    expected = [
        schema_pb2.Option(
            name='some_metadata',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING),
            value=schema_pb2.FieldValue(
                atomic_value=schema_pb2.AtomicTypeValue(string='foo'))),
        schema_pb2.Option(
            name='some_other_metadata',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING),
            value=schema_pb2.FieldValue(
                atomic_value=schema_pb2.AtomicTypeValue(string='baz'))),
        schema_pb2.Option(
            name='some_metadata',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING),
            value=schema_pb2.FieldValue(
                atomic_value=schema_pb2.AtomicTypeValue(string='bar'))),
        schema_pb2.Option(
            name='an_integer_option',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.INT32),
            value=schema_pb2.FieldValue(
                atomic_value=schema_pb2.AtomicTypeValue(int32=123456))),
    ]
    self.assertEqual(list(schema.options), expected)

  def test_row_type_constraint_to_schema_with_field_options(self):
    result_type = typing_to_runner_api(
        row_type.RowTypeConstraint.from_fields([
            ('foo', np.int8),
            ('bar', float),
            ('baz', bytes),
        ],
                                               field_options={
                                                   'foo': [
                                                       ('some_metadata', 123),
                                                       ('some_flag', None)
                                                   ]
                                               }))

    self.assertIsInstance(result_type, schema_pb2.FieldType)
    self.assertEqual(result_type.WhichOneof("type_info"), "row_type")

    field = result_type.row_type.schema.fields[0]

    expected = [
        schema_pb2.Option(
            name='some_metadata',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.INT64),
            value=schema_pb2.FieldValue(
                atomic_value=schema_pb2.AtomicTypeValue(int64=123)),
        ),
        schema_pb2.Option(name='some_flag')
    ]
    self.assertEqual(list(field.options), expected)

  def test_row_type_constraint_to_schema_with_field_descriptions(self):
    row_type_with_options = row_type.RowTypeConstraint.from_fields(
        [
            ('foo', np.int8),
            ('bar', float),
            ('baz', bytes),
        ],
        field_descriptions={
            'foo': 'foo description',
            'bar': 'bar description',
            'baz': 'baz description',
        })
    result_type = typing_to_runner_api(row_type_with_options)

    self.assertIsInstance(result_type, schema_pb2.FieldType)
    self.assertEqual(result_type.WhichOneof("type_info"), "row_type")

    fields = result_type.row_type.schema.fields

    expected = [
        schema_pb2.Field(
            name='foo',
            description='foo description',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.BYTE),
        ),
        schema_pb2.Field(
            name='bar',
            description='bar description',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.DOUBLE),
        ),
        schema_pb2.Field(
            name='baz',
            description='baz description',
            type=schema_pb2.FieldType(atomic_type=schema_pb2.BYTES),
        ),
    ]
    self.assertEqual(list(fields), expected)

  def assert_namedtuple_equivalent(self, actual, expected):
    # Two types are only considered equal if they are literally the same
    # object (i.e. `actual == expected` is the same as `actual is expected` in
    # this case).
    # That's a much stricter check than we need, and it's necessarily not true
    # if types are pickled/unpickled. Here we just verify the features of the
    # types that actually matter to us.

    self.assertTrue(match_is_named_tuple(expected))
    self.assertTrue(match_is_named_tuple(actual))

    # TODO(https://github.com/apache/beam/issues/22082): This will break for
    # nested complex types.
    self.assertEqual(actual.__annotations__, expected.__annotations__)

    # TODO(https://github.com/apache/beam/issues/22082): Serialize user_type and
    # re-hydrate with sdk_options to make these checks pass.
    #self.assertEqual(dir(actual), dir(expected))
    #
    #for attr in dir(expected):
    #  self.assertEqual(getattr(actual, attr), getattr(expected, attr))

  @parameterized.expand([
      (fieldtype_proto, )
      for fieldtype_proto in get_test_beam_fieldtype_protos()
  ])
  def test_proto_survives_typing_roundtrip(self, fieldtype_proto):
    self.assertEqual(
        fieldtype_proto,
        typing_to_runner_api(
            typing_from_runner_api(
                fieldtype_proto, schema_registry=SchemaTypeRegistry()),
            schema_registry=SchemaTypeRegistry()))

  def test_unknown_primitive_maps_to_any(self):
    self.assertEqual(
        typing_to_runner_api(np.uint32),
        schema_pb2.FieldType(
            logical_type=schema_pb2.LogicalType(
                urn="beam:logical:pythonsdk_any:v1"),
            nullable=True))

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

  def test_python_callable_maps_to_logical_type(self):
    from apache_beam.utils.python_callable import PythonCallableWithSource
    self.assertEqual(
        schema_pb2.FieldType(
            logical_type=schema_pb2.LogicalType(
                urn=common_urns.python_callable.urn,
                representation=typing_to_runner_api(str))),
        typing_to_runner_api(PythonCallableWithSource))
    self.assertEqual(
        typing_from_runner_api(
            schema_pb2.FieldType(
                logical_type=schema_pb2.LogicalType(
                    urn=common_urns.python_callable.urn,
                    representation=typing_to_runner_api(str))),
            schema_registry=SchemaTypeRegistry()),
        PythonCallableWithSource)

  def test_trivial_example(self):
    MyCuteClass = NamedTuple(
        'MyCuteClass',
        [
            ('name', str),
            ('age', Optional[int]),
            ('interests', List[str]),
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

  def test_user_type_annotated_with_id_after_conversion(self):
    MyCuteClass = NamedTuple('MyCuteClass', [
        ('name', str),
    ])
    self.assertFalse(hasattr(MyCuteClass, '_beam_schema_id'))

    schema = named_tuple_to_schema(MyCuteClass)
    self.assertTrue(hasattr(MyCuteClass, '_beam_schema_id'))
    self.assertEqual(MyCuteClass._beam_schema_id, schema.id)

  def test_schema_with_bad_field_raises_helpful_error(self):
    schema_proto = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name="type_with_no_typeinfo", type=schema_pb2.FieldType())
        ],
        id="helpful-error-uuid",
    )

    # Should raise an exception referencing the problem field
    self.assertRaisesRegex(
        ValueError,
        "type_with_no_typeinfo",
        lambda: named_tuple_from_schema(
            schema_proto,
            # bypass schema cache
            schema_registry=SchemaTypeRegistry()))

  def test_row_type_is_callable(self):
    simple_row_type = row_type.RowTypeConstraint.from_fields([('foo', np.int64),
                                                              ('bar', str)])
    instance = simple_row_type(np.int64(35), 'baz')
    self.assertIsInstance(instance, simple_row_type.user_type)
    self.assertEqual(instance, (np.int64(35), 'baz'))

  def test_union(self):
    with_int = row_type.RowTypeConstraint.from_fields([('common', str),
                                                       ('unique', int)])
    with_any = row_type.RowTypeConstraint.from_fields([('common', str),
                                                       ('unique', Any)])
    union_type = typehints.Union[with_int, with_any]
    self.assertEqual(
        named_fields_from_element_type(union_type), [('common', str),
                                                     ('unique', Any)])


class HypothesisTest(unittest.TestCase):
  # There is considerable variablility in runtime for this test, disable
  # deadline.
  @settings(deadline=None)
  @given(named_fields())
  def test_named_fields_roundtrip(self, named_fields):
    typehint = row_type.RowTypeConstraint.from_fields(named_fields)
    roundtripped = typing_from_runner_api(
        typing_to_runner_api(typehint, schema_registry=SchemaTypeRegistry()),
        schema_registry=SchemaTypeRegistry())

    self.assertEqual(typehint, roundtripped)


@parameterized_class([
    {
        'pickler': pickle,
    },
    {
        'pickler': dill,
    },
    {
        'pickler': cloudpickle,
    },
])
class PickleTest(unittest.TestCase):
  def test_generated_class_pickle_instance(self):
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

    self.assertEqual(instance, self.pickler.loads(self.pickler.dumps(instance)))

  def test_generated_class_pickle(self):
    if self.pickler in [pickle, dill]:
      self.skipTest('https://github.com/apache/beam/issues/22714')

    schema = schema_pb2.Schema(
        id="some-uuid",
        fields=[
            schema_pb2.Field(
                name='name',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING),
            )
        ])
    user_type = named_tuple_from_schema(schema)

    self.assertEqual(
        user_type, self.pickler.loads(self.pickler.dumps(user_type)))

  def test_generated_class_row_type_pickle(self):
    row_proto = schema_pb2.FieldType(
        row_type=schema_pb2.RowType(
            schema=schema_pb2.Schema(
                id="some-other-uuid",
                fields=[
                    schema_pb2.Field(
                        name='name',
                        type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING),
                    )
                ])))
    row_type_constraint = typing_from_runner_api(
        row_proto, schema_registry=SchemaTypeRegistry())

    self.assertIsInstance(row_type_constraint, row_type.RowTypeConstraint)

    self.assertEqual(
        row_type_constraint,
        self.pickler.loads(self.pickler.dumps(row_type_constraint)))


if __name__ == '__main__':
  unittest.main()
