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

"""Utilities for converting between JSON and Beam Schema'd data.

For internal use, no backward compatibility guarantees.
"""

import json
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional

import jsonschema

import apache_beam as beam
from apache_beam.portability.api import schema_pb2
from apache_beam.typehints import schemas

JSON_ATOMIC_TYPES_TO_BEAM = {
    'boolean': schema_pb2.BOOLEAN,
    'integer': schema_pb2.INT64,
    'number': schema_pb2.DOUBLE,
    'string': schema_pb2.STRING,
}

BEAM_ATOMIC_TYPES_TO_JSON = {
    schema_pb2.INT16: 'integer',
    schema_pb2.INT32: 'integer',
    schema_pb2.FLOAT: 'number',
    **{v: k
       for k, v in JSON_ATOMIC_TYPES_TO_BEAM.items()}
}


def json_schema_to_beam_schema(
    json_schema: Dict[str, Any]) -> schema_pb2.Schema:
  """Returns a Beam schema equivalent for the given Json schema."""
  def maybe_nullable(beam_type, nullable):
    if nullable:
      beam_type.nullable = True
    return beam_type

  json_type = json_schema.get('type', None)
  if json_type != 'object':
    raise ValueError('Expected object type, got {json_type}.')
  if 'properties' not in json_schema:
    # Technically this is a valid (vacuous) schema, but as it's not generally
    # meaningful, throw an informative error instead.
    # (We could add a flag to allow this degenerate case.)
    raise ValueError('Missing properties for {json_schema}.')
  required = set(json_schema.get('required', []))
  return schema_pb2.Schema(
      fields=[
          schemas.schema_field(
              name,
              maybe_nullable(json_type_to_beam_type(t), name not in required),
              description=t.get('description') if isinstance(t, dict) else None)
          for (name, t) in json_schema['properties'].items()
      ])


def json_type_to_beam_type(json_type: Dict[str, Any]) -> schema_pb2.FieldType:
  """Returns a Beam schema type for the given Json (schema) type."""
  if not isinstance(json_type, dict) or 'type' not in json_type:
    raise ValueError(f'Malformed type {json_type}.')
  type_name = json_type['type']
  if type_name in JSON_ATOMIC_TYPES_TO_BEAM:
    return schema_pb2.FieldType(
        atomic_type=JSON_ATOMIC_TYPES_TO_BEAM[type_name])
  elif type_name == 'array':
    return schema_pb2.FieldType(
        array_type=schema_pb2.ArrayType(
            element_type=json_type_to_beam_type(json_type['items'])))
  elif type_name == 'object':
    if 'properties' in json_type:
      return schema_pb2.FieldType(
          row_type=schema_pb2.RowType(
              schema=json_schema_to_beam_schema(json_type)))
    elif 'additionalProperties' in json_type:
      return schema_pb2.FieldType(
          map_type=schema_pb2.MapType(
              key_type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING),
              value_type=json_type_to_beam_type(
                  json_type['additionalProperties'])))
    else:
      raise ValueError(
          f'Object type must have either properties or additionalProperties, '
          f'got {json_type}.')
  else:
    raise ValueError(f'Unable to convert {json_type} to a Beam schema.')


def beam_type_to_json_type(beam_type: schema_pb2.FieldType) -> Dict[str, Any]:
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    if beam_type.atomic_type in BEAM_ATOMIC_TYPES_TO_JSON:
      return {'type': BEAM_ATOMIC_TYPES_TO_JSON[beam_type.atomic_type]}
    else:
      return {}
  elif type_info == "array_type":
    return {
        'type': 'array',
        'items': beam_type_to_json_type(beam_type.array_type.element_type)
    }
  elif type_info == "iterable_type":
    return {
        'type': 'array',
        'items': beam_type_to_json_type(beam_type.iterable_type.element_type)
    }
  elif type_info == "map_type":
    return {
        'type': 'object',
        'properties': {
            '__line__': {
                'type': 'integer'
            }, '__uuid__': {}
        },
        'additionalProperties': beam_type_to_json_type(
            beam_type.map_type.value_type)
    }
  elif type_info == "row_type":
    return {
        'type': 'object',
        'properties': {
            field.name: beam_type_to_json_type(field.type)
            for field in beam_type.row_type.schema.fields
        },
        'additionalProperties': False
    }
  else:
    return {}


def json_to_row(beam_type: schema_pb2.FieldType) -> Callable[[Any], Any]:
  """Returns a callable converting Json objects to Beam rows of the given type.

  The input to the returned callable is expected to conform to the Json schema
  corresponding to this Beam type.
  """
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    return lambda value: value
  elif type_info == "array_type":
    element_converter = json_to_row(beam_type.array_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "iterable_type":
    element_converter = json_to_row(beam_type.iterable_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "map_type":
    if beam_type.map_type.key_type.atomic_type != schema_pb2.STRING:
      raise TypeError(
          f'Only strings allowd as map keys when converting from JSON, '
          f'found {beam_type}')
    value_converter = json_to_row(beam_type.map_type.value_type)
    return lambda value: {k: value_converter(v) for (k, v) in value.items()}
  elif type_info == "row_type":
    converters = {
        field.name: json_to_row(field.type)
        for field in beam_type.row_type.schema.fields
    }
    return lambda value: beam.Row(
        **
        {name: convert(value[name])
         for (name, convert) in converters.items()})
  elif type_info == "logical_type":
    return lambda value: value
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")


def json_parser(
    beam_schema: schema_pb2.Schema,
    json_schema: Optional[Dict[str,
                               Any]] = None) -> Callable[[bytes], beam.Row]:
  """Returns a callable converting Json strings to Beam rows of the given type.

  The input to the returned callable is expected to conform to the Json schema
  corresponding to this Beam type.
  """
  if json_schema is None:
    validate_fn = None
  else:
    cls = jsonschema.validators.validator_for(json_schema)
    cls.check_schema(json_schema)
    validate_fn = _PicklableFromConstructor(
        lambda: jsonschema.validators.validator_for(json_schema)
        (json_schema).validate)

  to_row = json_to_row(
      schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=beam_schema)))

  def parse(s: bytes):
    o = json.loads(s)
    if validate_fn is not None:
      validate_fn(o)
    return to_row(o)

  return parse


class _PicklableFromConstructor:
  def __init__(self, constructor):
    self._constructor = constructor
    self._value = None

  def __call__(self, o):
    if self._value is None:
      self._value = self._constructor()
    return self._value(o)

  def __getstate__(self):
    return {'_constructor': self._constructor, '_value': None}


def row_to_json(beam_type: schema_pb2.FieldType) -> Callable[[Any], Any]:
  """Returns a callable converting rows of the given type to Json objects."""
  type_info = beam_type.WhichOneof("type_info")
  if type_info == "atomic_type":
    return lambda value: value
  elif type_info == "array_type":
    element_converter = row_to_json(beam_type.array_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "iterable_type":
    element_converter = row_to_json(beam_type.iterable_type.element_type)
    return lambda value: [element_converter(e) for e in value]
  elif type_info == "map_type":
    if beam_type.map_type.key_type.atomic_type != schema_pb2.STRING:
      raise TypeError(
          f'Only strings allowd as map keys when converting to JSON, '
          f'found {beam_type}')
    value_converter = row_to_json(beam_type.map_type.value_type)
    return lambda value: {k: value_converter(v) for (k, v) in value.items()}
  elif type_info == "row_type":
    converters = {
        field.name: row_to_json(field.type)
        for field in beam_type.row_type.schema.fields
    }
    return lambda row: {
        name: convert(getattr(row, name))
        for (name, convert) in converters.items()
    }
  elif type_info == "logical_type":
    return lambda value: value
  else:
    raise ValueError(f"Unrecognized type_info: {type_info!r}")


def json_formater(
    beam_schema: schema_pb2.Schema) -> Callable[[beam.Row], bytes]:
  """Returns a callable converting rows of the given schema to Json strings."""
  convert = row_to_json(
      schema_pb2.FieldType(row_type=schema_pb2.RowType(schema=beam_schema)))
  return lambda row: json.dumps(convert(row), sort_keys=True).encode('utf-8')
