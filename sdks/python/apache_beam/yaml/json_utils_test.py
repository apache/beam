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

import unittest

import apache_beam as beam
from apache_beam.portability.api import schema_pb2
from apache_beam.yaml import json_utils


class JsonUtilsTest(unittest.TestCase):
  def test_json_to_row_with_missing_optional_field(self):
    beam_schema = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name='id',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name='event_subtype',
                type=schema_pb2.FieldType(
                    atomic_type=schema_pb2.STRING, nullable=True)),
        ])
    beam_type = schema_pb2.FieldType(
        row_type=schema_pb2.RowType(schema=beam_schema))
    converter = json_utils.json_to_row(beam_type)
    json_data = {'id': '123'}
    beam_row = converter(json_data)
    self.assertEqual(beam_row, beam.Row(id='123', event_subtype=None))

  def test_json_to_row_with_missing_optional_object(self):
    nested_schema = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name='nested_id',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
        ])
    beam_schema = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name='id',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name='nested',
                type=schema_pb2.FieldType(
                    row_type=schema_pb2.RowType(schema=nested_schema),
                    nullable=True)),
        ])
    beam_type = schema_pb2.FieldType(
        row_type=schema_pb2.RowType(schema=beam_schema))
    converter = json_utils.json_to_row(beam_type)
    json_data = {'id': '123'}
    beam_row = converter(json_data)
    self.assertEqual(beam_row, beam.Row(id='123', nested=None))

  def test_json_to_row_with_missing_optional_array(self):
    beam_schema = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name='id',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name='items',
                type=schema_pb2.FieldType(
                    array_type=schema_pb2.ArrayType(
                        element_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING)),
                    nullable=True)),
        ])
    beam_type = schema_pb2.FieldType(
        row_type=schema_pb2.RowType(schema=beam_schema))
    converter = json_utils.json_to_row(beam_type)
    json_data = {'id': '123'}
    beam_row = converter(json_data)
    self.assertEqual(beam_row, beam.Row(id='123', items=None))

  def test_json_to_row_with_all_fields(self):
    nested_schema = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name='nested_id',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
        ])
    beam_schema = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name='id',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name='event_subtype',
                type=schema_pb2.FieldType(
                    atomic_type=schema_pb2.STRING, nullable=True)),
            schema_pb2.Field(
                name='nested',
                type=schema_pb2.FieldType(
                    row_type=schema_pb2.RowType(schema=nested_schema),
                    nullable=True)),
            schema_pb2.Field(
                name='items',
                type=schema_pb2.FieldType(
                    array_type=schema_pb2.ArrayType(
                        element_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING)),
                    nullable=True)),
        ])
    beam_type = schema_pb2.FieldType(
        row_type=schema_pb2.RowType(schema=beam_schema))
    converter = json_utils.json_to_row(beam_type)
    json_data = {
        'id': '123',
        'event_subtype': 'subtype_val',
        'nested': {
            'nested_id': 'nested_123'
        },
        'items': ['a', 'b', 'c']
    }
    beam_row = converter(json_data)
    expected_row = beam.Row(
        id='123',
        event_subtype='subtype_val',
        nested=beam.Row(nested_id='nested_123'),
        items=['a', 'b', 'c'])
    self.assertEqual(beam_row, expected_row)

  def test_json_to_row_with_missing_required_field(self):
    beam_schema = schema_pb2.Schema(
        fields=[
            schema_pb2.Field(
                name='id',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name='required_field',
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
        ])
    beam_type = schema_pb2.FieldType(
        row_type=schema_pb2.RowType(schema=beam_schema))
    converter = json_utils.json_to_row(beam_type)
    json_data = {'id': '123'}
    with self.assertRaises(KeyError):
      converter(json_data)


if __name__ == '__main__':
  unittest.main()
