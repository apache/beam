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

"""Unit tests for BigQuery Storage Write API dynamic schemas."""

import unittest
from unittest import mock

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints.row_type import RowTypeConstraint
from apache_beam.typehints.typehints import Any

try:
  from google.api_core.exceptions import GoogleAPICallError
except ImportError:
  GoogleAPICallError = None


@unittest.skipIf(GoogleAPICallError is None, 'GCP dependencies are not installed')
@mock.patch('apache_beam.io.gcp.bigquery.BeamJarExpansionService')
class BigQueryStorageWriteDynamicSchemaTest(unittest.TestCase):
  """Test dynamic schema support in BigQuery Storage Write API."""
  def test_storage_write_init_with_schema_side_inputs(
      self, mock_expansion_service):
    """Test that StorageWriteToBigQuery accepts schema_side_inputs."""
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table',
        schema=lambda dest: None,
        schema_side_inputs=('side_input_1', ))
    self.assertEqual(transform._schema_side_inputs, ('side_input_1', ))
    self.assertEqual(transform._table_side_inputs, ())

  def test_convert_to_beam_rows_dynamic_destinations_dynamic_schema(
      self, mock_expansion_service):
    """Test ConvertToBeamRows with dynamic destinations and dynamic schema."""
    schema1 = {
        'fields': [
            {
                'name': 'id', 'type': 'INTEGER'
            },
            {
                'name': 'name', 'type': 'STRING'
            },
        ]
    }
    schema2 = {
        'fields': [
            {
                'name': 'id', 'type': 'INTEGER'
            },
            {
                'name': 'score', 'type': 'FLOAT'
            },
        ]
    }
    schema_map = {'table1': schema1, 'table2': schema2}

    converter = bigquery.StorageWriteToBigQuery.ConvertToBeamRows(
        schema=lambda dest: schema_map[dest], dynamic_destinations=True)

    with TestPipeline() as p:
      input_data = [
          ('table1', {
              'id': 1, 'name': 'foo'
          }),
          ('table2', {
              'id': 2, 'score': 3.14
          }),
      ]
      res = p | "CreateInput" >> beam.Create(input_data) | converter

      expected_rows = [
          beam.Row(destination='table1', record=beam.Row(id=1, name='foo')),
          beam.Row(destination='table2', record=beam.Row(id=2, score=3.14)),
      ]
      assert_that(res, equal_to(expected_rows))

  def test_convert_to_beam_rows_dynamic_destinations_with_side_inputs(
      self, mock_expansion_service):
    """Test ConvertToBeamRows with dynamic schema and side inputs."""
    schema1 = {
        'fields': [
            {
                'name': 'id', 'type': 'INTEGER'
            },
            {
                'name': 'name', 'type': 'STRING'
            },
        ]
    }
    schema2 = {
        'fields': [
            {
                'name': 'id', 'type': 'INTEGER'
            },
            {
                'name': 'score', 'type': 'FLOAT'
            },
        ]
    }

    with TestPipeline() as p:
      side_pcoll = (
          p
          | "CreateSide" >> beam.Create([{
              'table1': schema1, 'table2': schema2
          }]))

      converter = bigquery.StorageWriteToBigQuery.ConvertToBeamRows(
          schema=lambda dest, side_map: side_map[dest],
          dynamic_destinations=True,
          schema_side_inputs=(beam.pvalue.AsSingleton(side_pcoll), ))

      input_data = [
          ('table1', {
              'id': 1, 'name': 'foo'
          }),
          ('table2', {
              'id': 2, 'score': 3.14
          }),
      ]
      res = p | "CreateInput" >> beam.Create(input_data) | converter

      expected_rows = [
          beam.Row(destination='table1', record=beam.Row(id=1, name='foo')),
          beam.Row(destination='table2', record=beam.Row(id=2, score=3.14)),
      ]
      assert_that(res, equal_to(expected_rows))

  def test_storage_write_static_destination_dynamic_schema_raises_error(
      self, mock_expansion_service):
    """Test that static destination with dynamic schema raises ValueError."""
    transform = bigquery.StorageWriteToBigQuery(
        table='test-project:test_dataset.test_table', schema=lambda dest: None)
    with self.assertRaisesRegex(
        ValueError,
        "Writing with a dynamic schema is only supported when writing to "
        "dynamic destinations."):
      with TestPipeline() as p:
        _ = p | "CreateInput" >> beam.Create([{'id': 1}]) | transform

  def test_convert_to_beam_rows_with_output_types_dynamic_schema(
      self, mock_expansion_service):
    """Test with_output_types when schema is callable."""
    converter_dyn = bigquery.StorageWriteToBigQuery.ConvertToBeamRows(
        schema=lambda dest: None, dynamic_destinations=True)
    type_hint_dyn = converter_dyn.with_output_types().get_type_hints(
    ).simple_output_type('')
    self.assertIsInstance(type_hint_dyn, RowTypeConstraint)
    self.assertEqual(
        type_hint_dyn._fields,
        (
            (bigquery.StorageWriteToBigQuery.DESTINATION, str),
            (bigquery.StorageWriteToBigQuery.RECORD, Any),
        ))

  def test_storage_write_to_bigquery_expand_dynamic_schema(
      self, mock_expansion_service):
    """Test StorageWriteToBigQuery expand does not fail for callable schema."""
    schema1 = {
        'fields': [
            {
                'name': 'id', 'type': 'INTEGER'
            },
        ]
    }

    class _DummyExternalTransform(beam.PTransform):
      def expand(self, pcoll):
        return {
            bigquery.StorageWriteToBigQuery.FAILED_ROWS_WITH_ERRORS: (
                pcoll.pipeline | "CreateErrors" >> beam.Create([]))
        }

    with mock.patch.object(bigquery,
                           'SchemaAwareExternalTransform',
                           autospec=True) as mock_ext:
      mock_ext.return_value = _DummyExternalTransform()
      transform = bigquery.StorageWriteToBigQuery(
          table=lambda record: 'table1', schema=lambda dest: schema1)

      with TestPipeline() as p:
        _ = p | "CreateInput" >> beam.Create([{'id': 1}]) | transform

      mock_ext.assert_called_once()
      _, kwargs = mock_ext.call_args
      self.assertEqual(
          kwargs['table'], bigquery.StorageWriteToBigQuery.DYNAMIC_DESTINATIONS)

  def test_write_to_bigquery_storage_api_passes_schema_side_inputs(
      self, mock_expansion_service):
    """Test WriteToBigQuery passes schema_side_inputs to StorageWriteToBigQuery."""
    with mock.patch.object(bigquery, 'StorageWriteToBigQuery',
                           autospec=True) as mock_storage_write:
      mock_storage_write.return_value = beam.Map(lambda x: x)
      with TestPipeline() as p:
        side_pc = p | "CreateSide" >> beam.Create([1])
        write_transform = bigquery.WriteToBigQuery(
            table='proj:ds.table',
            method=bigquery.WriteToBigQuery.Method.STORAGE_WRITE_API,
            schema=lambda dest: None,
            schema_side_inputs=(beam.pvalue.AsSingleton(side_pc), ))
        _ = p | "CreateInput" >> beam.Create([{'id': 1}]) | write_transform

      mock_storage_write.assert_called_once()
      _, kwargs = mock_storage_write.call_args
      self.assertEqual(len(kwargs['schema_side_inputs']), 1)


if __name__ == '__main__':
  unittest.main()
