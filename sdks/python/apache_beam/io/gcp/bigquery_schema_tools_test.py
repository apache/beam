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

import logging
import typing
import unittest.mock

import mock
import numpy as np

import apache_beam as beam
import apache_beam.io.gcp.bigquery
from apache_beam.io.gcp import bigquery_schema_tools
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options import value_provider

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQueryToSchema(unittest.TestCase):
  def test_check_schema_conversions(self):
    fields = [
        bigquery.TableFieldSchema(name='stn', type='STRING', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode=None)
    ]
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.generate_user_type_from_bq_schema(
        the_table_schema=schema)
    self.assertEqual(
        usertype.__annotations__,
        {
            'stn': typing.Optional[str],
            'temp': typing.Sequence[np.float64],
            'count': typing.Optional[np.int64]
        })

  def test_check_conversion_with_selected_fields(self):
    fields = [
        bigquery.TableFieldSchema(name='stn', type='STRING', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode=None)
    ]
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.generate_user_type_from_bq_schema(
        the_table_schema=schema, selected_fields=['stn', 'count'])
    self.assertEqual(
        usertype.__annotations__, {
            'stn': typing.Optional[str], 'count': typing.Optional[np.int64]
        })

  def test_check_conversion_with_empty_schema(self):
    fields = []
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.generate_user_type_from_bq_schema(
        the_table_schema=schema)
    self.assertEqual(usertype.__annotations__, {})

  def test_check_schema_conversions_with_timestamp(self):
    fields = [
        bigquery.TableFieldSchema(name='stn', type='STRING', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(
            name='times', type='TIMESTAMP', mode="NULLABLE")
    ]
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.generate_user_type_from_bq_schema(
        the_table_schema=schema)
    self.assertEqual(
        usertype.__annotations__,
        {
            'stn': typing.Optional[str],
            'temp': typing.Sequence[np.float64],
            'times': typing.Optional[apache_beam.utils.timestamp.Timestamp]
        })

  def test_unsupported_type(self):
    fields = [
        bigquery.TableFieldSchema(
            name='number', type='DOUBLE', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode=None)
    ]
    schema = bigquery.TableSchema(fields=fields)
    with self.assertRaisesRegex(ValueError,
                                "Encountered an unsupported type: 'DOUBLE'"):
      bigquery_schema_tools.generate_user_type_from_bq_schema(
          the_table_schema=schema)

  def test_unsupported_mode(self):
    fields = [
        bigquery.TableFieldSchema(name='number', type='INTEGER', mode="NESTED"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode=None)
    ]
    schema = bigquery.TableSchema(fields=fields)
    with self.assertRaisesRegex(ValueError,
                                "Encountered an unsupported mode: 'NESTED'"):
      bigquery_schema_tools.generate_user_type_from_bq_schema(
          the_table_schema=schema)

  @mock.patch.object(BigQueryWrapper, 'get_table')
  def test_bad_schema_public_api_export(self, get_table):
    fields = [
        bigquery.TableFieldSchema(name='stn', type='DOUBLE', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode=None)
    ]
    schema = bigquery.TableSchema(fields=fields)
    table = apache_beam.io.gcp.internal.clients.bigquery.\
        bigquery_v2_messages.Table(
        schema=schema)
    get_table.return_value = table

    with self.assertRaisesRegex(ValueError,
                                "Encountered an unsupported type: 'DOUBLE'"):
      p = apache_beam.Pipeline()
      _ = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="dataset.sample_table",
          method="EXPORT",
          project="project",
          output_type='BEAM_ROW')

  @mock.patch.object(BigQueryWrapper, 'get_table')
  def test_bad_schema_public_api_direct_read(self, get_table):
    fields = [
        bigquery.TableFieldSchema(name='stn', type='DOUBLE', mode="NULLABLE"),
        bigquery.TableFieldSchema(name='temp', type='FLOAT64', mode="REPEATED"),
        bigquery.TableFieldSchema(name='count', type='INTEGER', mode=None)
    ]
    schema = bigquery.TableSchema(fields=fields)
    table = apache_beam.io.gcp.internal.clients.bigquery. \
        bigquery_v2_messages.Table(
        schema=schema)
    get_table.return_value = table

    with self.assertRaisesRegex(ValueError,
                                "Encountered an unsupported type: 'DOUBLE'"):
      p = apache_beam.Pipeline()
      _ = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="dataset.sample_table",
          method="DIRECT_READ",
          project="project",
          output_type='BEAM_ROW')

  def test_unsupported_value_provider(self):
    with self.assertRaisesRegex(TypeError,
                                'ReadFromBigQuery: table must be of type string'
                                '; got ValueProvider instead'):
      p = apache_beam.Pipeline()
      _ = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table=value_provider.ValueProvider(), output_type='BEAM_ROW')

  def test_unsupported_callable(self):
    def filterTable(table):
      if table is not None:
        return table

    res = filterTable
    with self.assertRaisesRegex(TypeError,
                                'ReadFromBigQuery: table must be of type string'
                                '; got a callable instead'):
      p = apache_beam.Pipeline()
      _ = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table=res, output_type='BEAM_ROW')

  def test_unsupported_query_export(self):
    with self.assertRaisesRegex(
        ValueError,
        "Both a query and an output type of 'BEAM_ROW' were specified. "
        "'BEAM_ROW' is not currently supported with queries."):
      p = apache_beam.Pipeline()
      _ = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="project:dataset.sample_table",
          method="EXPORT",
          query='SELECT name FROM dataset.sample_table',
          output_type='BEAM_ROW')

  def test_unsupported_query_direct_read(self):
    with self.assertRaisesRegex(
        ValueError,
        "Both a query and an output type of 'BEAM_ROW' were specified. "
        "'BEAM_ROW' is not currently supported with queries."):
      p = apache_beam.Pipeline()
      _ = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="project:dataset.sample_table",
          method="DIRECT_READ",
          query='SELECT name FROM dataset.sample_table',
          output_type='BEAM_ROW')

  def test_geography_type_support(self):
    """Test that GEOGRAPHY type is properly supported in schema conversion."""
    fields = [
        bigquery.TableFieldSchema(
            name='location', type='GEOGRAPHY', mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name='locations', type='GEOGRAPHY', mode="REPEATED"),
        bigquery.TableFieldSchema(
            name='required_location', type='GEOGRAPHY', mode="REQUIRED")
    ]
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.generate_user_type_from_bq_schema(
        the_table_schema=schema)

    expected_annotations = {
        'location': typing.Optional[str],
        'locations': typing.Sequence[str],
        'required_location': str
    }

    self.assertEqual(usertype.__annotations__, expected_annotations)

  def test_geography_in_bq_to_python_types_mapping(self):
    """Test that GEOGRAPHY is included in BIG_QUERY_TO_PYTHON_TYPES mapping."""
    from apache_beam.io.gcp.bigquery_schema_tools import BIG_QUERY_TO_PYTHON_TYPES

    self.assertIn("GEOGRAPHY", BIG_QUERY_TO_PYTHON_TYPES)
    self.assertEqual(BIG_QUERY_TO_PYTHON_TYPES["GEOGRAPHY"], str)

  def test_geography_field_type_conversion(self):
    """Test bq_field_to_type function with GEOGRAPHY fields."""
    from apache_beam.io.gcp.bigquery_schema_tools import bq_field_to_type

    # Test required GEOGRAPHY field
    result = bq_field_to_type("GEOGRAPHY", "REQUIRED")
    self.assertEqual(result, str)

    # Test nullable GEOGRAPHY field
    result = bq_field_to_type("GEOGRAPHY", "NULLABLE")
    self.assertEqual(result, typing.Optional[str])

    # Test repeated GEOGRAPHY field
    result = bq_field_to_type("GEOGRAPHY", "REPEATED")
    self.assertEqual(result, typing.Sequence[str])

    # Test GEOGRAPHY field with None mode (should default to nullable)
    result = bq_field_to_type("GEOGRAPHY", None)
    self.assertEqual(result, typing.Optional[str])

    # Test GEOGRAPHY field with empty mode (should default to nullable)
    result = bq_field_to_type("GEOGRAPHY", "")
    self.assertEqual(result, typing.Optional[str])

  def test_convert_to_usertype_with_geography(self):
    """Test convert_to_usertype function with GEOGRAPHY fields."""
    schema = bigquery.TableSchema(
        fields=[
            bigquery.TableFieldSchema(
                name='id', type='INTEGER', mode="REQUIRED"),
            bigquery.TableFieldSchema(
                name='location', type='GEOGRAPHY', mode="NULLABLE"),
            bigquery.TableFieldSchema(
                name='name', type='STRING', mode="REQUIRED")
        ])

    conversion_transform = bigquery_schema_tools.convert_to_usertype(schema)

    # Verify the transform is created successfully
    self.assertIsNotNone(conversion_transform)

    # The transform should be a ParDo with BeamSchemaConversionDoFn
    self.assertIsInstance(conversion_transform, beam.ParDo)

  def test_beam_schema_conversion_dofn_with_geography(self):
    """Test BeamSchemaConversionDoFn with GEOGRAPHY data."""
    from apache_beam.io.gcp.bigquery_schema_tools import BeamSchemaConversionDoFn

    # Create a user type with GEOGRAPHY field
    fields = [
        bigquery.TableFieldSchema(name='id', type='INTEGER', mode="REQUIRED"),
        bigquery.TableFieldSchema(
            name='location', type='GEOGRAPHY', mode="NULLABLE")
    ]
    schema = bigquery.TableSchema(fields=fields)
    usertype = bigquery_schema_tools.generate_user_type_from_bq_schema(schema)

    # Create the DoFn
    dofn = BeamSchemaConversionDoFn(usertype)

    # Test processing a dictionary with GEOGRAPHY data
    input_dict = {'id': 1, 'location': 'POINT(30 10)'}

    results = list(dofn.process(input_dict))
    self.assertEqual(len(results), 1)

    result = results[0]
    self.assertEqual(result.id, 1)
    self.assertEqual(result.location, 'POINT(30 10)')

  def test_geography_with_complex_wkt(self):
    """Test GEOGRAPHY type with complex Well-Known Text geometries."""
    fields = [
        bigquery.TableFieldSchema(
            name='simple_point', type='GEOGRAPHY', mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name='linestring', type='GEOGRAPHY', mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name='polygon', type='GEOGRAPHY', mode="NULLABLE"),
        bigquery.TableFieldSchema(
            name='multigeometry', type='GEOGRAPHY', mode="NULLABLE")
    ]
    schema = bigquery.TableSchema(fields=fields)

    usertype = bigquery_schema_tools.generate_user_type_from_bq_schema(schema)

    # All GEOGRAPHY fields should map to Optional[str]
    expected_annotations = {
        'simple_point': typing.Optional[str],
        'linestring': typing.Optional[str],
        'polygon': typing.Optional[str],
        'multigeometry': typing.Optional[str]
    }

    self.assertEqual(usertype.__annotations__, expected_annotations)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
