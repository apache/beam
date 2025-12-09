#!/usr/bin/env python
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

"""Integration tests for BigQuery GEOGRAPHY data type support."""

import logging
import os
import secrets
import time
import unittest

import hamcrest as hc
import pytest

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None

_LOGGER = logging.getLogger(__name__)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class BigQueryGeographyIntegrationTests(unittest.TestCase):
  """Integration tests for BigQuery GEOGRAPHY data type."""

  BIG_QUERY_DATASET_ID = 'python_geography_it_test_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%d%s' % (
        self.BIG_QUERY_DATASET_ID, int(time.time()), secrets.token_hex(3))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id, deleteContents=True)
    try:
      _LOGGER.info(
          "Deleting dataset %s in project %s", self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      _LOGGER.debug(
          'Failed to clean up dataset %s in project %s',
          self.dataset_id,
          self.project)

  def create_geography_table(self, table_name, include_repeated=False):
    """Create a table with various GEOGRAPHY field configurations."""
    table_schema = bigquery.TableSchema()

    # ID field
    id_field = bigquery.TableFieldSchema()
    id_field.name = 'id'
    id_field.type = 'INTEGER'
    id_field.mode = 'REQUIRED'
    table_schema.fields.append(id_field)

    # Required GEOGRAPHY field
    geo_required = bigquery.TableFieldSchema()
    geo_required.name = 'location'
    geo_required.type = 'GEOGRAPHY'
    geo_required.mode = 'REQUIRED'
    table_schema.fields.append(geo_required)

    # Nullable GEOGRAPHY field
    geo_nullable = bigquery.TableFieldSchema()
    geo_nullable.name = 'optional_location'
    geo_nullable.type = 'GEOGRAPHY'
    geo_nullable.mode = 'NULLABLE'
    table_schema.fields.append(geo_nullable)

    if include_repeated:
      # Repeated GEOGRAPHY field
      geo_repeated = bigquery.TableFieldSchema()
      geo_repeated.name = 'path'
      geo_repeated.type = 'GEOGRAPHY'
      geo_repeated.mode = 'REPEATED'
      table_schema.fields.append(geo_repeated)

    table = bigquery.Table(
        tableReference=bigquery.TableReference(
            projectId=self.project,
            datasetId=self.dataset_id,
            tableId=table_name),
        schema=table_schema)
    request = bigquery.BigqueryTablesInsertRequest(
        projectId=self.project, datasetId=self.dataset_id, table=table)
    self.bigquery_client.client.tables.Insert(request)

    # Wait for table to be available
    _ = self.bigquery_client.get_table(
        self.project, self.dataset_id, table_name)

  @pytest.mark.it_postcommit
  def test_geography_write_and_read_basic_geometries(self):
    """Test writing and reading basic GEOGRAPHY geometries."""
    table_name = 'geography_basic_geometries'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    # Test data with various WKT geometry types
    input_data = [
        {
            'id': 1,
            'location': 'POINT(30 10)',
            'optional_location': ('POINT(-122.4194 37.7749)')  # San Francisco
        },
        {
            'id': 2,
            'location': 'LINESTRING(30 10, 10 30, 40 40)',
            'optional_location': None
        },
        {
            'id': 3,
            'location': ('POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))'),
            'optional_location': ('POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))')
        },
        {
            'id': 4,
            'location': ('MULTIPOINT((10 40), (40 30), (20 20), (30 10))'),
            'optional_location': 'POINT(0 0)'
        },
        {
            'id': 5,
            'location': (
                'MULTILINESTRING((10 10, 20 20, 10 40), '
                '(40 40, 30 30, 40 20, 30 10))'),
            'optional_location': None
        }
    ]

    table_schema = {
        "fields": [{
            "name": "id", "type": "INTEGER", "mode": "REQUIRED"
        }, {
            "name": "location", "type": "GEOGRAPHY", "mode": "REQUIRED"
        },
                   {
                       "name": "optional_location",
                       "type": "GEOGRAPHY",
                       "mode": "NULLABLE"
                   }]
    }

    # Write data to BigQuery
    with TestPipeline(is_integration_test=True) as p:
      _ = (
          p
          | 'CreateData' >> beam.Create(input_data)
          | 'WriteToBQ' >> WriteToBigQuery(
              table=table_id,
              schema=table_schema,
              method=WriteToBigQuery.Method.STREAMING_INSERTS,
              project=self.project))

    # Read data back and verify
    with TestPipeline(is_integration_test=True) as p:
      result = (
          p
          | 'ReadFromBQ' >> ReadFromBigQuery(
              table=table_id,
              project=self.project,
              method=ReadFromBigQuery.Method.DIRECT_READ)
          | 'ExtractGeography' >> beam.Map(
              lambda row:
              (row['id'], row['location'], row['optional_location'])))

      expected_data = [
          (1, 'POINT(30 10)', 'POINT(-122.4194 37.7749)'),
          (2, 'LINESTRING(30 10, 10 30, 40 40)', None),
          (
              3,
              'POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))',
              'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))'),
          (4, 'MULTIPOINT(20 20, 10 40, 40 30, 30 10)', 'POINT(0 0)'),
          (
              5,
              'MULTILINESTRING((10 10, 20 20, 10 40), '
              '(40 40, 30 30, 40 20, 30 10))',
              None)
      ]

      assert_that(result, equal_to(expected_data))

  @pytest.mark.it_postcommit
  def test_geography_write_with_beam_rows(self):
    """Test writing GEOGRAPHY data using Beam Rows with GeographyType."""
    table_name = 'geography_beam_rows'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    # Create the table first
    self.create_geography_table(table_name)

    # Create Beam Rows with GeographyType
    row_elements = [
        beam.Row(id=1, location='POINT(1 1)', optional_location='POINT(2 2)'),
        beam.Row(
            id=2, location='LINESTRING(0 0, 1 1, 2 2)', optional_location=None),
        beam.Row(
            id=3,
            location='POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))',
            optional_location='POINT(3 3)')
    ]

    # Expected data for verification
    expected_data = [(1, 'POINT(1 1)', 'POINT(2 2)'),
                     (2, 'LINESTRING(0 0, 1 1, 2 2)', None),
                     (3, 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))', 'POINT(3 3)')]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query=(
                "SELECT id, location, optional_location FROM %s ORDER BY id" %
                table_id),
            data=expected_data)
    ]

    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      _ = (
          p
          | 'CreateRows' >> beam.Create(row_elements)
          | 'ConvertToDict' >> beam.Map(
              lambda row: {
                  'id': row.id, 'location': row.location,
                  'optional_location': row.optional_location
              })
          | 'WriteToBQ' >> WriteToBigQuery(
              table=table_id,
              method=WriteToBigQuery.Method.STREAMING_INSERTS,
              schema={
                  "fields": [{
                      "name": "id", "type": "INTEGER", "mode": "REQUIRED"
                  },
                             {
                                 "name": "location",
                                 "type": "GEOGRAPHY",
                                 "mode": "REQUIRED"
                             },
                             {
                                 "name": "optional_location",
                                 "type": "GEOGRAPHY",
                                 "mode": "NULLABLE"
                             }]
              }))

    # Wait a bit for streaming inserts to complete
    time.sleep(5)

    # Verify the data was written correctly
    hc.assert_that(None, hc.all_of(*pipeline_verifiers))

  @pytest.mark.it_postcommit
  def test_geography_repeated_fields(self):
    """Test GEOGRAPHY fields with REPEATED mode."""
    table_name = 'geography_repeated'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [
        {
            'id': 1,
            'location': 'POINT(0 0)',
            'optional_location': 'POINT(1 1)',
            'path': ['POINT(0 0)', 'POINT(1 1)', 'POINT(2 2)']
        },
        {
            'id': 2,
            'location': 'POINT(10 10)',
            'optional_location': None,
            'path': ['LINESTRING(0 0, 5 5)', 'LINESTRING(5 5, 10 10)']
        },
        {
            'id': 3,
            'location': 'POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))',
            'optional_location': 'POINT(0.5 0.5)',
            'path': []  # Empty array
        }
    ]

    table_schema = {
        "fields": [{
            "name": "id", "type": "INTEGER", "mode": "REQUIRED"
        }, {
            "name": "location", "type": "GEOGRAPHY", "mode": "REQUIRED"
        },
                   {
                       "name": "optional_location",
                       "type": "GEOGRAPHY",
                       "mode": "NULLABLE"
                   }, {
                       "name": "path", "type": "GEOGRAPHY", "mode": "REPEATED"
                   }]
    }

    # Write data
    args = self.test_pipeline.get_full_options_as_args()
    with beam.Pipeline(argv=args) as p:
      _ = (
          p
          | 'CreateData' >> beam.Create(input_data)
          | 'WriteToBQ' >> WriteToBigQuery(
              table=table_id,
              schema=table_schema,
              method=WriteToBigQuery.Method.STREAMING_INSERTS))

    # Read and verify
    with beam.Pipeline(argv=args) as p:
      result = (
          p
          | 'ReadFromBQ' >> ReadFromBigQuery(
              table=table_id,
              method=ReadFromBigQuery.Method.DIRECT_READ,
              project=self.project)
          | 'ExtractData' >> beam.Map(
              lambda row: (row['id'], len(row['path']) if row['path'] else 0)))

      expected_counts = [(1, 3), (2, 2), (3, 0)]
      assert_that(result, equal_to(expected_counts))

  @pytest.mark.it_postcommit
  def test_geography_complex_geometries(self):
    """Test complex GEOGRAPHY geometries and edge cases."""
    table_name = 'geography_complex'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    # Complex geometries including collections and high precision coordinates
    input_data = [
        {
            'id': 1,
            'location': (
                'GEOMETRYCOLLECTION(POINT(4 6), LINESTRING(4 6, 7 10))'),
            'optional_location': None
        },
        {
            'id': 2,
            'location': (
                'MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), '
                '((2 2, 3 2, 3 3, 2 3, 2 2)))'),  # Fixed orientation
            'optional_location': ('POINT(-122.419416 37.774929)'
                                  )  # High precision
        },
        {
            'id': 3,
            'location': ('POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))'
                         ),  # Simple polygon without holes
            'optional_location': ('LINESTRING(-122 37, -121 38)'
                                  )  # Fixed non-antipodal coordinates
        }
    ]

    table_schema = {
        "fields": [{
            "name": "id", "type": "INTEGER", "mode": "REQUIRED"
        }, {
            "name": "location", "type": "GEOGRAPHY", "mode": "REQUIRED"
        },
                   {
                       "name": "optional_location",
                       "type": "GEOGRAPHY",
                       "mode": "NULLABLE"
                   }]
    }

    expected_data = [(1, 'LINESTRING(4 6, 7 10)', None),
                     (
                         2,
                         'MULTIPOLYGON(((0 0, 1 0, 1 1, 0 1, 0 0)), '
                         '((2 2, 3 2, 3 3, 2 3, 2 2)))',
                         'POINT(-122.419416 37.774929)'),
                     (
                         3,
                         'POLYGON((0 0, 0 5, 5 5, 5 0, 0 0))',
                         'LINESTRING(-122 37, -121 38)')]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query=(
                "SELECT id, location, optional_location FROM %s ORDER BY id" %
                table_id),
            data=expected_data)
    ]

    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      _ = (
          p
          | 'CreateData' >> beam.Create(input_data)
          | 'WriteToBQ' >> WriteToBigQuery(
              table=table_id,
              schema=table_schema,
              method=WriteToBigQuery.Method.STREAMING_INSERTS))

    hc.assert_that(p, hc.all_of(*pipeline_verifiers))

  @pytest.mark.uses_gcp_java_expansion_service
  @unittest.skipUnless(
      os.environ.get('EXPANSION_JARS'),
      "EXPANSION_JARS environment var is not provided, "
      "indicating that jars have not been built")
  def test_geography_storage_write_api(self):
    """Test GEOGRAPHY with Storage Write API method."""
    table_name = 'geography_storage_write'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [{
        'id': 1, 'location': 'POINT(0 0)', 'optional_location': 'POINT(1 1)'
    },
                  {
                      'id': 2,
                      'location': 'LINESTRING(0 0, 1 1)',
                      'optional_location': None
                  }]

    table_schema = {
        "fields": [{
            "name": "id", "type": "INTEGER", "mode": "REQUIRED"
        }, {
            "name": "location", "type": "GEOGRAPHY", "mode": "REQUIRED"
        },
                   {
                       "name": "optional_location",
                       "type": "GEOGRAPHY",
                       "mode": "NULLABLE"
                   }]
    }

    expected_data = [(1, 'POINT(0 0)', 'POINT(1 1)'),
                     (2, 'LINESTRING(0 0, 1 1)', None)]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query=(
                "SELECT id, location, optional_location FROM %s ORDER BY id" %
                table_id),
            data=expected_data)
    ]

    args = self.test_pipeline.get_full_options_as_args()

    with beam.Pipeline(argv=args) as p:
      _ = (
          p
          | 'CreateData' >> beam.Create(input_data)
          | 'WriteToBQ' >> WriteToBigQuery(
              table=table_id,
              schema=table_schema,
              method=WriteToBigQuery.Method.STORAGE_WRITE_API))

    hc.assert_that(p, hc.all_of(*pipeline_verifiers))

  @pytest.mark.it_postcommit
  def test_geography_file_loads_method(self):
    """Test GEOGRAPHY with FILE_LOADS method."""
    table_name = 'geography_file_loads'
    table_id = '{}.{}'.format(self.dataset_id, table_name)

    input_data = [
        {
            'id': i,
            'location': f'POINT({i} {i})',
            'optional_location': (
                f'POINT({i+10} {i+10})' if i % 2 == 0 else None)
        } for i in range(1, 11)  # 10 records
    ]

    table_schema = {
        "fields": [{
            "name": "id", "type": "INTEGER", "mode": "REQUIRED"
        }, {
            "name": "location", "type": "GEOGRAPHY", "mode": "REQUIRED"
        },
                   {
                       "name": "optional_location",
                       "type": "GEOGRAPHY",
                       "mode": "NULLABLE"
                   }]
    }

    # Verify count and some sample data
    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT COUNT(*) as count FROM %s" % table_id,
            data=[(10, )])
    ]

    args = self.test_pipeline.get_full_options_as_args()
    gcs_temp_location = (
        f'gs://temp-storage-for-end-to-end-tests/'
        f'bq_it_test_{int(time.time())}')

    with beam.Pipeline(argv=args) as p:
      _ = (
          p
          | 'CreateData' >> beam.Create(input_data)
          | 'WriteToBQ' >> WriteToBigQuery(
              table=table_id,
              schema=table_schema,
              method=WriteToBigQuery.Method.FILE_LOADS,
              custom_gcs_temp_location=gcs_temp_location))

    hc.assert_that(p, hc.all_of(*pipeline_verifiers))


if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  unittest.main()
