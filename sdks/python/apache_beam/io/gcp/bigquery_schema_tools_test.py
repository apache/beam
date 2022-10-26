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
      pipeline = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="dataset.sample_table",
          method="EXPORT",
          project="project",
          output_type='BEAM_ROW')
      pipeline

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
      pipeline = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="dataset.sample_table",
          method="DIRECT_READ",
          project="project",
          output_type='BEAM_ROW')
      pipeline

  def test_unsupported_value_provider(self):
    with self.assertRaisesRegex(TypeError,
                                'ReadFromBigQuery: table must be of type string'
                                '; got ValueProvider instead'):
      p = apache_beam.Pipeline()
      pipeline = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table=value_provider.ValueProvider(), output_type='BEAM_ROW')
      pipeline

  def test_unsupported_callable(self):
    def filterTable(table):
      if table is not None:
        return table

    res = filterTable
    with self.assertRaisesRegex(TypeError,
                                'ReadFromBigQuery: table must be of type string'
                                '; got a callable instead'):
      p = apache_beam.Pipeline()
      pipeline = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table=res, output_type='BEAM_ROW')
      pipeline

  def test_unsupported_query_export(self):
    with self.assertRaisesRegex(
        ValueError,
        "Both a query and an output type of 'BEAM_ROW' were specified. "
        "'BEAM_ROW' is not currently supported with queries."):
      p = apache_beam.Pipeline()
      pipeline = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="project:dataset.sample_table",
          method="EXPORT",
          query='SELECT name FROM dataset.sample_table',
          output_type='BEAM_ROW')
      pipeline

  def test_unsupported_query_direct_read(self):
    with self.assertRaisesRegex(
        ValueError,
        "Both a query and an output type of 'BEAM_ROW' were specified. "
        "'BEAM_ROW' is not currently supported with queries."):
      p = apache_beam.Pipeline()
      pipeline = p | apache_beam.io.gcp.bigquery.ReadFromBigQuery(
          table="project:dataset.sample_table",
          method="DIRECT_READ",
          query='SELECT name FROM dataset.sample_table',
          output_type='BEAM_ROW')
      pipeline

  if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
