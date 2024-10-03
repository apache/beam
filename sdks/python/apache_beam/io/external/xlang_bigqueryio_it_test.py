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

"""Unit tests for cross-language BigQuery sources and sinks."""
# pytype: skip-file

import datetime
import logging
import os
import secrets
import time
import unittest
from decimal import Decimal

import pytest
from hamcrest.core import assert_that as hamcrest_assert
from hamcrest.core.core.allof import all_of

import apache_beam as beam
from apache_beam.io.gcp.bigquery import StorageWriteToBigQuery
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultStreamingMatcher
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.utils.timestamp import Timestamp

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None
# pylint: enable=wrong-import-order, wrong-import-position

_LOGGER = logging.getLogger(__name__)


@pytest.mark.uses_gcp_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class BigQueryXlangStorageWriteIT(unittest.TestCase):
  BIGQUERY_DATASET = 'python_xlang_storage_write'

  ELEMENTS = [
      # (int, float, numeric, string, bool, bytes, timestamp)
      {
          "int": 1,
          "float": 0.1,
          "numeric": Decimal("1.11"),
          "str": "a",
          "bool": True,
          "bytes": b'a',
          "timestamp": Timestamp(1000, 100)
      },
      {
          "int": 2,
          "float": 0.2,
          "numeric": Decimal("2.22"),
          "str": "b",
          "bool": False,
          "bytes": b'b',
          "timestamp": Timestamp(2000, 200)
      },
      {
          "int": 3,
          "float": 0.3,
          "numeric": Decimal("3.33"),
          "str": "c",
          "bool": True,
          "bytes": b'd',
          "timestamp": Timestamp(3000, 300)
      },
      {
          "int": 4,
          "float": 0.4,
          "numeric": Decimal("4.44"),
          "str": "d",
          "bool": False,
          "bytes": b'd',
          "timestamp": Timestamp(4000, 400)
      }
  ]
  ALL_TYPES_SCHEMA = (
      "int:INTEGER,float:FLOAT,numeric:NUMERIC,str:STRING,"
      "bool:BOOLEAN,bytes:BYTES,timestamp:TIMESTAMP")

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.args = self.test_pipeline.get_full_options_as_args()
    self.project = self.test_pipeline.get_option('project')
    self._runner = PipelineOptions(self.args).get_all_options()['runner']

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s_%s_%s' % (
        self.BIGQUERY_DATASET, str(int(time.time())), secrets.token_hex(3))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)

  def tearDown(self):
    try:
      _LOGGER.info(
          "Deleting dataset %s in project %s", self.dataset_id, self.project)
      self.bigquery_client._delete_dataset(
          project_id=self.project,
          dataset_id=self.dataset_id,
          delete_contents=True)
    except HttpError:
      _LOGGER.debug(
          'Failed to clean up dataset %s in project %s',
          self.dataset_id,
          self.project)

  def parse_expected_data(self, expected_elements):
    if not isinstance(expected_elements, list):
      expected_elements = [expected_elements]
    data = []
    for row in expected_elements:
      values = list(row.values())
      for i, val in enumerate(values):
        if isinstance(val, Timestamp):
          # BigQuery matcher query returns a datetime.datetime object
          values[i] = val.to_utc_datetime().replace(
              tzinfo=datetime.timezone.utc)
      data.append(tuple(values))

    return data

  def run_storage_write_test(
      self, table_name, items, schema, use_at_least_once=False):
    table_id = '{}:{}.{}'.format(self.project, self.dataset_id, table_name)

    bq_matcher = BigqueryFullResultMatcher(
        project=self.project,
        query="SELECT * FROM %s" % '{}.{}'.format(self.dataset_id, table_name),
        data=self.parse_expected_data(items))

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(items)
          | beam.io.WriteToBigQuery(
              table=table_id,
              method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
              schema=schema,
              use_at_least_once=use_at_least_once))
    hamcrest_assert(p, bq_matcher)

  def test_all_types(self):
    table_name = "all_types"
    schema = self.ALL_TYPES_SCHEMA
    self.run_storage_write_test(table_name, self.ELEMENTS, schema)

  def test_with_at_least_once_semantics(self):
    table_name = "with_at_least_once_semantics"
    schema = self.ALL_TYPES_SCHEMA
    self.run_storage_write_test(
        table_name, self.ELEMENTS, schema, use_at_least_once=True)

  def test_nested_records_and_lists(self):
    table_name = "nested_records_and_lists"
    schema = {
        "fields": [{
            "name": "repeated_int", "type": "INTEGER", "mode": "REPEATED"
        },
                   {
                       "name": "struct",
                       "type": "STRUCT",
                       "fields": [{
                           "name": "nested_int", "type": "INTEGER"
                       }, {
                           "name": "nested_str", "type": "STRING"
                       }]
                   },
                   {
                       "name": "repeated_struct",
                       "type": "STRUCT",
                       "mode": "REPEATED",
                       "fields": [{
                           "name": "nested_numeric", "type": "NUMERIC"
                       }, {
                           "name": "nested_bytes", "type": "BYTES"
                       }]
                   }]
    }
    items = [{
        "repeated_int": [1, 2, 3],
        "struct": {
            "nested_int": 1, "nested_str": "a"
        },
        "repeated_struct": [{
            "nested_numeric": Decimal("1.23"), "nested_bytes": b'a'
        },
                            {
                                "nested_numeric": Decimal("3.21"),
                                "nested_bytes": b'aa'
                            }]
    }]

    self.run_storage_write_test(table_name, items, schema)

  def test_write_with_beam_rows(self):
    table = 'write_with_beam_rows'
    table_id = '{}:{}.{}'.format(self.project, self.dataset_id, table)

    row_elements = [
        beam.Row(
            my_int=e['int'],
            my_float=e['float'],
            my_numeric=e['numeric'],
            my_string=e['str'],
            my_bool=e['bool'],
            my_bytes=e['bytes'],
            my_timestamp=e['timestamp']) for e in self.ELEMENTS
    ]

    bq_matcher = BigqueryFullResultMatcher(
        project=self.project,
        query="SELECT * FROM {}.{}".format(self.dataset_id, table),
        data=self.parse_expected_data(self.ELEMENTS))

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(row_elements)
          | StorageWriteToBigQuery(table=table_id))
    hamcrest_assert(p, bq_matcher)

  def test_write_with_beam_rows_cdc(self):
    table = 'write_with_beam_rows_cdc'
    table_id = '{}:{}.{}'.format(self.project, self.dataset_id, table)

    expected_data_on_bq = [
        # (name, value)
        {
            "name": "cdc_test",
            "value": 5,
        }
    ]

    rows_with_cdc = [
        beam.Row(
            row_mutation_info=beam.Row(
                mutation_type="UPSERT", change_sequence_number="AAA/2"),
            record=beam.Row(name="cdc_test", value=5)),
        beam.Row(
            row_mutation_info=beam.Row(
                mutation_type="UPSERT", change_sequence_number="AAA/1"),
            record=beam.Row(name="cdc_test", value=3))
    ]

    bq_matcher = BigqueryFullResultMatcher(
        project=self.project,
        query="SELECT * FROM {}.{}".format(self.dataset_id, table),
        data=self.parse_expected_data(expected_data_on_bq))

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(rows_with_cdc)
          | beam.io.WriteToBigQuery(
              table=table_id,
              method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
              use_at_least_once=True,
              use_cdc_writes=True,
              primary_key=["name"]))
    hamcrest_assert(p, bq_matcher)

  def test_write_to_dynamic_destinations(self):
    base_table_spec = '{}.dynamic_dest_'.format(self.dataset_id)
    spec_with_project = '{}:{}'.format(self.project, base_table_spec)
    tables = [base_table_spec + str(record['int']) for record in self.ELEMENTS]

    bq_matchers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % tables[i],
            data=self.parse_expected_data(self.ELEMENTS[i]))
        for i in range(len(tables))
    ]

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(self.ELEMENTS)
          | beam.io.WriteToBigQuery(
              table=lambda record: spec_with_project + str(record['int']),
              method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
              schema=self.ALL_TYPES_SCHEMA,
              use_at_least_once=False))
    hamcrest_assert(p, all_of(*bq_matchers))

  def test_write_to_dynamic_destinations_with_beam_rows(self):
    base_table_spec = '{}.dynamic_dest_'.format(self.dataset_id)
    spec_with_project = '{}:{}'.format(self.project, base_table_spec)
    tables = [base_table_spec + str(record['int']) for record in self.ELEMENTS]

    bq_matchers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % tables[i],
            data=self.parse_expected_data(self.ELEMENTS[i]))
        for i in range(len(tables))
    ]

    row_elements = [
        beam.Row(
            my_int=e['int'],
            my_float=e['float'],
            my_numeric=e['numeric'],
            my_string=e['str'],
            my_bool=e['bool'],
            my_bytes=e['bytes'],
            my_timestamp=e['timestamp']) for e in self.ELEMENTS
    ]

    with beam.Pipeline(argv=self.args) as p:
      _ = (
          p
          | beam.Create(row_elements)
          | beam.io.WriteToBigQuery(
              table=lambda record: spec_with_project + str(record.my_int),
              method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
              use_at_least_once=False))
    hamcrest_assert(p, all_of(*bq_matchers))

  def run_streaming(self, table_name, num_streams=0, use_at_least_once=False):
    elements = self.ELEMENTS.copy()
    schema = self.ALL_TYPES_SCHEMA
    table_id = '{}:{}.{}'.format(self.project, self.dataset_id, table_name)

    bq_matcher = BigqueryFullResultStreamingMatcher(
        project=self.project,
        query="SELECT * FROM {}.{}".format(self.dataset_id, table_name),
        data=self.parse_expected_data(self.ELEMENTS))

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=bq_matcher,
        streaming=True,
        allow_unsafe_triggers=True)

    auto_sharding = (num_streams == 0)
    with beam.Pipeline(argv=args) as p:
      _ = (
          p
          | PeriodicImpulse(0, 4, 1)
          | beam.Map(lambda t: elements[t])
          | beam.io.WriteToBigQuery(
              table=table_id,
              method=beam.io.WriteToBigQuery.Method.STORAGE_WRITE_API,
              schema=schema,
              triggering_frequency=1,
              with_auto_sharding=auto_sharding,
              num_storage_api_streams=num_streams,
              use_at_least_once=use_at_least_once))
    hamcrest_assert(p, bq_matcher)

  def skip_if_not_dataflow_runner(self) -> bool:
    # skip if dataflow runner is not specified
    if not self._runner or "dataflowrunner" not in self._runner.lower():
      self.skipTest(
          "Streaming with exactly-once route has the requirement "
          "`beam:requirement:pardo:on_window_expiration:v1`, "
          "which is currently only supported by the Dataflow runner")

  def test_streaming_with_fixed_num_streams(self):
    self.skip_if_not_dataflow_runner()
    table = 'streaming_fixed_num_streams'
    self.run_streaming(table_name=table, num_streams=4)

  @unittest.skip(
      "Streaming to the Storage Write API sink with autosharding is broken "
      "with Dataflow Runner V2.")
  def test_streaming_with_auto_sharding(self):
    self.skip_if_not_dataflow_runner()
    table = 'streaming_with_auto_sharding'
    self.run_streaming(table_name=table)

  def test_streaming_with_at_least_once(self):
    table = 'streaming_with_at_least_once'
    self.run_streaming(table_name=table, use_at_least_once=True)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
