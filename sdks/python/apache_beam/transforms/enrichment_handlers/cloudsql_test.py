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

from parameterized import parameterized

# pylint: disable=ungrouped-imports
try:
  from apache_beam.transforms.enrichment_handlers.cloudsql import (
      CloudSQLEnrichmentHandler,
      DatabaseTypeAdapter,
      CustomQueryConfig,
      TableFieldsQueryConfig,
      TableFunctionQueryConfig,
      CloudSQLConnectionConfig,
      ExternalSQLDBConnectionConfig)
  from apache_beam.transforms.enrichment_handlers.cloudsql_it_test import (
      query_fn,
      where_clause_value_fn,
  )
except ImportError as e:
  raise unittest.SkipTest(f'CloudSQL dependencies not installed: {str(e)}')


class TestCloudSQLEnrichment(unittest.TestCase):
  def test_invalid_external_db_connection_params(self):
    with self.assertRaises(ValueError) as context:
      _ = ExternalSQLDBConnectionConfig(
          db_adapter=DatabaseTypeAdapter.POSTGRESQL,
          host='',
          port=5432,
          user='',
          password='',
          db_id='')
    self.assertIn("Database host cannot be empty", str(context.exception))

  def test_invalid_cloudsql_db_connection_params(self):
    with self.assertRaises(ValueError) as context:
      _ = CloudSQLConnectionConfig(
          db_adapter=DatabaseTypeAdapter.POSTGRESQL,
          instance_connection_uri='',
          user='',
          password='',
          db_id='')
    self.assertIn(
      "Instance connection URI cannot be empty", str(context.exception))

  @parameterized.expand([
      # Empty TableFieldsQueryConfig.
      (
          lambda: TableFieldsQueryConfig(
              table_id="", where_clause_template="", where_clause_fields=[]),
          1,
          2,
          "must provide table_id and where_clause_template"
      ),
      # Missing where_clause_template in TableFieldsQueryConfig.
      (
          lambda: TableFieldsQueryConfig(
              table_id="table",
              where_clause_template="",
              where_clause_fields=["id"]),
          2,
          10,
          "must provide table_id and where_clause_template"
      ),
      # Invalid CustomQueryConfig with None query_fn.
      (
          lambda: CustomQueryConfig(query_fn=None),  # type: ignore[arg-type]
          2,
          10,
          "must provide a valid query_fn"
      ),
      # Missing table_id in TableFunctionQueryConfig.
      (
          lambda: TableFunctionQueryConfig(
              table_id="",
              where_clause_template="id='{}'",
              where_clause_value_fn=where_clause_value_fn),
          2,
          10,
          "must provide table_id and where_clause_template"
      ),
      # Missing where_clause_fields in TableFieldsQueryConfig.
      (
          lambda: TableFieldsQueryConfig(
              table_id="table",
              where_clause_template="id = '{}'",
              where_clause_fields=[]),
          1,
          10,
          "must provide non-empty where_clause_fields"
      ),
      # Missing where_clause_value_fn in TableFunctionQueryConfig.
      (
          lambda: TableFunctionQueryConfig(
              table_id="table",
              where_clause_template="id = '{}'",
              where_clause_value_fn=None),  # type: ignore[arg-type]
          1,
          10,
          "must provide where_clause_value_fn"
      ),
  ])
  def test_invalid_query_config(
      self, create_config, min_batch_size, max_batch_size, expected_error_msg):
    """Test that validation errors are raised for invalid query configs.

    The test verifies both that the appropriate ValueError is raised and that
    the error message contains the expected text.
    """
    with self.assertRaises(ValueError) as context:
      # Call the lambda to create the config.
      query_config = create_config()

      connection_config = ExternalSQLDBConnectionConfig(
          db_adapter=DatabaseTypeAdapter.POSTGRESQL,
          host='localhost',
          port=5432,
          user='',
          password='',
          db_id='')

      _ = CloudSQLEnrichmentHandler(
          connection_config=connection_config,
          query_config=query_config,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
      )
    # Verify the error message contains the expected text.
    self.assertIn(expected_error_msg, str(context.exception))

  def test_valid_query_configs(self):
    """Test valid query configuration cases."""
    # Valid TableFieldsQueryConfig.
    table_fields_config = TableFieldsQueryConfig(
        table_id="my_table",
        where_clause_template="id = '{}'",
        where_clause_fields=["id"])

    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    handler1 = CloudSQLEnrichmentHandler(
        connection_config=connection_config,
        query_config=table_fields_config,
        min_batch_size=1,
        max_batch_size=10)

    self.assertEqual(
        handler1.query_template, "SELECT * FROM my_table WHERE id = '{}'")

    # Valid TableFunctionQueryConfig.
    table_function_config = TableFunctionQueryConfig(
        table_id="my_table",
        where_clause_template="id = '{}'",
        where_clause_value_fn=where_clause_value_fn)

    handler2 = CloudSQLEnrichmentHandler(
        connection_config=connection_config,
        query_config=table_function_config,
        min_batch_size=1,
        max_batch_size=10)

    self.assertEqual(
        handler2.query_template, "SELECT * FROM my_table WHERE id = '{}'")

    # Valid CustomQueryConfig.
    custom_config = CustomQueryConfig(query_fn=query_fn)

    handler3 = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=custom_config)

    # Verify that batching kwargs are empty for CustomQueryConfig.
    self.assertEqual(handler3.batch_elements_kwargs(), {})

  def test_custom_query_config_cache_key_error(self):
    """Test get_cache_key raises NotImplementedError with CustomQueryConfig."""
    custom_config = CustomQueryConfig(query_fn=query_fn)

    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=custom_config)

    # Create a dummy request
    import apache_beam as beam
    request = beam.Row(id=1)

    # Verify that get_cache_key raises NotImplementedError
    with self.assertRaises(NotImplementedError):
      handler.get_cache_key(request)


if __name__ == '__main__':
  unittest.main()
