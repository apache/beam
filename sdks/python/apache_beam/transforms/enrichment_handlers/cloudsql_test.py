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
      TableFunctionQueryConfig)
  from apache_beam.transforms.enrichment_handlers.cloudsql_it_test import (
      query_fn,
      where_clause_value_fn,
  )
except ImportError:
  raise unittest.SkipTest('Google Cloud SQL dependencies are not installed.')


class TestCloudSQLEnrichment(unittest.TestCase):
  @parameterized.expand([
      # Empty TableFieldsQueryConfig.
      (
          TableFieldsQueryConfig(
              table_id="", where_clause_template="", where_clause_fields=[]),
          1,
          2),
      # Missing where_clause_template in TableFieldsQueryConfig.
      (
          TableFieldsQueryConfig(
              table_id="table",
              where_clause_template="",
              where_clause_fields=["id"]),
          2,
          10),
      # Invalid CustomQueryConfig with None query_fn.
      (CustomQueryConfig(query_fn=None), 2, 10),  # type: ignore[arg-type]
      # Missing table_id in TableFunctionQueryConfig.
      (
          TableFunctionQueryConfig(
              table_id="",
              where_clause_template="id='{}'",
              where_clause_value_fn=where_clause_value_fn),
          2,
          10),
  ])
  def test_invalid_query_config(
      self, query_config, min_batch_size, max_batch_size):
    """
    TC 1: Empty TableFieldsQueryConfig.

    It should raise an error.
    TC 2: Missing where_clause_template in TableFieldsQueryConfig.
    TC 3: Invalid CustomQueryConfig with None query_fn.
    TC 4: Missing table_id in TableFunctionQueryConfig.
    """
    with self.assertRaises(ValueError):
      _ = CloudSQLEnrichmentHandler(
          database_type_adapter=DatabaseTypeAdapter.POSTGRESQL,
          database_address='',
          database_user='',
          database_password='',
          database_id='',
          query_config=query_config,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
      )

  def test_valid_query_configs(self):
    """Test valid query configuration cases."""
    # Valid TableFieldsQueryConfig.
    table_fields_config = TableFieldsQueryConfig(
        table_id="my_table",
        where_clause_template="id = '{}'",
        where_clause_fields=["id"])

    handler1 = CloudSQLEnrichmentHandler(
        database_type_adapter=DatabaseTypeAdapter.POSTGRESQL,
        database_address='localhost',
        database_user='user',
        database_password='password',
        database_id='db',
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
        database_type_adapter=DatabaseTypeAdapter.POSTGRESQL,
        database_address='localhost',
        database_user='user',
        database_password='password',
        database_id='db',
        query_config=table_function_config,
        min_batch_size=1,
        max_batch_size=10)

    self.assertEqual(
        handler2.query_template, "SELECT * FROM my_table WHERE id = '{}'")

    # Valid CustomQueryConfig.
    custom_config = CustomQueryConfig(query_fn=query_fn)

    handler3 = CloudSQLEnrichmentHandler(
        database_type_adapter=DatabaseTypeAdapter.POSTGRESQL,
        database_address='localhost',
        database_user='user',
        database_password='password',
        database_id='db',
        query_config=custom_config)

    # Verify that batching kwargs are empty for CustomQueryConfig.
    self.assertEqual(handler3.batch_elements_kwargs(), {})

  def test_custom_query_config_cache_key_error(self):
    """Test get_cache_key raises NotImplementedError with CustomQueryConfig."""
    custom_config = CustomQueryConfig(query_fn=query_fn)

    handler = CloudSQLEnrichmentHandler(
        database_type_adapter=DatabaseTypeAdapter.POSTGRESQL,
        database_address='localhost',
        database_user='user',
        database_password='password',
        database_id='db',
        query_config=custom_config)

    # Create a dummy request
    import apache_beam as beam
    request = beam.Row(id=1)

    # Verify that get_cache_key raises NotImplementedError
    with self.assertRaises(NotImplementedError):
      handler.get_cache_key(request)


if __name__ == '__main__':
  unittest.main()
