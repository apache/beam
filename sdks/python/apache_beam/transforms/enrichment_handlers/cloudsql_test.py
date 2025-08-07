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
        where_clause_template="id = :id",
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
        handler1.query_template, "SELECT * FROM my_table WHERE id = :id")

    # Valid TableFunctionQueryConfig.
    table_function_config = TableFunctionQueryConfig(
        table_id="my_table",
        where_clause_template="id = :id",
        where_clause_value_fn=where_clause_value_fn)

    handler2 = CloudSQLEnrichmentHandler(
        connection_config=connection_config,
        query_config=table_function_config,
        min_batch_size=1,
        max_batch_size=10)

    self.assertEqual(
        handler2.query_template, "SELECT * FROM my_table WHERE id = :id")

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

    # Create a dummy request.
    import apache_beam as beam
    request = beam.Row(id=1)

    # Verify that get_cache_key raises NotImplementedError.
    with self.assertRaises(NotImplementedError):
      handler.get_cache_key(request)

  def test_extract_parameter_names(self):
    """Test parameter extraction from SQL templates."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id",
        where_clause_fields=["id"])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    # Test simple parameter extraction.
    self.assertEqual(handler._extract_parameter_names("id = :id"), ["id"])

    # Test multiple parameters.
    self.assertEqual(
        handler._extract_parameter_names("id = :id AND name = :name"),
        ["id", "name"])

    # Test no parameters.
    self.assertEqual(
        handler._extract_parameter_names("SELECT * FROM users"), [])

    # Test complex query.
    self.assertEqual(
        handler._extract_parameter_names(
            "age > :min_age AND city = :city AND status = :status"),
        ["min_age", "city", "status"])

  def test_build_single_param_dict(self):
    """Test building parameter dictionaries for single requests."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    # Test TableFieldsQueryConfig.
    table_config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id AND name = :name",
        where_clause_fields=["id", "name"])

    handler1 = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=table_config)

    result1 = handler1._build_single_param_dict([123, "John"])
    self.assertEqual(result1, {"id": 123, "name": "John"})

    # Test TableFunctionQueryConfig.
    table_func_config = TableFunctionQueryConfig(
        table_id="users",
        where_clause_template="age > :min_age AND city = :city",
        where_clause_value_fn=lambda row: [row.min_age, row.city])

    handler2 = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=table_func_config)

    result2 = handler2._build_single_param_dict([3, "NYC"])
    self.assertEqual(result2, {"min_age": 3, "city": "NYC"})

  def test_extract_values_from_request(self):
    """Test extracting values from requests based on query configuration."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    # Test TableFieldsQueryConfig.
    table_config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id AND name = :name",
        where_clause_fields=["id", "name"])

    handler1 = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=table_config)

    import apache_beam as beam
    request1 = beam.Row(id=123, name="John", age=30)
    result1 = handler1._extract_values_from_request(request1)
    self.assertEqual(result1, [123, "John"])

    # Test TableFunctionQueryConfig.
    def test_value_fn(row):
      return [row.age, row.city]

    table_func_config = TableFunctionQueryConfig(
        table_id="users",
        where_clause_template="age > :min_age AND city = :city",
        where_clause_value_fn=test_value_fn)

    handler2 = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=table_func_config)

    request2 = beam.Row(age=25, city="NYC", name="Jane")
    result2 = handler2._extract_values_from_request(request2)
    self.assertEqual(result2, [25, "NYC"])

    # Test missing field error.
    request3 = beam.Row(age=30)  # Missing name field.
    with self.assertRaises(KeyError) as context:
      handler1._extract_values_from_request(request3)
    self.assertIn("where_clause_fields", str(context.exception))

  def test_build_batch_query_single_request(self):
    """Test batch query building with single request returns original query."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id",
        where_clause_fields=["id"])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    import apache_beam as beam
    requests = [beam.Row(id=1)]

    # Single request should return original query template.
    result = handler._build_batch_query(requests, batch_size=1)
    self.assertEqual(result, "SELECT * FROM users WHERE id = :id")

  def test_build_batch_query_multiple_requests(self):
    """Test batch query building with multiple requests creates OR clauses."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id AND name = :name",
        where_clause_fields=["id", "name"])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    import apache_beam as beam
    requests = [beam.Row(id=1, name="Alice"), beam.Row(id=2, name="Bob")]

    result = handler._build_batch_query(requests, batch_size=2)
    expected = (
        "SELECT * FROM users WHERE "
        "(id = :batch_0_id AND name = :batch_0_name) OR "
        "(id = :batch_1_id AND name = :batch_1_name)")
    self.assertEqual(result, expected)

  def test_build_parameters_dict_batch(self):
    """Test parameter dictionary building for batch requests."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id",
        where_clause_fields=["id"])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    import apache_beam as beam
    requests = [beam.Row(id=1), beam.Row(id=2)]

    result = handler._build_parameters_dict(requests, batch_size=2)
    expected = {"batch_0_id": 1, "batch_1_id": 2}
    self.assertEqual(result, expected)

  def test_create_batch_clause(self):
    """Test batch clause creation with unique parameter names."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id AND name = :name",
        where_clause_fields=["id", "name"])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    result = handler._create_batch_clause(batch_index=0)
    expected = "(id = :batch_0_id AND name = :batch_0_name)"
    self.assertEqual(result, expected)

    result2 = handler._create_batch_clause(batch_index=1)
    expected2 = "(id = :batch_1_id AND name = :batch_1_name)"
    self.assertEqual(result2, expected2)

  def test_security_parameter_extraction_edge_cases(self):
    """Test parameter extraction with edge cases and security issues."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id",
        where_clause_fields=["id"])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    # Test empty template.
    self.assertEqual(handler._extract_parameter_names(""), [])

    # Test template with no parameters.
    self.assertEqual(
        handler._extract_parameter_names("SELECT * FROM users"), [])

    # Test template with malformed parameters (should not match).
    self.assertEqual(handler._extract_parameter_names("id = :"), [])

    # Test template with numeric parameter names.
    self.assertEqual(
        handler._extract_parameter_names("col = :param_123"), ["param_123"])

    # Test template with underscore parameter names.
    self.assertEqual(
        handler._extract_parameter_names("col = :user_id"), ["user_id"])

    # Test duplicate parameter names.
    result = handler._extract_parameter_names("id = :id OR id = :id")
    # Note: re.findall returns all matches, so we'd get ["id", "id"].
    self.assertEqual(result, ["id", "id"])

  def test_build_single_param_dict_with_generic_names(self):
    """Test parameter dict building with generic names."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    # Test TableFunctionQueryConfig with generic parameter names.
    config = TableFunctionQueryConfig(
        table_id="users",
        where_clause_template="id = :param_0 AND name = :param_1",
        where_clause_value_fn=lambda row: [row.id, row.name])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    result = handler._build_single_param_dict([123, "John"])
    expected = {"param_0": 123, "param_1": "John"}
    self.assertEqual(result, expected)

  def test_duplicate_binding_parameter_names_handling(self):
    """Test handling of duplicate binding parameter names."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    # Test TableFunctionQueryConfig with duplicate parameter names.
    config = TableFunctionQueryConfig(
        table_id="users",
        where_clause_template="id = :param AND name = :param",
        where_clause_value_fn=lambda row: [row.id, row.name])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    # Test that duplicate parameter names are made unique.
    result = handler._build_single_param_dict([123, "John"])
    expected = {"param_0": 123, "param_1": "John"}
    self.assertEqual(result, expected)

    # Test the helper method for template and params.
    template = "id = :param AND name = :param"
    values = [123, "John"]
    updated_template, param_dict = handler._get_unique_template_and_params(
        template, values)

    expected_template = "id = :param_0 AND name = :param_1"
    expected_params = {"param_0": 123, "param_1": "John"}
    self.assertEqual(updated_template, expected_template)
    self.assertEqual(param_dict, expected_params)

  def test_unsupported_query_config_type_error(self):
    """Test that unsupported query config types raise ValueError."""
    connection_config = ExternalSQLDBConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        host='localhost',
        port=5432,
        user='user',
        password='password',
        db_id='db')

    config = TableFieldsQueryConfig(
        table_id="users",
        where_clause_template="id = :id",
        where_clause_fields=["id"])

    handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config, query_config=config)

    # Temporarily replace the config with an unsupported type.
    handler._query_config = "unsupported_type"

    import apache_beam as beam
    request = beam.Row(id=1)

    with self.assertRaises(ValueError) as context:
      handler._extract_values_from_request(request)
    self.assertIn(
        "Unsupported query configuration type", str(context.exception))


if __name__ == '__main__':
  unittest.main()
