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
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import List
from typing import Optional
from typing import Union
from typing import cast

from sqlalchemy import create_engine
from sqlalchemy import text

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

QueryFn = Callable[[beam.Row], str]
ConditionValueFn = Callable[[beam.Row], list[Any]]


@dataclass
class CustomQueryConfig:
  """Configuration for using a custom query function."""
  query_fn: QueryFn


@dataclass
class TableFieldsQueryConfig:
  """Configuration for using table name, where clause, and field names."""
  table_id: str
  where_clause_template: str
  where_clause_fields: List[str]


@dataclass
class TableFunctionQueryConfig:
  """Configuration for using table name, where clause, and a value function."""
  table_id: str
  where_clause_template: str
  where_clause_value_fn: ConditionValueFn


QueryConfig = Union[CustomQueryConfig,
                    TableFieldsQueryConfig,
                    TableFunctionQueryConfig]


def _validate_query_config(query_config: QueryConfig):
  """Validates the provided query configuration."""
  if isinstance(query_config, CustomQueryConfig):
    if not query_config.query_fn:
      raise ValueError("CustomQueryConfig must provide a valid query_fn")
  elif isinstance(query_config,
                  (TableFieldsQueryConfig, TableFunctionQueryConfig)):
    if not query_config.table_id or not query_config.where_clause_template:
      raise ValueError(
          "TableFieldsQueryConfig and " +
          "TableFunctionQueryConfig must provide table_id " +
          "and where_clause_template")

    is_table_fields = isinstance(query_config, TableFieldsQueryConfig)
    if is_table_fields:
      table_fields_config = cast(TableFieldsQueryConfig, query_config)
      if not table_fields_config.where_clause_fields:
        raise ValueError(
            "TableFieldsQueryConfig must provide non-empty " +
            "where_clause_fields")

    is_table_function = isinstance(query_config, TableFunctionQueryConfig)
    if is_table_function:
      table_function_config = cast(TableFunctionQueryConfig, query_config)
      if not table_function_config.where_clause_value_fn:
        raise ValueError(
            "TableFunctionQueryConfig must provide " + "where_clause_value_fn")
  else:
    raise ValueError("Invalid query_config type provided")


class DatabaseTypeAdapter(Enum):
  POSTGRESQL = "psycopg2"
  MYSQL = "pymysql"
  SQLSERVER = "pymysql"

  def to_sqlalchemy_dialect(self):
    """Map the adapter type to its corresponding SQLAlchemy dialect.

    Returns:
        str: SQLAlchemy dialect string.
    """
    if self == DatabaseTypeAdapter.POSTGRESQL:
      return f"postgresql+{self.value}"
    elif self == DatabaseTypeAdapter.MYSQL:
      return f"mysql+{self.value}"
    elif self == DatabaseTypeAdapter.SQLSERVER:
      return f"mssql+{self.value}"
    else:
      raise ValueError(f"Unsupported adapter type: {self.name}")


class CloudSQLEnrichmentHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
  """Enrichment handler for Cloud SQL databases.

  This handler is designed to work with the
  :class:`apache_beam.transforms.enrichment.Enrichment` transform.

  To use this handler, you need to provide one of the following query configs:
    * CustomQueryConfig - For providing a custom query function
    * TableFieldsQueryConfig - For specifying table, where clause, and fields
    * TableFunctionQueryConfig - For specifying table, where clause, and val fn

  By default, the handler retrieves all columns from the specified table.
  To limit the columns, use the `column_names` parameter to specify
  the desired column names.

  This handler queries the Cloud SQL database per element by default.
  To enable batching, set the `min_batch_size` and `max_batch_size` parameters.
  These values control the batching behavior in the
  :class:`apache_beam.transforms.utils.BatchElements` transform.

  NOTE: Batching is not supported when using the CustomQueryConfig.
  """
  def __init__(
      self,
      database_type_adapter: DatabaseTypeAdapter,
      database_address: str,
      database_user: str,
      database_password: str,
      database_id: str,
      *,
      query_config: QueryConfig,
      column_names: Optional[list[str]] = None,
      min_batch_size: int = 1,
      max_batch_size: int = 10000,
      **kwargs,
  ):
    """
    Example Usage:
      handler = CloudSQLEnrichmentHandler(
          database_type_adapter=adapter,
          database_address='127.0.0.1:5432',
          database_user='user',
          database_password='password',
          database_id='my_database',
          query_config=TableFieldsQueryConfig('my_table',"id = '{}'",['id']),
          min_batch_size=2,
          max_batch_size=100)

    Args:
      database_type_adapter: Adapter to handle specific database type operations
        (e.g., MySQL, PostgreSQL).
      database_address (str): Address or hostname of the Cloud SQL database, in
        the form `<ip>:<port>`. The port is optional if the database uses
        the default port.
      database_user (str): Username for accessing the database.
      database_password (str): Password for accessing the database.
      database_id (str): Identifier for the database to query.
      query_config: Configuration for database queries. Must be one of:
        * CustomQueryConfig: For providing a custom query function
        * TableFieldsQueryConfig: specifies table, where clause, and field names
        * TableFunctionQueryConfig: specifies table, where clause, and val func
      column_names (Optional[list[str]]): List of column names to select from
        the Cloud SQL table. If not provided, all columns (`*`) are selected.
      min_batch_size (int): Minimum number of rows to batch together when
        querying the database. Defaults to 1 if `query_fn` is not used.
      max_batch_size (int): Maximum number of rows to batch together. Defaults
        to 10,000 if `query_fn` is not used.
      **kwargs: Additional keyword arguments for database connection or query
        handling.

    Note:
      * Cannot use `min_batch_size` or `max_batch_size` with `query_fn`.
      * Either `where_clause_fields` or `where_clause_value_fn` must be provided
        for query construction if `query_fn` is not provided.
      * Ensure that the database user has the necessary permissions to query the
        specified table.
    """
    _validate_query_config(query_config)
    self._database_type_adapter = database_type_adapter
    self._database_id = database_id
    self._database_user = database_user
    self._database_password = database_password
    self._database_address = database_address
    self._query_config = query_config
    self._column_names = ",".join(column_names) if column_names else "*"
    self.kwargs = kwargs
    self._batching_kwargs = {}
    table_query_configs = (TableFieldsQueryConfig, TableFunctionQueryConfig)
    if isinstance(query_config, table_query_configs):
      self.query_template = (
          f"SELECT {self._column_names} "
          f"FROM {query_config.table_id} "
          f"WHERE {query_config.where_clause_template}")
      self._batching_kwargs['min_batch_size'] = min_batch_size
      self._batching_kwargs['max_batch_size'] = max_batch_size

  def __enter__(self):
    db_url = self._get_db_url()
    self._engine = create_engine(db_url)
    self._connection = self._engine.connect()

  def _get_db_url(self) -> str:
    dialect = self._database_type_adapter.to_sqlalchemy_dialect()
    url = (
        f"{dialect}://{self._database_user}:{self._database_password}"
        f"@{self._database_address}/{self._database_id}")
    return url

  def _execute_query(self, query: str, is_batch: bool, **params):
    try:
      result = self._connection.execute(text(query), **params)
      if is_batch:
        return [row._asdict() for row in result]
      else:
        return result.first()._asdict()
    except RuntimeError as e:
      raise RuntimeError(
          f'Could not execute the query: {query}. Please check if '
          f'the query is properly formatted and the BigQuery '
          f'table exists. {e}')

  def __call__(self, request: Union[beam.Row, list[beam.Row]], *args, **kwargs):
    if isinstance(request, list):
      values, responses = [], []
      requests_map: dict[Any, Any] = {}
      batch_size = len(request)
      raw_query = self.query_template

      # For multiple requests in the batch, combine the WHERE clause conditions
      # using 'OR' and update the query template to handle all requests.
      table_query_configs = (TableFieldsQueryConfig, TableFunctionQueryConfig)
      if batch_size > 1 and isinstance(self._query_config, table_query_configs):
        where_clause_template_batched = ' OR '.join(
            [fr'({self._query_config.where_clause_template})'] * batch_size)
        raw_query = self.query_template.replace(
            self._query_config.where_clause_template,
            where_clause_template_batched)

      # Extract where_clause_fields values and map the generated request key to
      # the original request object.
      for req in request:
        request_dict = req._asdict()
        try:
          if isinstance(self._query_config, TableFunctionQueryConfig):
            current_values = self._query_config.where_clause_value_fn(req)
          elif isinstance(self._query_config, TableFieldsQueryConfig):
            current_values = [
                request_dict[field]
                for field in self._query_config.where_clause_fields
            ]
        except KeyError as e:
          raise KeyError(
              "Make sure the values passed in `where_clause_fields` are the "
              "keys in the input `beam.Row`." + str(e))
        values.extend(current_values)
        requests_map[self.create_row_key(req)] = req

      # Formulate the query, execute it, and return a list of original requests
      # paired with their responses.
      query = raw_query.format(*values)
      responses_dict = self._execute_query(query, is_batch=True)
      for response in responses_dict:
        response_row = beam.Row(**response)
        response_key = self.create_row_key(response_row)
        if response_key in requests_map:
          responses.append((requests_map[response_key], response_row))
      return responses
    else:
      request_dict = request._asdict()
      if isinstance(self._query_config, CustomQueryConfig):
        query = self._query_config.query_fn(request)
      else:
        try:
          if isinstance(self._query_config, TableFunctionQueryConfig):
            values = self._query_config.where_clause_value_fn(request)
          elif isinstance(self._query_config, TableFieldsQueryConfig):
            values = [
                request_dict[field]
                for field in self._query_config.where_clause_fields
            ]
        except KeyError as e:
          raise KeyError(
              "Make sure the values passed in `where_clause_fields` are the "
              "keys in the input `beam.Row`." + str(e))
        query = self.query_template.format(*values)
      response_dict = self._execute_query(query, is_batch=False)
      return request, beam.Row(**response_dict)

  def create_row_key(self, row: beam.Row):
    if isinstance(self._query_config, TableFunctionQueryConfig):
      return tuple(self._query_config.where_clause_value_fn(row))
    if isinstance(self._query_config, TableFieldsQueryConfig):
      row_dict = row._asdict()
      return (
          tuple(
              row_dict[where_clause_field]
              for where_clause_field in self._query_config.where_clause_fields))
    raise ValueError(
        "Either where_clause_fields or where_clause_value_fn must be specified")

  def __exit__(self, exc_type, exc_val, exc_tb):
    self._connection.close()
    self._engine.dispose(close=True)
    self._engine, self._connection = None, None

  def get_cache_key(self, request: Union[beam.Row, list[beam.Row]]):
    if isinstance(self._query_config, CustomQueryConfig):
      raise NotImplementedError(
          "Caching is not supported for CustomQueryConfig. "
          "Consider using TableFieldsQueryConfig or " +
          "TableFunctionQueryConfig instead.")

    if isinstance(request, list):
      cache_keys = []
      for req in request:
        req_dict = req._asdict()
        try:
          if isinstance(self._query_config, TableFunctionQueryConfig):
            current_values = self._query_config.where_clause_value_fn(req)
          elif isinstance(self._query_config, TableFieldsQueryConfig):
            current_values = [
                req_dict[field]
                for field in self._query_config.where_clause_fields
            ]
          key = ";".join(["%s"] * len(current_values))
          cache_keys.extend([key % tuple(current_values)])
        except KeyError as e:
          raise KeyError(
              "Make sure the values passed in `where_clause_fields` are the "
              "keys in the input `beam.Row`." + str(e))
      return cache_keys
    else:
      req_dict = request._asdict()
      try:
        if isinstance(self._query_config, TableFunctionQueryConfig):
          current_values = self._query_config.where_clause_value_fn(request)
        else:  # TableFieldsQueryConfig
          current_values = [
              req_dict[field]
              for field in self._query_config.where_clause_fields
          ]
        key = ";".join(["%s"] * len(current_values))
        cache_key = key % tuple(current_values)
      except KeyError as e:
        raise KeyError(
            "Make sure the values passed in `where_clause_fields` are the "
            "keys in the input `beam.Row`." + str(e))
      return cache_key

  def batch_elements_kwargs(self) -> Mapping[str, Any]:
    """Returns a kwargs suitable for `beam.BatchElements`."""
    return self._batching_kwargs
