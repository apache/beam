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
from abc import abstractmethod, ABC
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict
from typing import List
from typing import Optional
from typing import Union

import pymysql
import pg8000
import pytds
from sqlalchemy import create_engine
from sqlalchemy import text
from sqlalchemy.engine import Connection as DBAPIConnection
from google.cloud.sql.connector.enums import RefreshStrategy
from google.cloud.sql.connector import Connector as CloudSQLConnector

import apache_beam as beam
from apache_beam.transforms.enrichment import EnrichmentSourceHandler

QueryFn = Callable[[beam.Row], str]
ConditionValueFn = Callable[[beam.Row], list[Any]]


@dataclass
class CustomQueryConfig:
  """Configuration for using a custom query function."""
  query_fn: QueryFn

  def __post_init__(self):
    if not self.query_fn:
      raise ValueError("CustomQueryConfig must provide a valid query_fn")


@dataclass
class TableFieldsQueryConfig:
  """Configuration for using table name, where clause, and field names."""
  table_id: str
  where_clause_template: str
  where_clause_fields: List[str]

  def __post_init__(self):
    if not self.table_id or not self.where_clause_template:
      raise ValueError(
          "TableFieldsQueryConfig and " +
          "TableFunctionQueryConfig must provide table_id " +
          "and where_clause_template")

    if not self.where_clause_fields:
      raise ValueError(
          "TableFieldsQueryConfig must provide non-empty " +
          "where_clause_fields")


@dataclass
class TableFunctionQueryConfig:
  """Configuration for using table name, where clause, and a value function."""
  table_id: str
  where_clause_template: str
  where_clause_value_fn: ConditionValueFn

  def __post_init__(self):
    if not self.table_id or not self.where_clause_template:
      raise ValueError(
          "TableFieldsQueryConfig and " +
          "TableFunctionQueryConfig must provide table_id " +
          "and where_clause_template")

    if not self.where_clause_value_fn:
      raise ValueError(
          "TableFunctionQueryConfig must provide " + "where_clause_value_fn")


class DatabaseTypeAdapter(Enum):
  POSTGRESQL = "pg8000"
  MYSQL = "pymysql"
  SQLSERVER = "pytds"

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
      raise ValueError(f"Unsupported database adapter type: {self.name}")


class ConnectionConfig(ABC):
  @abstractmethod
  def get_connector_handler(self) -> Callable[[], DBAPIConnection]:
    pass

  @abstractmethod
  def get_db_url(self) -> str:
    pass


@dataclass
class CloudSQLConnectionConfig(ConnectionConfig):
    """Connects to Google Cloud SQL using Cloud SQL Python Connector.

    Args:
        db_adapter: The database adapter type (PostgreSQL, MySQL, SQL Server).
        instance_connection_uri: URI for connecting to the Cloud SQL instance.
        user: Username for authentication.
        password: Password for authentication. Defaults to None.
        db_id: Database identifier/name.
        refresh_strategy: Strategy for refreshing connection (default: LAZY).
        connector_kwargs: Additional keyword arguments for the
          Cloud SQL Python Connector. Enables forward compatibility.
        connect_kwargs: Additional keyword arguments for the client connect
          method. Enables forward compatibility.
    """
    db_adapter: DatabaseTypeAdapter
    instance_connection_uri: str
    user: str = field(default_factory=str)
    password: str = field(default_factory=str)
    db_id: str = field(default_factory=str)
    refresh_strategy: RefreshStrategy = RefreshStrategy.LAZY
    connector_kwargs: Dict[str, Any] = field(default_factory=dict)
    connect_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
      if not self.instance_connection_uri:
          raise ValueError("Instance connection URI cannot be empty")

    def get_connector_handler(self) -> Callable[[], DBAPIConnection]:
      """Returns a function that creates a new database connection.

      The returned connector function creates database connections that should
      be properly closed by the caller when no longer needed.
      """
      cloudsql_client = CloudSQLConnector(
          refresh_strategy=self.refresh_strategy,
          **self.connector_kwargs)

      cloudsql_connector = lambda: cloudsql_client.connect(
          instance_connection_string=self.instance_connection_uri,
          driver=self.db_adapter.value,
          user=self.user,
          password=self.password,
          db=self.db_id,
          **self.connect_kwargs)

      return cloudsql_connector

    def get_db_url(self) -> str:
        return self.db_adapter.to_sqlalchemy_dialect() + "://"


@dataclass
class ExternalSQLDBConnectionConfig(ConnectionConfig):
    """Connects to External SQL DBs (PostgreSQL, MySQL, SQL Server) over TCP.

    Args:
        db_adapter: The database adapter type (PostgreSQL, MySQL, SQL Server).
        host: Hostname or IP address of the database server.
        port: Port number for the database connection.
        user: Username for authentication.
        password: Password for authentication.
        db_id: Database identifier/name.
        connect_kwargs: Additional keyword arguments for the client connect
          method. Enables forward compatibility.
    """
    db_adapter: DatabaseTypeAdapter
    host: str
    port: int
    user: str = field(default_factory=str)
    password: str  = field(default_factory=str)
    db_id: str = field(default_factory=str)
    connect_kwargs: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
      if not self.host:
          raise ValueError("Database host cannot be empty")

    def get_connector_handler(self) -> Callable[[], DBAPIConnection]:
      """Returns a function that creates a new database connection.

      The returned connector function creates database connections that should
      be properly closed by the caller when no longer needed.
      """
      if self.db_adapter == DatabaseTypeAdapter.POSTGRESQL:
        sql_connector = lambda: pg8000.connect(
            host=self.host, port=self.port, database=self.db_id,
            user=self.user, password=self.password, **self.connect_kwargs)
      elif self.db_adapter == DatabaseTypeAdapter.MYSQL:
        sql_connector = lambda: pymysql.connect(
            host=self.host, port=self.port, database=self.db_id,
            user=self.user, password=self.password, **self.connect_kwargs)
      elif self.db_adapter == DatabaseTypeAdapter.SQLSERVER:
        sql_connector = lambda: pytds.connect(
            dsn=self.host, port=self.port, database=self.db_id, user=self.user,
            password=self.password, **self.connect_kwargs)
      return sql_connector

    def get_db_url(self) -> str:
        return self.db_adapter.to_sqlalchemy_dialect() + "://"


QueryConfig = Union[CustomQueryConfig,
                    TableFieldsQueryConfig,
                    TableFunctionQueryConfig]


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
      connection_config: ConnectionConfig,
      *,
      query_config: QueryConfig,
      column_names: Optional[list[str]] = None,
      min_batch_size: int = 1,
      max_batch_size: int = 10000,
      **kwargs,
  ):
    """
    Example Usage:
      connection_config = CloudSQLConnectionConfig(
        db_adapter=DatabaseTypeAdapter.POSTGRESQL,
        instance_connection_uri="apache-beam-testing:us-central1:itests",
        user=postgres,
        password= os.getenv("CLOUDSQL_PG_PASSWORD"))
      query_config=TableFieldsQueryConfig('my_table',"id = '{}'",['id']),
      cloudsql_handler = CloudSQLEnrichmentHandler(
        connection_config=connection_config,
        query_config=query_config,
        min_batch_size=2,
        max_batch_size=100)

    Args:
      connection_config (ConnectionConfig): Configuration for connecting to
        the SQL database. Must be an instance of a subclass of
        `ConnectionConfig`, such as `CloudSQLConnectionConfig` or
        `ExternalSQLDBConnectionConfig`. This determines how the handler
        connects to the target SQL database.
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
    self._connection_config = connection_config
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
    connector = self._connection_config.get_connector_handler()
    self._engine = create_engine(
        url=self._connection_config.get_db_url(),
        creator=connector)

  def _execute_query(self, query: str, is_batch: bool, **params):
    try:
      connection = self._engine.connect()
      transaction = connection.begin()
      try:
          result = connection.execute(text(query), **params)
          # Materialize results while transaction is active.
          if is_batch:
            data = [row._asdict() for row in result]
          else:
            data = result.first()._asdict()
          # Explicitly commit the transaction.
          transaction.commit()
          return data
      except Exception as e:
        transaction.rollback()
        raise RuntimeError(f"Database operation failed: {e}")
    except Exception as e:
      raise Exception(
            f'Could not execute the query: {query}. Please check if '
            f'the query is properly formatted and the table exists. {e}')
    finally:
      if connection:
        connection.close()

  def __call__(self, request: Union[beam.Row, list[beam.Row]], *args, **kwargs):
    """Handle requests by delegating to single or batch processing."""
    if isinstance(request, list):
      return self._process_batch_request(request)
    else:
      return self._process_single_request(request)

  def _process_batch_request(self, requests: list[beam.Row]):
    """Process batch requests and match responses to original requests."""
    values, responses = [], []
    requests_map: dict[Any, Any] = {}
    batch_size = len(requests)
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
    for req in requests:
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
            "Make sure the values passed in `where_clause_fields` are "
            " thekeys in the input `beam.Row`." + str(e))
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

  def _process_single_request(self, request: beam.Row):
    """Process a single request and return with its response."""
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
            "Make sure the values passed in `where_clause_fields` are "
            "the keys in the input `beam.Row`." + str(e))
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
    self._engine.dispose(close=True)
    self._engine = None

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
