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

"""Enrichment handler for vector similarity search with pgvector.

Searches tables in PostgreSQL, AlloyDB for PostgreSQL and Cloud SQL for
PostgreSQL that store embeddings in a pgvector ``vector`` column, such as the
tables written by
:class:`~apache_beam.ml.rag.ingestion.postgres.PostgresVectorWriterConfig`.
"""

import math
import re
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from enum import Enum
from typing import Any
from typing import Optional
from typing import Union

from google.cloud.sql.connector import Connector as CloudSQLConnector
from sqlalchemy import create_engine
from sqlalchemy import exc as sqlalchemy_exc
from sqlalchemy import text

from apache_beam.io.requestresponse import UserCodeQuotaException
from apache_beam.io.requestresponse import UserCodeTimeoutException
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.transforms.enrichment_handlers.cloudsql import CloudSQLConnectionConfig
from apache_beam.transforms.enrichment_handlers.cloudsql import ConnectionConfig
from apache_beam.transforms.enrichment_handlers.cloudsql import DatabaseTypeAdapter
from apache_beam.transforms.enrichment_handlers.cloudsql import ExternalSQLDBConnectionConfig

__all__ = [
    'CloudSQLConnectionConfig',
    'DatabaseTypeAdapter',
    'ExternalSQLDBConnectionConfig',
    'PostgresVectorDistance',
    'PostgresVectorSearchParameters',
    'PostgresVectorSearchResult',
    'PostgresVectorSearchEnrichmentHandler',
]

# A table name, optionally qualified by a schema, or a column name. Names are
# used unquoted, as PostgresVectorWriterConfig does, so they follow the usual
# Postgres rules (unquoted names are case-insensitive).
_TABLE_NAME_RE = re.compile(
    r'^[A-Za-z_][A-Za-z0-9_$]*(\.[A-Za-z_][A-Za-z0-9_$]*)?$')
_COLUMN_NAME_RE = re.compile(r'^[A-Za-z_][A-Za-z0-9_$]*$')
# Named bind parameters such as `:language`, matched the way SQLAlchemy's
# `text()` matches them, so `::jsonb` casts are not mistaken for parameters.
_BIND_PARAM_RE = re.compile(r'(?<![:\w\x5c]):(\w+)(?!:)')

# Column aliases used in the generated query. They start with underscores so
# that they don't clash with user columns.
_QUERY_INDEX_ALIAS = '__beam_query_index'
_DISTANCE_ALIAS = '__beam_distance'

# Postgres SQLSTATE codes that Enrichment should retry.
_QUERY_CANCELED = '57014'  # For example, statement_timeout was reached.
_TOO_MANY_CONNECTIONS = '53300'


class PostgresVectorDistance(Enum):
  """pgvector distance operators.

  Smaller values mean more similar vectors for every operator, so results are
  always ordered by ascending distance.
  """
  #: Cosine distance (``<=>``), from 0 for vectors with the same direction to
  #: 2 for opposite ones. A zero vector has no direction, so its distances are
  #: NaN and its matches are in no particular order.
  COSINE = '<=>'
  #: Euclidean (L2) distance (``<->``).
  L2 = '<->'
  #: Negative inner product (``<#>``). pgvector returns the inner product
  #: multiplied by -1, so that smaller values are more similar.
  INNER_PRODUCT = '<#>'


@dataclass
class PostgresVectorSearchParameters:
  """Parameters for a pgvector similarity search.

  The defaults match the table layout written by
  :class:`~apache_beam.ml.rag.ingestion.postgres.PostgresVectorWriterConfig`
  with its default column specs: an ``id`` column, an ``embedding`` column of
  type ``vector``, and ``content`` and ``metadata`` columns.

  Example::

      params = PostgresVectorSearchParameters(
          table_name='embeddings',
          limit=5,
          distance=PostgresVectorDistance.COSINE,
          # Only search documents in the same language as the query.
          filter_template="metadata->>'language' = :language",
          filter_params_fn=lambda item: {
              'language': item.metadata['language']
          })

  Table and column names are used unquoted, as in
  ``PostgresVectorWriterConfig``, so they are case-insensitive and can't be SQL
  reserved words such as ``user`` or ``order``.

  Args:
    table_name: Table to search, optionally qualified by a schema (for
      example ``public.embeddings``).
    embedding_column: Column of type ``vector`` that holds the embeddings.
      Defaults to ``embedding``.
    id_column: Column that identifies a row. Defaults to ``id``.
    columns: Other columns to return for each match. Defaults to
      ``['content', 'metadata']``. Use an empty list to return only the ids
      and distances. Values of type ``vector`` are returned in pgvector's text
      form, such as ``'[1,0.5]'``.
    limit: Maximum number of matches to return per item. Must be positive.
      Defaults to 3.
    distance: The pgvector distance operator. Defaults to cosine distance.
    filter_template: Optional SQL condition that a row must meet to be
      returned, for example ``"metadata->>'language' = :language"``. Use named
      parameters (``:name``) for values that depend on the item, rather than
      formatting them into the string: the values are sent separately from
      the query, so they can't change it. With an approximate (HNSW or
      IVFFlat) index, Postgres applies the filter to the rows the index scan
      returns, so a selective filter can return fewer than ``limit`` rows. See
      the pgvector documentation on filtering for ways to avoid this.
    filter_params_fn: Function that returns the values of the parameters in
      ``filter_template`` for an item, as a dict from parameter name to value.
      Required when ``filter_template`` has parameters.
  """
  table_name: str
  embedding_column: str = 'embedding'
  id_column: str = 'id'
  columns: list[str] = field(default_factory=lambda: ['content', 'metadata'])
  limit: int = 3
  distance: PostgresVectorDistance = PostgresVectorDistance.COSINE
  filter_template: Optional[str] = None
  filter_params_fn: Optional[Callable[[EmbeddableItem], dict[str, Any]]] = None

  def __post_init__(self):
    if not self.table_name or not _TABLE_NAME_RE.match(self.table_name):
      raise ValueError(
          'table_name must be a table name, optionally qualified by a '
          f'schema, such as "embeddings" or "public.embeddings"; got '
          f'{self.table_name!r}')
    for name, value in (('embedding_column', self.embedding_column),
                        ('id_column', self.id_column)):
      _check_column_name(name, value)
    for column in self.columns:
      _check_column_name('columns', column)
    # Unquoted names are case-insensitive in Postgres.
    selected = [c.lower() for c in (self.id_column, *self.columns)]
    duplicates = sorted({c for c in selected if selected.count(c) > 1})
    if duplicates:
      raise ValueError(
          f'id_column and columns must not repeat a column, got {duplicates}')
    is_int = isinstance(self.limit, int) and not isinstance(self.limit, bool)
    if not is_int or self.limit <= 0:
      raise ValueError(f'limit must be a positive integer, got {self.limit!r}')
    if not isinstance(self.distance, PostgresVectorDistance):
      raise ValueError(
          'distance must be a PostgresVectorDistance, got '
          f'{self.distance!r}')
    if self.filter_template is None:
      if self.filter_params_fn is not None:
        raise ValueError('filter_params_fn requires filter_template')
    else:
      if not self.filter_template.strip():
        raise ValueError('filter_template must not be empty')
      if self.filter_param_names and self.filter_params_fn is None:
        raise ValueError(
            'filter_template has parameters '
            f'{sorted(self.filter_param_names)}, so filter_params_fn must be '
            'set to provide their values')

  @property
  def filter_param_names(self) -> set[str]:
    """Names of the parameters in ``filter_template``."""
    if not self.filter_template:
      return set()
    return set(_BIND_PARAM_RE.findall(self.filter_template))


def _check_column_name(arg_name: str, value: str):
  if not isinstance(value, str) or not _COLUMN_NAME_RE.match(value):
    raise ValueError(
        f'{arg_name} must contain column names made of letters, digits, _ and '
        f'$ that do not start with a digit, got {value!r}')


@dataclass
class PostgresVectorSearchResult:
  """Search result for one item.

  Args:
    id: Values of ``id_column`` for the matching rows, most similar first.
    distance: Distance between the item's embedding and each match, see
      :class:`PostgresVectorDistance`.
    fields: For each match, a dict from each of ``columns`` to its value.
  """
  id: list[Any] = field(default_factory=list)
  distance: list[float] = field(default_factory=list)
  fields: list[dict[str, Any]] = field(default_factory=list)


InputT, OutputT = (Union[EmbeddableItem, list[EmbeddableItem]],
                    list[tuple[EmbeddableItem, dict[str, Any]]])


class PostgresVectorSearchEnrichmentHandler(EnrichmentSourceHandler[InputT,
                                                                    OutputT]):
  """Enrichment handler for pgvector similarity search.

  This handler is designed to work with the
  :class:`apache_beam.transforms.enrichment.Enrichment` transform. For each
  :class:`~apache_beam.ml.rag.types.EmbeddableItem`, it finds the rows of a
  table whose embeddings are nearest to the item's dense embedding, and adds
  them to ``item.metadata['enrichment_data']`` as a
  :class:`PostgresVectorSearchResult` dict.

  It works with PostgreSQL, AlloyDB for PostgreSQL and Cloud SQL for
  PostgreSQL databases that have the
  `pgvector <https://github.com/pgvector/pgvector>`_ extension. Connect with
  :class:`~apache_beam.transforms.enrichment_handlers.cloudsql.CloudSQLConnectionConfig`
  for Cloud SQL, or with
  :class:`~apache_beam.transforms.enrichment_handlers.cloudsql.ExternalSQLDBConnectionConfig`
  for any database reachable over TCP, using
  ``DatabaseTypeAdapter.POSTGRESQL``.

  Elements are batched by :class:`apache_beam.transforms.util.BatchElements`,
  and each batch is searched with a single query. Each item in the query gets
  its own ``ORDER BY ... LIMIT`` search, so a pgvector index on the embedding
  column is used for every item.

  Errors for a statement timeout or too many connections are raised as
  :class:`~apache_beam.io.requestresponse.UserCodeTimeoutException` and
  :class:`~apache_beam.io.requestresponse.UserCodeQuotaException`, so that
  ``Enrichment`` retries them. Other errors propagate unchanged.

  Example::

      connection_config = ExternalSQLDBConnectionConfig(
          db_adapter=DatabaseTypeAdapter.POSTGRESQL,
          host='10.0.0.5',
          port=5432,
          user='beam',
          password='...',
          db_id='rag')
      handler = PostgresVectorSearchEnrichmentHandler(
          connection_config,
          PostgresVectorSearchParameters(table_name='embeddings', limit=5))
      enriched = items | Enrichment(handler)
  """
  def __init__(
      self,
      connection_config: ConnectionConfig,
      search_parameters: PostgresVectorSearchParameters,
      *,
      min_batch_size: int = 1,
      max_batch_size: int = 100):
    """
    Args:
      connection_config: How to connect to the database. Must use
        ``DatabaseTypeAdapter.POSTGRESQL``.
      search_parameters: The table to search and how to search it.
      min_batch_size: Minimum number of items to search with one query.
        Defaults to 1.
      max_batch_size: Maximum number of items to search with one query.
        Defaults to 100. Each item adds a search to the query, so larger
        batches make longer queries. Set to 1 to search once per item.
    """
    db_adapter = getattr(connection_config, 'db_adapter', None)
    if db_adapter is not None and db_adapter != DatabaseTypeAdapter.POSTGRESQL:
      raise ValueError(
          'PostgresVectorSearchEnrichmentHandler requires '
          f'DatabaseTypeAdapter.POSTGRESQL, got {db_adapter}')
    self._connection_config = connection_config
    self._search_parameters = search_parameters
    self._batching_kwargs = {
        'min_batch_size': min_batch_size, 'max_batch_size': max_batch_size
    }
    self._engine = None
    self._cloudsql_connector = None
    self.join_fn = join_fn
    self.use_custom_types = True

  def __enter__(self):
    if self._engine is None:
      try:
        self._engine = create_engine(
            url=self._connection_config.get_db_url(),
            creator=self._create_connection_factory())
      except BaseException:
        # __exit__ isn't called when __enter__ fails, so release the Cloud SQL
        # connector here.
        self.__exit__(None, None, None)
        raise
    return self

  def _create_connection_factory(self) -> Callable[[], Any]:
    config = self._connection_config
    if not isinstance(config, CloudSQLConnectionConfig):
      return config.get_connector_handler()
    # Create the Cloud SQL connector here, rather than through
    # CloudSQLConnectionConfig.get_connector_handler(), so that __exit__ can
    # close it and stop its background thread.
    connector = CloudSQLConnector(
        refresh_strategy=config.refresh_strategy, **config.connector_kwargs)
    self._cloudsql_connector = connector

    def connect():
      return connector.connect(
          instance_connection_string=config.instance_connection_uri,
          driver=config.db_adapter.value,
          user=config.user,
          password=config.password,
          db=config.db_id,
          **config.connect_kwargs)

    return connect

  def __call__(
      self,
      request: Union[EmbeddableItem, list[EmbeddableItem]],
      *args,
      **kwargs) -> list[tuple[EmbeddableItem, dict[str, Any]]]:
    items = request if isinstance(request, list) else [request]
    if not items:
      return []
    query, params = _build_query(self._search_parameters, items)
    try:
      with self._engine.connect() as connection:
        rows = connection.execute(text(query), params).mappings().all()
    except sqlalchemy_exc.TimeoutError as e:
      # No connection became available from the pool in time.
      raise UserCodeTimeoutException(str(e)) from e
    except sqlalchemy_exc.DBAPIError as e:
      code = _sqlstate(e)
      if code == _QUERY_CANCELED:
        raise UserCodeTimeoutException(str(e)) from e
      if code == _TOO_MANY_CONNECTIONS:
        raise UserCodeQuotaException(str(e)) from e
      raise
    return _group_results(self._search_parameters, items, rows)

  def __exit__(self, exc_type, exc_val, exc_tb):
    try:
      if self._engine is not None:
        self._engine.dispose()
    finally:
      self._engine = None
      if self._cloudsql_connector is not None:
        try:
          self._cloudsql_connector.close()
        finally:
          self._cloudsql_connector = None

  def batch_elements_kwargs(self) -> dict[str, int]:
    """Returns kwargs for beam.BatchElements."""
    return self._batching_kwargs


def _build_query(
    search_parameters: PostgresVectorSearchParameters,
    items: list[EmbeddableItem]) -> tuple[str, dict[str, Any]]:
  """Builds one query that searches for the nearest rows to each item.

  Each item gets its own `ORDER BY distance LIMIT n` subquery, which lets
  Postgres use a pgvector index, and the subqueries are combined with
  UNION ALL. Values are passed as bind parameters, never formatted into the
  query.
  """
  p = search_parameters
  select_columns = ', '.join([p.id_column, *p.columns])
  param_names = p.filter_param_names
  params: dict[str, Any] = {}
  searches = []
  for i, item in enumerate(items):
    vector_param = f'query_vector_{i}'
    params[vector_param] = _vector_literal(item)
    distance = (
        f'{p.embedding_column} {p.distance.value} '
        f'CAST(:{vector_param} AS vector)')
    where = ''
    if p.filter_template:
      where = f' WHERE ({_rename_params(p.filter_template, i)})'
      if param_names:
        params.update(_filter_params(p, item, i, param_names))
    searches.append(
        f'(SELECT {i} AS {_QUERY_INDEX_ALIAS}, {select_columns}, '
        f'{distance} AS {_DISTANCE_ALIAS} FROM {p.table_name}{where} '
        f'ORDER BY {distance} LIMIT {p.limit})')
  query = (
      f'SELECT * FROM ({" UNION ALL ".join(searches)}) AS search_results '
      f'ORDER BY {_QUERY_INDEX_ALIAS}, {_DISTANCE_ALIAS}')
  return query, params


def _vector_literal(item: EmbeddableItem) -> str:
  """Formats an item's dense embedding as a pgvector literal."""
  embedding = item.dense_embedding
  if not embedding:
    raise ValueError(
        f'Item {item.id} is missing the dense embedding required for vector '
        'search')
  values = [float(x) for x in embedding]
  if not all(math.isfinite(x) for x in values):
    # pgvector rejects these, which would fail the query for the whole batch.
    raise ValueError(
        f'Item {item.id} has a dense embedding with NaN or infinite values')
  return '[' + ','.join(repr(x) for x in values) + ']'


def _rename_params(template: str, index: int) -> str:
  """Gives the parameters in a filter a unique name for each item."""
  return _BIND_PARAM_RE.sub(lambda m: f':filter_{index}_{m.group(1)}', template)


def _filter_params(
    search_parameters: PostgresVectorSearchParameters,
    item: EmbeddableItem,
    index: int,
    param_names: set[str]) -> dict[str, Any]:
  values = search_parameters.filter_params_fn(item)
  if not isinstance(values, dict):
    raise ValueError(
        'filter_params_fn must return a dict of parameter values, got '
        f'{type(values).__name__} for item {item.id}')
  missing = sorted(param_names - values.keys())
  if missing:
    raise ValueError(
        f'filter_params_fn returned no value for parameters {missing} of '
        f'filter_template for item {item.id}')
  return {f'filter_{index}_{name}': values[name] for name in param_names}


def _group_results(
    search_parameters: PostgresVectorSearchParameters,
    items: list[EmbeddableItem],
    rows) -> list[tuple[EmbeddableItem, dict[str, Any]]]:
  p = search_parameters
  results = [PostgresVectorSearchResult() for _ in items]
  for row in rows:
    # Postgres folds unquoted names to lower case in the result columns.
    result = results[row[_QUERY_INDEX_ALIAS]]
    result.id.append(row[p.id_column.lower()])
    result.distance.append(row[_DISTANCE_ALIAS])
    result.fields.append({c: row[c.lower()] for c in p.columns})
  return [(item, result.__dict__) for item, result in zip(items, results)]


def _sqlstate(error: sqlalchemy_exc.DBAPIError) -> Optional[str]:
  """Returns the Postgres SQLSTATE code of a pg8000 error, if any."""
  args = getattr(error.orig, 'args', ())
  if args and isinstance(args[0], dict):
    return args[0].get('C')
  return None


def join_fn(left: EmbeddableItem, right: dict[str, Any]) -> EmbeddableItem:
  return replace(left, metadata={**left.metadata, 'enrichment_data': right})
