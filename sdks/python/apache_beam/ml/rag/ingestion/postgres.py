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
from typing import Callable
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Optional
from typing import Union

import apache_beam as beam
from apache_beam.coders import registry
from apache_beam.coders.row_coder import RowCoder
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.ingestion.jdbc_common import ConnectionConfig
from apache_beam.ml.rag.ingestion.jdbc_common import WriteConfig
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpec
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpecsBuilder
from apache_beam.ml.rag.ingestion.postgres_common import ConflictResolution
from apache_beam.ml.rag.types import Chunk

_LOGGER = logging.getLogger(__name__)

MetadataSpec = Union[ColumnSpec, Dict[str, ColumnSpec]]


class _PostgresQueryBuilder:
  def __init__(
      self,
      table_name: str,
      *,
      column_specs: List[ColumnSpec],
      conflict_resolution: Optional[ConflictResolution] = None):
    """Builds SQL queries for writing Chunks with Embeddings to Postgres.
    """
    self.table_name = table_name

    self.column_specs = column_specs
    self.conflict_resolution = conflict_resolution

    # Validate no duplicate column names
    names = [col.column_name for col in self.column_specs]
    duplicates = set(name for name in names if names.count(name) > 1)
    if duplicates:
      raise ValueError(f"Duplicate column names found: {duplicates}")

    # Create NamedTuple type
    fields = [(col.column_name, col.python_type) for col in self.column_specs]
    type_name = f"VectorRecord_{table_name}"
    self.record_type = NamedTuple(type_name, fields)  # type: ignore

    # Register coder
    registry.register_coder(self.record_type, RowCoder)

    # Set default update fields to all non-conflict fields if update fields are
    # not specified
    if self.conflict_resolution:
      self.conflict_resolution.maybe_set_default_update_fields(
          [col.column_name for col in self.column_specs if col.column_name])

  def build_insert(self) -> str:
    """Build INSERT query with proper type casting."""
    # Get column names and placeholders
    fields = [col.column_name for col in self.column_specs]
    placeholders = [col.placeholder for col in self.column_specs]

    # Build base query
    query = f"""
        INSERT INTO {self.table_name}
        ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
    """

    # Add conflict handling if configured
    if self.conflict_resolution:
      query += f" {self.conflict_resolution.get_conflict_clause()}"

    _LOGGER.info("Query with placeholders %s", query)
    return query

  def create_converter(self) -> Callable[[Chunk], NamedTuple]:
    """Creates a function to convert Chunks to records."""
    def convert(chunk: Chunk) -> self.record_type:  # type: ignore
      return self.record_type(
          **{col.column_name: col.value_fn(chunk)
             for col in self.column_specs})  # type: ignore

    return convert


class PostgresVectorWriterConfig(VectorDatabaseWriteConfig):
  def __init__(
      self,
      connection_config: ConnectionConfig,
      table_name: str,
      *,
      # pylint: disable=dangerous-default-value
      write_config: WriteConfig = WriteConfig(),
      column_specs: List[ColumnSpec] = ColumnSpecsBuilder.with_defaults().build(
      ),
      conflict_resolution: Optional[ConflictResolution] = ConflictResolution(
          on_conflict_fields=[], action='IGNORE')):
    """Configuration for writing vectors to Postgres using jdbc.
    
    Supports flexible schema configuration through column specifications and
    conflict resolution strategies.

    Args:
        connection_config:
          :class:`~apache_beam.ml.rag.ingestion.jdbc_common.ConnectionConfig`.
        table_name: Target table name.
        write_config: JdbcIO :class:`~.jdbc_common.WriteConfig` to control
          batch sizes, authosharding, etc.
        column_specs:
            Use :class:`~.postgres_common.ColumnSpecsBuilder` to configure how
            embeddings and metadata are written a database
            schema. If None, uses default Chunk schema.
        conflict_resolution: Optional
            :class:`~.postgres_common.ConflictResolution`
            strategy for handling insert conflicts. ON CONFLICT DO NOTHING by
            default.
    
    Examples:
        Simple case with default schema:

        >>> config = PostgresVectorWriterConfig(
        ...     connection_config=ConnectionConfig(...),
        ...     table_name='embeddings'
        ... )

        Custom schema with metadata fields:

        >>> specs = (ColumnSpecsBuilder()
        ...         .with_id_spec(column_name="my_id_column")
        ...         .with_embedding_spec(column_name="embedding_vec")
        ...         .add_metadata_field(field="source", column_name="src")
        ...         .add_metadata_field(
        ...             "timestamp",
        ...             column_name="created_at",
        ...             sql_typecast="::timestamp"
        ...         )
        ...         .build())

        Minimal schema (only ID + embedding written)

        >>> column_specs = (ColumnSpecsBuilder()
        ...     .with_id_spec()
        ...     .with_embedding_spec()
        ...     .build())

        >>> config = PostgresVectorWriterConfig(
        ...     connection_config=ConnectionConfig(...),
        ...     table_name='embeddings',
        ...     column_specs=specs
        ... )
    """
    self.connection_config = connection_config
    self.write_config = write_config
    # NamedTuple is created and registered here during pipeline construction
    self.query_builder = _PostgresQueryBuilder(
        table_name,
        column_specs=column_specs,
        conflict_resolution=conflict_resolution)

  def create_write_transform(self) -> beam.PTransform:
    return _WriteToPostgresVectorDatabase(self)


class _WriteToPostgresVectorDatabase(beam.PTransform):
  """Implementation of Postgres vector database write. """
  def __init__(self, config: PostgresVectorWriterConfig):
    self.config = config
    self.query_builder = config.query_builder
    self.connection_config = config.connection_config
    self.write_config = config.write_config

  def expand(self, pcoll: beam.PCollection[Chunk]):
    return (
        pcoll
        |
        "Convert to Records" >> beam.Map(self.query_builder.create_converter())
        | "Write to Postgres" >> WriteToJdbc(
            table_name=self.query_builder.table_name,
            driver_class_name="org.postgresql.Driver",
            jdbc_url=self.connection_config.jdbc_url,
            username=self.connection_config.username,
            password=self.connection_config.password,
            statement=self.query_builder.build_insert(),
            connection_properties=self.connection_config.connection_properties,
            connection_init_sqls=self.connection_config.connection_init_sqls,
            autosharding=self.write_config.autosharding,
            max_connections=self.write_config.max_connections,
            write_batch_size=self.write_config.write_batch_size,
            **self.connection_config.additional_jdbc_args))
