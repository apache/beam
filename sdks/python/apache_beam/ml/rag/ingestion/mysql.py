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

import logging
from abc import ABC
from abc import abstractmethod
from typing import Callable
from typing import List
from typing import NamedTuple
from typing import Optional

import apache_beam as beam
from apache_beam.coders import registry
from apache_beam.coders.row_coder import RowCoder
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.ingestion.jdbc_common import ConnectionConfig
from apache_beam.ml.rag.ingestion.jdbc_common import WriteConfig
from apache_beam.ml.rag.ingestion.mysql_common import ColumnSpec
from apache_beam.ml.rag.ingestion.mysql_common import ColumnSpecsBuilder
from apache_beam.ml.rag.ingestion.mysql_common import ConflictResolution
from apache_beam.ml.rag.types import Chunk

_LOGGER = logging.getLogger(__name__)


class _ConflictResolutionStrategy(ABC):
  """Abstract base class for conflict resolution strategies."""
  @abstractmethod
  def get_conflict_clause(self, all_columns: List[str]) -> str:
    """Generate the MySQL conflict clause."""
    pass


class _NoConflictStrategy(_ConflictResolutionStrategy):
  """Strategy for when no conflict resolution is needed."""
  def get_conflict_clause(self, all_columns: List[str]) -> str:
    return ""


class _UpdateStrategy(_ConflictResolutionStrategy):
  """Strategy for UPDATE action on conflict."""
  def __init__(self, update_fields: Optional[List[str]] = None):
    self.update_fields = update_fields

  def get_conflict_clause(self, all_columns: List[str]) -> str:
    # Use provided fields or default to all columns
    fields_to_update = self.update_fields or all_columns
    assert len(fields_to_update) > 0

    updates = [f"{field} = VALUES({field})" for field in fields_to_update]
    return f"ON DUPLICATE KEY UPDATE {', '.join(updates)}"


class _IgnoreStrategy(_ConflictResolutionStrategy):
  """Strategy for IGNORE action on conflict."""
  def __init__(self, primary_key_field: str):
    self.primary_key_field = primary_key_field

  def get_conflict_clause(self, all_columns: List[str]) -> str:
    return f"ON DUPLICATE KEY UPDATE {self.primary_key_field}"\
       f" = {self.primary_key_field}"


def _create_conflict_strategy(
    conflict_resolution: Optional[ConflictResolution]
) -> _ConflictResolutionStrategy:
  if conflict_resolution is None:
    return _NoConflictStrategy()
  if conflict_resolution.action == "UPDATE":
    return _UpdateStrategy(conflict_resolution.update_fields)
  if conflict_resolution.action == "IGNORE":
    assert conflict_resolution.primary_key_field is not None
    return _IgnoreStrategy(conflict_resolution.primary_key_field)
  raise ValueError(f"Unknown conflict resolution {conflict_resolution.action}")


class _MySQLQueryBuilder:
  def __init__(
      self,
      table_name: str,
      *,
      column_specs: List[ColumnSpec],
      conflict_resolution: Optional[ConflictResolution] = None):
    """Builds SQL queries for writing Chunks with Embeddings to MySQL.
    """
    self.table_name = table_name

    self.column_specs = column_specs
    self.conflict_resolution_strategy = _create_conflict_strategy(
        conflict_resolution)

    names = [col.column_name for col in self.column_specs]
    duplicates = set(name for name in names if names.count(name) > 1)
    if duplicates:
      raise ValueError(f"Duplicate column names found: {duplicates}")

    fields = [(col.column_name, col.python_type) for col in self.column_specs]
    type_name = f"VectorRecord_{table_name}"
    self.record_type = NamedTuple(type_name, fields)  # type: ignore

    registry.register_coder(self.record_type, RowCoder)

  def build_insert(self) -> str:
    fields = [col.column_name for col in self.column_specs]
    placeholders = [col.placeholder for col in self.column_specs]

    # Build base query
    query = f"""
        INSERT INTO {self.table_name}
        ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
    """
    conflict_clause = self.conflict_resolution_strategy.get_conflict_clause(
        all_columns=fields)
    query += f" {conflict_clause}"

    _LOGGER.info("MySQL Query with placeholders %s", query)
    return query

  def create_converter(self) -> Callable[[Chunk], NamedTuple]:
    """Creates a function to convert Chunks to records."""
    def convert(chunk: Chunk) -> self.record_type:  # type: ignore
      return self.record_type(
          **{col.column_name: col.value_fn(chunk)
             for col in self.column_specs})  # type: ignore

    return convert


class MySQLVectorWriterConfig(VectorDatabaseWriteConfig):
  def __init__(
      self,
      connection_config: ConnectionConfig,
      table_name: str,
      *,
      # pylint: disable=dangerous-default-value
      write_config: WriteConfig = WriteConfig(),
      column_specs: List[ColumnSpec] = ColumnSpecsBuilder.with_defaults().build(
      ),
      conflict_resolution: Optional[ConflictResolution] = None):
    """Configuration for writing vectors to MySQL using jdbc.
    
    Supports flexible schema configuration through column specifications and
    conflict resolution strategies with MySQL-specific syntax.

    Args:
        connection_config:
          :class:`~apache_beam.ml.rag.ingestion.jdbc_common.ConnectionConfig`.
        table_name: Target table name.
        write_config: JdbcIO :class:`~.jdbc_common.WriteConfig` to control
          batch sizes, authosharding, etc.
        column_specs:
            Use :class:`~.mysql_common.ColumnSpecsBuilder` to configure how
            embeddings and metadata are written to the database
            schema. If None, uses default Chunk schema with MySQL vector
            functions.
        conflict_resolution: Optional
            :class:`~.mysql_common.ConflictResolution`
            strategy for handling insert conflicts. ON DUPLICATE KEY UPDATE.
            None by default, meaning errors are thrown when attempting to insert
            duplicates.
    
    Examples:
        Simple case with default schema:

        >>> config = MySQLVectorWriterConfig(
        ...     connection_config=ConnectionConfig(...),
        ...     table_name='embeddings'
        ... )

        Custom schema with metadata fields and MySQL functions:

        >>> specs = (ColumnSpecsBuilder()
        ...         .with_id_spec(column_name="my_id_column")
        ...         .with_embedding_spec(
        ...             column_name="embedding_vec",
        ...             placeholder="string_to_vector(?)"
        ...         )
        ...         .add_metadata_field(field="source", column_name="src")
        ...         .add_metadata_field(
        ...             "timestamp",
        ...             column_name="created_at",
        ...             placeholder="STR_TO_DATE(?, '%Y-%m-%d %H:%i:%s')"
        ...         )
        ...         .build())

        Minimal schema (only ID + embedding written):

        >>> column_specs = (ColumnSpecsBuilder()
        ...     .with_id_spec()
        ...     .with_embedding_spec()
        ...     .build())

        >>> config = MySQLVectorWriterConfig(
        ...     connection_config=ConnectionConfig(...),
        ...     table_name='embeddings',
        ...     column_specs=specs,
        ...     conflict_resolution=ConflictResolution(
        ...         on_conflict_fields=["id"],
        ...         action="UPDATE",
        ...         update_fields=["embedding", "content"]
        ...     )
        ... )

        Using MySQL JSON functions:

        >>> specs = (ColumnSpecsBuilder()
        ...     .with_id_spec()
        ...     .with_embedding_spec()
        ...     .with_metadata_spec(
        ...         column_name="metadata_json",
        ...         placeholder="CAST(? AS JSON)"
        ...     )
        ...     .build())
    """
    self.connection_config = connection_config
    self.write_config = write_config
    # NamedTuple is created and registered here during pipeline construction
    self.query_builder = _MySQLQueryBuilder(
        table_name,
        column_specs=column_specs,
        conflict_resolution=conflict_resolution)

  def create_write_transform(self) -> beam.PTransform:
    return _WriteToMySQLVectorDatabase(self)


class _WriteToMySQLVectorDatabase(beam.PTransform):
  """Implementation of MySQL vector database write."""
  def __init__(self, config: MySQLVectorWriterConfig):
    self.config = config
    self.query_builder = config.query_builder
    self.connection_config = config.connection_config
    self.write_config = config.write_config

  def expand(self, pcoll: beam.PCollection[Chunk]):
    return (
        pcoll
        |
        "Convert to Records" >> beam.Map(self.query_builder.create_converter())
        | "Write to MySQL" >> WriteToJdbc(
            table_name=self.query_builder.table_name,
            driver_class_name="com.mysql.cj.jdbc.Driver",
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
