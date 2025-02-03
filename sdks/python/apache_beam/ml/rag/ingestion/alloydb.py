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

import json
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Literal
from typing import NamedTuple
from typing import Optional
from typing import Type
from typing import Union

import apache_beam as beam
from apache_beam.coders import registry
from apache_beam.coders.row_coder import RowCoder
from apache_beam.io.jdbc import WriteToJdbc
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.types import Chunk


@dataclass
class AlloyDBConnectionConfig:
  """Configuration for AlloyDB database connection.
    
    Provides connection details and options for connecting to an AlloyDB
    instance.
    
    Attributes:
        jdbc_url: JDBC URL for the AlloyDB instance.
            Example: 'jdbc:postgresql://host:port/database'
        username: Database username.
        password: Database password.
        connection_properties: Optional JDBC connection properties dict.
            Example: {'ssl': 'true'}
        connection_init_sqls: Optional list of SQL statements to execute when
            connection is established.
        autosharding: Enable automatic re-sharding of bundles to scale the
            number of shards with workers.
        max_connections: Optional number of connections in the pool.
            Use negative for no limit.
        write_batch_size: Optional write batch size for bulk operations.
    
    Example:
        >>> config = AlloyDBConnectionConfig(
        ...     jdbc_url='jdbc:postgresql://localhost:5432/mydb',
        ...     username='user',
        ...     password='pass',
        ...     connection_properties={'ssl': 'true'},
        ...     max_connections=10
        ... )
    """
  jdbc_url: str
  username: str
  password: str
  connection_properties: Optional[Dict[str, str]] = None
  connection_init_sqls: Optional[List[str]] = None
  autosharding: Optional[bool] = None
  max_connections: Optional[int] = None
  write_batch_size: Optional[int] = None


@dataclass
class ConflictResolution:
  """Specification for how to handle conflicts during insert.

    Configures conflict handling behavior when inserting records that may
    violate unique constraints.

    Attributes:
        on_conflict_fields: Field(s) that determine uniqueness. Can be a single
            field name or list of field names for composite constraints.
        action: How to handle conflicts - either "UPDATE" or "IGNORE".
            UPDATE: Updates existing record with new values.
            IGNORE: Skips conflicting records.
        update_fields: Optional list of fields to update on conflict. If None,
            all non-conflict fields are updated.
        
    Examples:
        Simple primary key:
        >>> ConflictResolution("id")
        
        Composite key with specific update fields:
        >>> ConflictResolution(
        ...     on_conflict_fields=["source", "timestamp"],
        ...     action="UPDATE",
        ...     update_fields=["embedding", "content"]
        ... )
        
        Ignore conflicts:
        >>> ConflictResolution(
        ...     on_conflict_fields="id",
        ...     action="IGNORE"
        ... )
    """
  on_conflict_fields: Union[str, List[str]]
  action: Literal["UPDATE", "IGNORE"] = "UPDATE"
  update_fields: Optional[List[str]] = None

  def get_conflict_clause(self) -> str:
    """Get conflict clause with update fields."""
    fields = [self.on_conflict_fields] \
      if isinstance(self.on_conflict_fields, str) \
        else self.on_conflict_fields

    if self.action == "IGNORE":
      return f"ON CONFLICT ({', '.join(fields)}) DO NOTHING"

    # update_fields should be set by query builder before this is called
    assert self.update_fields is not None, \
      "update_fields must be set before generating conflict clause"
    updates = [f"{field} = EXCLUDED.{field}" for field in self.update_fields]
    return f"ON CONFLICT " \
      f"({', '.join(fields)}) DO UPDATE SET {', '.join(updates)}"


@dataclass
class ColumnSpec:
  """Specification for mapping Chunk fields to SQL columns for insertion.
    
    Defines how to extract and format values from Chunks into database columns,
    including type conversion and SQL type casting.

    Attributes:
        name: The column name in the database table.
        python_type: Python type for the column value (used in NamedTuple field)
        value_fn: Function to extract and format the value from a Chunk.
            Takes a Chunk and returns a value of python_type.
        sql_typecast: Optional SQL type cast to apply (e.g. "::float[]" for 
            vectors).
    
    Examples:
        Basic text column:
        >>> ColumnSpec.text(
        ...     name="content",
        ...     value_fn=lambda chunk: chunk.content.text
        ... )

        Vector column with type casting:
        >>> ColumnSpec.vector(
        ...     name="embedding",
        ...     value_fn=lambda chunk: ('{' + 
        ...       ','.join(map(str, chunk.embedding.dense_embedding)) + '}')
        ... )

        Custom metadata column:
        >>> ColumnSpec.text(
        ...     name="source",
        ...     value_fn=lambda chunk: chunk.metadata.get("source", "unknown")
        ... )

        Timestamp column with type casting:
        >>> ColumnSpec(
        ...     name="timestamp",
        ...     python_type=str,
        ...     value_fn=lambda chunk: chunk.metadata.get("timestamp", ""),
        ...     sql_typecast="::timestamp"
        ... )

    Factory Methods:
        text: Creates a text column specification.
        integer: Creates an integer column specification.
        float: Creates a float column specification.
        vector: Creates a vector column specification with float[] casting.
        jsonb: Creates a JSONB column specification.
    """
  name: str  # Column name in database
  python_type: Type  # Type for NamedTuple field
  value_fn: Callable[[Chunk], Any]
  sql_typecast: Optional[str] = None  # e.g. "::float[]" for vectors

  @property
  def placeholder(self) -> str:
    """Get SQL placeholder with optional typecast."""
    return f"?{self.sql_typecast or ''}"

  @classmethod
  def text(cls, name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a text column specification."""
    return cls(name, str, value_fn)

  @classmethod
  def integer(cls, name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create an integer column specification."""
    return cls(name, int, value_fn)

  @classmethod
  def float(cls, name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a float column specification."""
    return cls(name, float, value_fn)

  @classmethod
  def vector(cls, name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a vector column specification."""
    return cls(name, str, value_fn, "::float[]")

  @classmethod
  def jsonb(cls, name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a JSONB column specification."""
    return cls(name, str, value_fn, "::jsonb")


MetadataSpec = Union[ColumnSpec, Dict[str, ColumnSpec]]


def chunk_id_fn(chunk: Chunk) -> str:
  """Extract ID from chunk.
    
    Args:
        chunk: Input Chunk object.
    
    Returns:
        str: The chunk's ID.
    """
  return chunk.id


def chunk_embedding_fn(chunk: Chunk) -> str:
  """Convert embedding to PostgreSQL array string.
    
    Formats dense embedding as a PostgreSQL-compatible array string.
    Example: [1.0, 2.0] -> '{1.0,2.0}'
    
    Args:
        chunk: Input Chunk object.
    
    Returns:
        str: PostgreSQL array string representation of the embedding.
    
    Raises:
        ValueError: If chunk has no dense embedding.
    """
  if chunk.embedding is None or chunk.embedding.dense_embedding is None:
    raise ValueError(f'Expected chunk to contain embedding. {chunk}')
  return '{' + ','.join(str(x) for x in chunk.embedding.dense_embedding) + '}'


def chunk_content_fn(chunk: Chunk) -> str:
  """Extract content text from chunk.
    
    Args:
        chunk: Input Chunk object.
    
    Returns:
        str: The chunk's content text.
    """
  if chunk.content.text is None:
    raise ValueError(f'Expected chunk to contain content. {chunk}')
  return chunk.content.text


def chunk_metadata_fn(chunk: Chunk) -> str:
  """Extract metadata from chunk as JSON string.
    
    Args:
        chunk: Input Chunk object.
    
    Returns:
        str: JSON string representation of the chunk's metadata.
    """
  return json.dumps(chunk.metadata)


class _AlloyDBQueryBuilder:
  def __init__(
      self,
      table_name: str,
      *,
      id_spec: Optional[ColumnSpec] = None,
      embedding_spec: Optional[ColumnSpec] = None,
      content_spec: Optional[ColumnSpec] = None,
      metadata_spec: Optional[MetadataSpec] = None,
      custom_column_specs: Optional[List[ColumnSpec]] = None,
      conflict_resolution: Optional[ConflictResolution] = None):
    """Builds SQL queries for writing Chunks with Embeddings to AlloyDB.
    """
    self.table_name = table_name

    # Store core specs
    self.id_spec = id_spec
    self.embedding_spec = embedding_spec
    self.content_spec = content_spec
    self.metadata_spec = metadata_spec
    self.custom_column_specs = custom_column_specs or []
    self.conflict_resolution = conflict_resolution

    # Get all column specs in order
    columns = []

    # Add core fields if specified
    if self.id_spec:
      columns.append(self.id_spec)
    if self.embedding_spec:
      columns.append(self.embedding_spec)
    if self.content_spec:
      columns.append(self.content_spec)

    # Add metadata fields
    if isinstance(self.metadata_spec, dict):
      columns.extend(self.metadata_spec.values())
    elif self.metadata_spec:
      columns.append(self.metadata_spec)

    # Add custom columns
    columns.extend(self.custom_column_specs)

    # Validate no duplicate column names
    names = [col.name for col in columns]
    duplicates = set(name for name in names if names.count(name) > 1)
    if duplicates:
      raise ValueError(f"Duplicate column names found: {duplicates}")

    # Create NamedTuple type
    fields = [(col.name, col.python_type) for col in columns]
    type_name = f"VectorRecord_{table_name}"
    self.record_type = NamedTuple(type_name, fields)  # type: ignore

    # Register coder
    registry.register_coder(self.record_type, RowCoder)

    # Store columns for SQL generation
    self.columns = columns

    # Set default update fields for conflict resolution
    if self.conflict_resolution and self.conflict_resolution.action == "UPDATE":
      if self.conflict_resolution.update_fields is None:
        conflict_fields = ([self.conflict_resolution.on_conflict_fields]
                           if isinstance(
                               self.conflict_resolution.on_conflict_fields, str)
                           else self.conflict_resolution.on_conflict_fields)
        self.conflict_resolution.update_fields = [
            col.name for col in columns if col.name not in conflict_fields
        ]

  def build_insert(self) -> str:
    """Build INSERT query with proper type casting."""
    # Get column names and placeholders
    fields = [col.name for col in self.columns]
    placeholders = [col.placeholder for col in self.columns]

    # Build base query
    query = f"""
        INSERT INTO {self.table_name}
        ({', '.join(fields)})
        VALUES ({', '.join(placeholders)})
    """

    # Add conflict handling if configured
    if self.conflict_resolution:
      query += f" {self.conflict_resolution.get_conflict_clause()}"

    return query

  def create_converter(self) -> Callable[[Chunk], NamedTuple]:
    """Creates a function to convert Chunks to records."""
    def convert(chunk: Chunk) -> self.record_type:  # type: ignore
      return self.record_type(
          **{col.name: col.value_fn(chunk)
             for col in self.columns})  # type: ignore

    return convert


class AlloyDBVectorWriterConfig(VectorDatabaseWriteConfig):
  def __init__(
      self,
      connection_config: AlloyDBConnectionConfig,
      table_name: str,
      *,
      id_spec: Optional[ColumnSpec] = ColumnSpec.text(
          name="id", value_fn=chunk_id_fn),
      embedding_spec: Optional[ColumnSpec] = ColumnSpec.vector(
          name="embedding", value_fn=chunk_embedding_fn),
      content_spec: Optional[ColumnSpec] = ColumnSpec.text(
          name="content", value_fn=chunk_content_fn),
      metadata_spec: Optional[MetadataSpec] = ColumnSpec.jsonb(
          name="metadata", value_fn=chunk_metadata_fn),
      custom_column_specs: Optional[List[ColumnSpec]] = None,
      conflict_resolution: Optional[ConflictResolution] = None):
    """Configuration for writing vectors to AlloyDB using managed transforms.
    
    Supports flexible schema configuration through column specifications and
    conflict resolution strategies.

    Args:
        connection_config: AlloyDB connection configuration.
        table_name: Target table name.
        id_spec: Optional specification for ID column.
            Defaults to text column named "id" using chunk_id_fn.
        embedding_spec: Optional specification for embedding column.
            Defaults to vector column named "embedding" using
            chunk_embedding_fn.
        content_spec: Optional specification for content column.
            Defaults to text column named "content" using chunk_content_fn.
        metadata_spec: Optional specification for metadata handling.
            Can be either:
            - Single ColumnSpec for JSON metadata column
            - Dict mapping metadata keys to individual column specs
            Defaults to JSONB column named "metadata" using chunk_metadata_fn.
        custom_column_specs: Optional list of custom column specifications.
        conflict_resolution: Optional strategy for handling insert conflicts.
    
    Examples:
        Basic usage with default schema:
        >>> config = AlloyDBVectorWriterConfig(
        ...     connection_config=AlloyDBConnectionConfig(...),
        ...     table_name='embeddings'
        ... )

        Custom column specifications:
        >>> config = AlloyDBVectorWriterConfig(
        ...     connection_config=AlloyDBConnectionConfig(...),
        ...     table_name='custom_embeddings',
        ...     id_spec=ColumnSpec.text(
        ...         name="custom_id",
        ...         value_fn=lambda chunk: \
        ....          f"timestamp_{chunk.metadata['timestamp']}"
        ...     ),
        ...     embedding_spec=ColumnSpec.vector(
        ...         name="embedding_vec",
        ...         value_fn=chunk_embedding_fn
        ...     ),
        ...     content_spec=None  # Omit content column
        ... )

        Metadata columns with conflict resolution:
        >>> config = AlloyDBVectorWriterConfig(
        ...     connection_config=AlloyDBConnectionConfig(...),
        ...     table_name='versioned_embeddings',
        ...     metadata_spec={
        ...         "source": ColumnSpec(
        ...             "source", str,
        ...             lambda chunk: chunk.metadata.get("source", "unknown")
        ...         ),
        ...         "timestamp": ColumnSpec(
        ...             "timestamp", str,
        ...             lambda chunk: chunk.metadata.get("timestamp", ""),
        ...             "::timestamp"
        ...         )
        ...     },
        ...     conflict_resolution=ConflictResolution(
        ...         on_conflict_fields=["source", "timestamp"],
        ...         action="UPDATE",
        ...         update_fields=["embedding", "content"]
        ...     )
        ... )
    """
    self.connection_config = connection_config
    # NamedTuple is created and registered here during pipeline construction
    self.query_builder = _AlloyDBQueryBuilder(
        table_name,
        id_spec=id_spec,
        embedding_spec=embedding_spec,
        content_spec=content_spec,
        metadata_spec=metadata_spec,
        custom_column_specs=custom_column_specs,
        conflict_resolution=conflict_resolution)

  def create_write_transform(self) -> beam.PTransform:
    return _WriteToAlloyDBVectorDatabase(self)


class _WriteToAlloyDBVectorDatabase(beam.PTransform):
  """Implementation of BigQuery vector database write. """
  def __init__(self, config: AlloyDBVectorWriterConfig):
    self.config = config

  def expand(self, pcoll: beam.PCollection[Chunk]):
    return (
        pcoll
        | "Convert to Records" >> beam.Map(
            self.config.query_builder.create_converter())
        | "Write to AlloyDB" >> WriteToJdbc(
            table_name=self.config.query_builder.table_name,
            driver_class_name="org.postgresql.Driver",
            jdbc_url=self.config.connection_config.jdbc_url,
            username=self.config.connection_config.username,
            password=self.config.connection_config.password,
            statement=self.config.query_builder.build_insert()))
