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
import logging
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

_LOGGER = logging.getLogger(__name__)


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

  def maybe_set_default_update_fields(self, columns: List[str]):
    if self.action != "UPDATE":
      return
    if self.update_fields is not None:
      return

    conflict_fields = ([self.on_conflict_fields] if isinstance(
        self.on_conflict_fields, str) else self.on_conflict_fields)
    self.conflict_resolution.update_fields = [
        col for col in columns if col not in conflict_fields
    ]

  def get_conflict_clause(self) -> str:
    """Get conflict clause with update fields."""
    conflict_fields = [self.on_conflict_fields] \
      if isinstance(self.on_conflict_fields, str) \
        else self.on_conflict_fields

    if self.action == "IGNORE":
      conflict_fields_string = f"({', '.join(conflict_fields)})" \
        if len(conflict_fields) > 0 else ""
      return f"ON CONFLICT {conflict_fields_string} DO NOTHING"

    # update_fields should be set by query builder before this is called
    assert self.update_fields is not None, \
      "update_fields must be set before generating conflict clause"
    updates = [f"{field} = EXCLUDED.{field}" for field in self.update_fields]
    return f"ON CONFLICT " \
      f"({', '.join(conflict_fields)}) DO UPDATE SET {', '.join(updates)}"


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


@dataclass
class ColumnSpec:
  """Specification for mapping Chunk fields to SQL columns for insertion.
    
    Defines how to extract and format values from Chunks into database columns,
    handling the full pipeline from Python value to SQL insertion.

    The insertion process works as follows:
    - value_fn extracts a value from the Chunk and formats it as needed
    - The value is stored in a NamedTuple field with the specified python_type
    - During SQL insertion, the value is bound to a ? placeholder

    Attributes:
        column_name: The column name in the database table.
        python_type: Python type for the NamedTuple field that will hold the
            value. Must be compatible with must be compatible with
            :class:`~apache_beam.coders.row_coder.RowCoder`.
        value_fn: Function to extract and format the value from a Chunk.
            Takes a Chunk and returns a value of python_type.
        sql_typecast: Optional SQL type cast to append to the ? placeholder.
            Common examples:
            - "::float[]" for vector arrays
            - "::jsonb" for JSON data
    
    Examples:
        Basic text column (uses standard JDBC type mapping):
        >>> ColumnSpec.text(
        ...     column_name="content",
        ...     value_fn=lambda chunk: chunk.content.text
        ... )
        # Results in: INSERT INTO table (content) VALUES (?)

        Vector column with explicit array casting:
        >>> ColumnSpec.vector(
        ...     column_name="embedding",
        ...     value_fn=lambda chunk: '{' + 
        ...         ','.join(map(str, chunk.embedding.dense_embedding)) + '}'
        ... )
        # Results in: INSERT INTO table (embedding) VALUES (?::float[])
        # The value_fn formats [1.0, 2.0] as '{1.0,2.0}' for PostgreSQL array

        Timestamp from metadata with explicit casting:
        >>> ColumnSpec(
        ...     column_name="created_at",
        ...     python_type=str,
        ...     value_fn=lambda chunk: chunk.metadata.get("timestamp"),
        ...     sql_typecast="::timestamp"
        ... )
        # Results in: INSERT INTO table (created_at) VALUES (?::timestamp)
        # Allows inserting string timestamps with proper PostgreSQL casting

    Factory Methods:
        text: Creates a text column specification (no type cast).
        integer: Creates an integer column specification (no type cast).
        float: Creates a float column specification (no type cast).
        vector: Creates a vector column specification with float[] casting.
        jsonb: Creates a JSONB column specification with jsonb casting.
    """
  column_name: str
  python_type: Type
  value_fn: Callable[[Chunk], Any]
  sql_typecast: Optional[str] = None

  @property
  def placeholder(self) -> str:
    """Get SQL placeholder with optional typecast."""
    return f"?{self.sql_typecast or ''}"

  @classmethod
  def text(
      cls, column_name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a text column specification."""
    return cls(column_name, str, value_fn)

  @classmethod
  def integer(
      cls, column_name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create an integer column specification."""
    return cls(column_name, int, value_fn)

  @classmethod
  def float(
      cls, column_name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a float column specification."""
    return cls(column_name, float, value_fn)

  @classmethod
  def vector(
      cls,
      column_name: str,
      value_fn: Callable[[Chunk], Any] = chunk_embedding_fn) -> 'ColumnSpec':
    """Create a vector column specification."""
    return cls(column_name, str, value_fn, "::float[]")

  @classmethod
  def jsonb(
      cls, column_name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a JSONB column specification."""
    return cls(column_name, str, value_fn, "::jsonb")


MetadataSpec = Union[ColumnSpec, Dict[str, ColumnSpec]]


def chunk_id_fn(chunk: Chunk) -> str:
  """Extract ID from chunk.
    
    Args:
        chunk: Input Chunk object.
    
    Returns:
        str: The chunk's ID.
    """
  return chunk.id


class _AlloyDBQueryBuilder:
  def __init__(
      self,
      table_name: str,
      *,
      column_specs: List[ColumnSpec],
      conflict_resolution: Optional[ConflictResolution] = None):
    """Builds SQL queries for writing Chunks with Embeddings to AlloyDB.
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


class ColumnSpecsBuilder:
  """Builder for :class:`.ColumnSpec`'s with chainable methods."""
  def __init__(self):
    self._specs: List[ColumnSpec] = []

  @staticmethod
  def with_defaults() -> 'ColumnSpecsBuilder':
    """Add all default column specifications."""
    return (
        ColumnSpecsBuilder().with_id_spec().with_embedding_spec().
        with_content_spec().with_metadata_spec())

  def with_id_spec(
      self,
      column_name: str = "id",
      python_type: Type = str,
      convert_fn: Optional[Callable[[str], Any]] = None,
      sql_typecast: Optional[str] = None) -> 'ColumnSpecsBuilder':
    """Add ID :class:`.ColumnSpec` with optional type and conversion.
        
        Args:
            column_name: Name for the ID column (defaults to "id")
            python_type: Python type for the column (defaults to str)
            convert_fn: Optional function to convert the chunk ID
                       If None, uses ID as-is
            sql_typecast: Optional SQL type cast
        
        Returns:
            Self for method chaining
        
        Example:
            >>> builder.with_id_spec(
            ...     column_name="doc_id",
            ...     python_type=int,
            ...     convert_fn=lambda id: int(id.split('_')[1])
            ... )
        """
    def value_fn(chunk: Chunk) -> Any:
      value = chunk.id
      return convert_fn(value) if convert_fn else value

    self._specs.append(
        ColumnSpec(
            column_name=column_name,
            python_type=python_type,
            value_fn=value_fn,
            sql_typecast=sql_typecast))
    return self

  def with_content_spec(
      self,
      column_name: str = "content",
      python_type: Type = str,
      convert_fn: Optional[Callable[[str], Any]] = None,
      sql_typecast: Optional[str] = None) -> 'ColumnSpecsBuilder':
    """Add content :class:`.ColumnSpec` with optional type and conversion.
      
      Args:
          column_name: Name for the content column (defaults to "content")
          python_type: Python type for the column (defaults to str)
          convert_fn: Optional function to convert the content text
                      If None, uses content text as-is
          sql_typecast: Optional SQL type cast
      
      Returns:
          Self for method chaining
      
      Example:
          >>> builder.with_content_spec(
          ...     column_name="content_length",
          ...     python_type=int,
          ...     convert_fn=len  # Store content length instead of content
          ... )
      """
    def value_fn(chunk: Chunk) -> Any:
      if chunk.content.text is None:
        raise ValueError(f'Expected chunk to contain content. {chunk}')
      value = chunk.content.text
      return convert_fn(value) if convert_fn else value

    self._specs.append(
        ColumnSpec(
            column_name=column_name,
            python_type=python_type,
            value_fn=value_fn,
            sql_typecast=sql_typecast))
    return self

  def with_metadata_spec(
      self,
      column_name: str = "metadata",
      python_type: Type = str,
      convert_fn: Optional[Callable[[Dict[str, Any]], Any]] = None,
      sql_typecast: Optional[str] = "::jsonb") -> 'ColumnSpecsBuilder':
    """Add metadata :class:`.ColumnSpec` with optional type and conversion.
      
      Args:
          column_name: Name for the metadata column (defaults to "metadata")
          python_type: Python type for the column (defaults to str)
          convert_fn: Optional function to convert the metadata dictionary
                      If None and python_type is str, converts to JSON string
          sql_typecast: Optional SQL type cast (defaults to "::jsonb")
      
      Returns:
          Self for method chaining
      
      Example:
          >>> builder.with_metadata_spec(
          ...     column_name="meta_tags",
          ...     python_type=list,
          ...     convert_fn=lambda meta: list(meta.keys()),
          ...     sql_typecast="::text[]"
          ... )
      """
    def value_fn(chunk: Chunk) -> Any:
      if convert_fn:
        return convert_fn(chunk.metadata)
      return json.dumps(
          chunk.metadata) if python_type == str else chunk.metadata

    self._specs.append(
        ColumnSpec(
            column_name=column_name,
            python_type=python_type,
            value_fn=value_fn,
            sql_typecast=sql_typecast))
    return self

  def with_embedding_spec(
      self,
      column_name: str = "embedding",
      convert_fn: Optional[Callable[[List[float]], Any]] = None
  ) -> 'ColumnSpecsBuilder':
    """Add embedding :class:`.ColumnSpec` with optional conversion.
      
      Args:
          column_name: Name for the embedding column (defaults to "embedding")
          convert_fn: Optional function to convert the dense embedding values
                      If None, uses default PostgreSQL array format
      
      Returns:
          Self for method chaining
      
      Example:
          >>> builder.with_embedding_spec(
          ...     column_name="embedding_vector",
          ...     convert_fn=lambda values: '{' + ','.join(f"{x:.4f}" 
          ...       for x in values) + '}'
          ... )
      """
    def value_fn(chunk: Chunk) -> Any:
      if chunk.embedding is None or chunk.embedding.dense_embedding is None:
        raise ValueError(f'Expected chunk to contain embedding. {chunk}')
      values = chunk.embedding.dense_embedding
      if convert_fn:
        return convert_fn(values)
      return '{' + ','.join(str(x) for x in values) + '}'

    self._specs.append(
        ColumnSpec.vector(column_name=column_name, value_fn=value_fn))
    return self

  def add_metadata_field(
      self,
      field: str,
      python_type: Type,
      column_name: Optional[str] = None,
      convert_fn: Optional[Callable[[Any], Any]] = None,
      default: Any = None,
      sql_typecast: Optional[str] = None) -> 'ColumnSpecsBuilder':
    """""Add a :class:`.ColumnSpec` that extracts and converts a field from
        chunk metadata.

        Args:
            field: Key to extract from chunk metadata
            python_type: Python type for the column (e.g. str, int, float)
            column_name: Name for the column (defaults to metadata field name)
            convert_fn: Optional function to convert the extracted value to
                      desired type. If None, value is used as-is
            default: Default value if field is missing from metadata
            sql_typecast: Optional SQL type cast (e.g. "::timestamp")
        
        Returns:
            Self for chaining

        Examples:
            Simple string field:
            >>> builder.add_metadata_field("source", str)

            Integer with default:
            >>> builder.add_metadata_field(
            ...     field="count",
            ...     python_type=int,
            ...     column_name="item_count",
            ...     default=0
            ... )

            Float with conversion and default:
            >>> builder.add_metadata_field(
            ...     field="confidence",
            ...     python_type=intfloat,
            ...     convert_fn=lambda x: round(float(x), 2),
            ...     default=0.0
            ... )

            Timestamp with conversion and type cast:
            >>> builder.add_metadata_field(
            ...     field="created_at",
            ...     python_type=intstr,
            ...     convert_fn=lambda ts: ts.replace('T', ' '),
            ...     sql_typecast="::timestamp"
            ... )
        """
    name = column_name or field

    def value_fn(chunk: Chunk) -> Any:
      value = chunk.metadata.get(field, default)
      if value is not None and convert_fn is not None:
        value = convert_fn(value)
      return value

    spec = ColumnSpec(
        column_name=name,
        python_type=python_type,
        value_fn=value_fn,
        sql_typecast=sql_typecast)

    self._specs.append(spec)
    return self

  def add_custom_column_spec(self, spec: ColumnSpec) -> 'ColumnSpecsBuilder':
    """Add a custom :class:`.ColumnSpec` to the builder.
    
    Use this method when you need complete control over the :class:`.ColumnSpec`
    , including custom value extraction and type handling.
    
    Args:
        spec: A :class:`.ColumnSpec` instance defining the column name, type,
            value extraction, and optional SQL type casting.
    
    Returns:
        Self for method chaining
    
    Examples:
        Custom text column from chunk metadata:
        >>> builder.add_custom_column_spec(
        ...     ColumnSpec.text(
        ...         name="source_and_id",
        ...         value_fn=lambda chunk: \
        ...             f"{chunk.metadata.get('source')}_{chunk.id}"
        ...     )
        ... )
    """
    self._specs.append(spec)
    return self

  def build(self) -> List[ColumnSpec]:
    """Build the final list of column specifications."""
    return self._specs.copy()


class AlloyDBVectorWriterConfig(VectorDatabaseWriteConfig):
  def __init__(
      self,
      connection_config: AlloyDBConnectionConfig,
      table_name: str,
      *,
      # pylint: disable=dangerous-default-value
      column_specs: List[ColumnSpec] = ColumnSpecsBuilder.with_defaults().build(
      ),
      conflict_resolution: Optional[ConflictResolution] = ConflictResolution(
          on_conflict_fields=[], action='IGNORE')):
    """Configuration for writing vectors to AlloyDB using managed transforms.
    
    Supports flexible schema configuration through column specifications and
    conflict resolution strategies.

    Args:
        connection_config: AlloyDB connection configuration.
        table_name: Target table name.
        column_specs: Column specifications. If None, uses default Chunk schema.
            Use ColumnSpecsBuilder to construct the specifications.
        conflict_resolution: Optional strategy for handling insert conflicts.
            ON CONFLICT DO NOTHING by default.
    
    Examples:
        Basic usage with default schema:
        >>> config = AlloyDBVectorWriterConfig(
        ...     connection_config=AlloyDBConnectionConfig(...),
        ...     table_name='embeddings'
        ... )

        Custom schema with metadata fields:
        >>> specs = (ColumnSpecsBuilder()
        ...         .with_id_spec()
        ...         .with_embedding_spec(column_name="embedding_vec")
        ...         .add_metadata_field("source")
        ...         .add_metadata_field(
        ...             "timestamp",
        ...             column_name="created_at",
        ...             sql_typecast="::timestamp"
        ...         )
        ...         .build())
        >>> config = AlloyDBVectorWriterConfig(
        ...     connection_config=AlloyDBConnectionConfig(...),
        ...     table_name='embeddings',
        ...     column_specs=specs
        ... )
    """
    self.connection_config = connection_config
    # NamedTuple is created and registered here during pipeline construction
    self.query_builder = _AlloyDBQueryBuilder(
        table_name,
        column_specs=column_specs,
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
