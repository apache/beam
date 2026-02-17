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

"""Cloud Spanner vector store writer for RAG pipelines.

This module provides a writer for storing embeddings and associated metadata
in Google Cloud Spanner. It supports flexible schema configuration with the
ability to flatten metadata fields into dedicated columns.

Example usage:

    Default schema (id, embedding, content, metadata):
    >>> config = SpannerVectorWriterConfig(
    ...     project_id="my-project",
    ...     instance_id="my-instance",
    ...     database_id="my-db",
    ...     table_name="embeddings"
    ... )

    Flattened metadata fields:
    >>> specs = (
    ...     SpannerColumnSpecsBuilder()
    ...     .with_id_spec()
    ...     .with_embedding_spec()
    ...     .with_content_spec()
    ...     .add_metadata_field("source", str)
    ...     .add_metadata_field("page_number", int, default=0)
    ...     .with_metadata_spec()
    ...     .build()
    ... )
    >>> config = SpannerVectorWriterConfig(
    ...     project_id="my-project",
    ...     instance_id="my-instance",
    ...     database_id="my-db",
    ...     table_name="embeddings",
    ...     column_specs=specs
    ... )

Spanner schema example:

    CREATE TABLE embeddings (
        id STRING(1024) NOT NULL,
        embedding ARRAY<FLOAT32>(vector_length=>768),
        content STRING(MAX),
        source STRING(MAX),
        page_number INT64,
        metadata JSON
    ) PRIMARY KEY (id)
"""

import functools
import json
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import List
from typing import Literal
from typing import NamedTuple
from typing import Optional
from typing import Type

import apache_beam as beam
from apache_beam.coders import registry
from apache_beam.coders.row_coder import RowCoder
from apache_beam.io.gcp import spanner
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.types import Chunk


@dataclass
class SpannerColumnSpec:
  """Column specification for Spanner vector writes.
  
  Defines how to extract and format values from Chunks for insertion into
  Spanner table columns. Each spec maps to one column in the target table.
  
  Attributes:
      column_name: Name of the Spanner table column
      python_type: Python type for the NamedTuple field (required for RowCoder)
      value_fn: Function to extract value from a Chunk
  
  Examples:
      String column:
      >>> SpannerColumnSpec(
      ...     column_name="id",
      ...     python_type=str,
      ...     value_fn=lambda chunk: chunk.id
      ... )
      
      Array column with conversion:
      >>> SpannerColumnSpec(
      ...     column_name="embedding",
      ...     python_type=List[float],
      ...     value_fn=lambda chunk: chunk.embedding.dense_embedding
      ... )
  """
  column_name: str
  python_type: Type
  value_fn: Callable[[Chunk], Any]


def _extract_and_convert(extract_fn, convert_fn, chunk):
  if convert_fn:
    return convert_fn(extract_fn(chunk))
  return extract_fn(chunk)


class SpannerColumnSpecsBuilder:
  """Builder for creating Spanner column specifications.
  
  Provides a fluent API for defining table schemas and how to populate them
  from Chunk objects. Supports standard Chunk fields (id, embedding, content,
  metadata) and flattening metadata fields into dedicated columns.
  
  Example:
      >>> specs = (
      ...     SpannerColumnSpecsBuilder()
      ...     .with_id_spec()
      ...     .with_embedding_spec()
      ...     .with_content_spec()
      ...     .add_metadata_field("source", str)
      ...     .with_metadata_spec()
      ...     .build()
      ... )
  """
  def __init__(self):
    self._specs: List[SpannerColumnSpec] = []

  @staticmethod
  def with_defaults() -> 'SpannerColumnSpecsBuilder':
    """Create builder with default schema.
    
    Default schema includes:
    - id (STRING): Chunk ID
    - embedding (ARRAY<FLOAT32>): Dense embedding vector
    - content (STRING): Chunk content text
    - metadata (JSON): Full metadata as JSON
    
    Returns:
        Builder with default column specifications
    """
    return (
        SpannerColumnSpecsBuilder().with_id_spec().with_embedding_spec().
        with_content_spec().with_metadata_spec())

  def with_id_spec(
      self,
      column_name: str = "id",
      python_type: Type = str,
      convert_fn: Optional[Callable[[str], Any]] = None
  ) -> 'SpannerColumnSpecsBuilder':
    """Add ID column specification.
    
    Args:
        column_name: Column name (default: "id")
        python_type: Python type (default: str)
        convert_fn: Optional converter (e.g., to cast to int)
    
    Returns:
        Self for method chaining
    
    Examples:
        Default string ID:
        >>> builder.with_id_spec()
        
        Integer ID with conversion:
        >>> builder.with_id_spec(
        ...     python_type=int,
        ...     convert_fn=lambda id: int(id.split('_')[1])
        ... )
    """

    self._specs.append(
        SpannerColumnSpec(
            column_name=column_name,
            python_type=python_type,
            value_fn=functools.partial(
                _extract_and_convert, lambda chunk: chunk.id, convert_fn)))
    return self

  def with_embedding_spec(
      self,
      column_name: str = "embedding",
      convert_fn: Optional[Callable[[List[float]], List[float]]] = None
  ) -> 'SpannerColumnSpecsBuilder':
    """Add embedding array column (ARRAY<FLOAT32> or ARRAY<FLOAT64>).
    
    Args:
        column_name: Column name (default: "embedding")
        convert_fn: Optional converter (e.g., normalize, quantize)
    
    Returns:
        Self for method chaining
    
    Examples:
        Default embedding:
        >>> builder.with_embedding_spec()
        
        Normalized embedding:
        >>> def normalize(vec):
        ...     norm = (sum(x**2 for x in vec) ** 0.5) or 1.0
        ...     return [x/norm for x in vec]
        >>> builder.with_embedding_spec(convert_fn=normalize)
        
        Rounded precision:
        >>> builder.with_embedding_spec(
        ...     convert_fn=lambda vec: [round(x, 4) for x in vec]
        ... )
    """
    def extract_fn(chunk: Chunk) -> List[float]:
      if chunk.embedding is None or chunk.embedding.dense_embedding is None:
        raise ValueError(f'Chunk must contain embedding: {chunk}')
      return chunk.embedding.dense_embedding

    self._specs.append(
        SpannerColumnSpec(
            column_name=column_name,
            python_type=List[float],
            value_fn=functools.partial(
                _extract_and_convert, extract_fn, convert_fn)))
    return self

  def with_content_spec(
      self,
      column_name: str = "content",
      python_type: Type = str,
      convert_fn: Optional[Callable[[str], Any]] = None
  ) -> 'SpannerColumnSpecsBuilder':
    """Add content column.
    
    Args:
        column_name: Column name (default: "content")
        python_type: Python type (default: str)
        convert_fn: Optional converter
    
    Returns:
        Self for method chaining
    
    Examples:
        Default text content:
        >>> builder.with_content_spec()
        
        Content length as integer:
        >>> builder.with_content_spec(
        ...     column_name="content_length",
        ...     python_type=int,
        ...     convert_fn=lambda text: len(text.split())
        ... )
        
        Truncated content:
        >>> builder.with_content_spec(
        ...     convert_fn=lambda text: text[:1000]
        ... )
    """
    def extract_fn(chunk: Chunk) -> str:
      if chunk.content.text is None:
        raise ValueError(f'Chunk must contain content: {chunk}')
      return chunk.content.text

    self._specs.append(
        SpannerColumnSpec(
            column_name=column_name,
            python_type=python_type,
            value_fn=functools.partial(
                _extract_and_convert, extract_fn, convert_fn)))
    return self

  def with_metadata_spec(
      self, column_name: str = "metadata") -> 'SpannerColumnSpecsBuilder':
    """Add metadata JSON column.
    
    Stores the full metadata dictionary as a JSON string in Spanner.
    
    Args:
        column_name: Column name (default: "metadata")
    
    Returns:
        Self for method chaining
    
    Note:
        Metadata is automatically converted to JSON string using json.dumps()
    """
    value_fn = lambda chunk: json.dumps(chunk.metadata)
    self._specs.append(
        SpannerColumnSpec(
            column_name=column_name, python_type=str, value_fn=value_fn))
    return self

  def add_metadata_field(
      self,
      field: str,
      python_type: Type,
      column_name: Optional[str] = None,
      convert_fn: Optional[Callable[[Any], Any]] = None,
      default: Any = None) -> 'SpannerColumnSpecsBuilder':
    """Flatten a metadata field into its own column.
    
    Extracts a specific field from chunk.metadata and stores it in a
    dedicated table column.
    
    Args:
        field: Key in chunk.metadata to extract
        python_type: Python type (must be explicitly specified)
        column_name: Column name (default: same as field)
        convert_fn: Optional converter for type casting/transformation
        default: Default value if field is missing from metadata
    
    Returns:
        Self for method chaining
    
    Examples:
        String field:
        >>> builder.add_metadata_field("source", str)
        
        Integer with default:
        >>> builder.add_metadata_field(
        ...     "page_number",
        ...     int,
        ...     default=0
        ... )
        
        Float with conversion:
        >>> builder.add_metadata_field(
        ...     "confidence",
        ...     float,
        ...     convert_fn=lambda x: round(float(x), 2),
        ...     default=0.0
        ... )
        
        List of strings:
        >>> builder.add_metadata_field(
        ...     "tags",
        ...     List[str],
        ...     default=[]
        ... )
        
        Timestamp with conversion:
        >>> builder.add_metadata_field(
        ...     "created_at",
        ...     str,
        ...     convert_fn=lambda ts: ts.isoformat()
        ... )
    """
    name = column_name or field

    def value_fn(chunk: Chunk) -> Any:
      return chunk.metadata.get(field, default)

    self._specs.append(
        SpannerColumnSpec(
            column_name=name,
            python_type=python_type,
            value_fn=functools.partial(
                _extract_and_convert, value_fn, convert_fn)))
    return self

  def add_column(
      self,
      column_name: str,
      python_type: Type,
      value_fn: Callable[[Chunk], Any]) -> 'SpannerColumnSpecsBuilder':
    """Add a custom column with full control.
    
    Args:
        column_name: Column name
        python_type: Python type (required)
        value_fn: Value extraction function
    
    Returns:
        Self for method chaining
    
    Examples:
        Boolean flag:
        >>> builder.add_column(
        ...     column_name="has_code",
        ...     python_type=bool,
        ...     value_fn=lambda chunk: "```" in chunk.content.text
        ... )
        
        Computed value:
        >>> builder.add_column(
        ...     column_name="word_count",
        ...     python_type=int,
        ...     value_fn=lambda chunk: len(chunk.content.text.split())
        ... )
    """
    self._specs.append(
        SpannerColumnSpec(
            column_name=column_name, python_type=python_type,
            value_fn=value_fn))
    return self

  def build(self) -> List[SpannerColumnSpec]:
    """Build the final list of column specifications.
    
    Returns:
        List of SpannerColumnSpec objects
    """
    return self._specs.copy()


class _SpannerSchemaBuilder:
  """Internal: Builds NamedTuple schema and registers RowCoder.
  
  Creates a NamedTuple type from column specifications and registers it
  with Beam's RowCoder for serialization.
  """
  def __init__(self, table_name: str, column_specs: List[SpannerColumnSpec]):
    """Initialize schema builder.
    
    Args:
        table_name: Table name (used in NamedTuple type name)
        column_specs: List of column specifications
    
    Raises:
        ValueError: If duplicate column names are found
    """
    self.table_name = table_name
    self.column_specs = column_specs

    # Validate no duplicates
    names = [col.column_name for col in column_specs]
    duplicates = set(name for name in names if names.count(name) > 1)
    if duplicates:
      raise ValueError(f"Duplicate column names: {duplicates}")

    # Create NamedTuple type
    fields = [(col.column_name, col.python_type) for col in column_specs]
    type_name = f"SpannerVectorRecord_{table_name}"
    self.record_type = NamedTuple(type_name, fields)  # type: ignore

    # Register coder
    registry.register_coder(self.record_type, RowCoder)

  def create_converter(self) -> Callable[[Chunk], NamedTuple]:
    """Create converter function from Chunk to NamedTuple record.
    
    Returns:
        Function that converts a Chunk to a NamedTuple record
    """
    def convert(chunk: Chunk) -> self.record_type:  # type: ignore
      values = {
          col.column_name: col.value_fn(chunk)
          for col in self.column_specs
      }
      return self.record_type(**values)  # type: ignore

    return convert


class SpannerVectorWriterConfig(VectorDatabaseWriteConfig):
  """Configuration for writing vectors to Cloud Spanner.
  
  Supports flexible schema configuration through column specifications and
  provides control over Spanner-specific write parameters.
  
  Examples:
      Default schema:
      >>> config = SpannerVectorWriterConfig(
      ...     project_id="my-project",
      ...     instance_id="my-instance",
      ...     database_id="my-db",
      ...     table_name="embeddings"
      ... )
      
      Custom schema with flattened metadata:
      >>> specs = (
      ...     SpannerColumnSpecsBuilder()
      ...     .with_id_spec()
      ...     .with_embedding_spec()
      ...     .with_content_spec()
      ...     .add_metadata_field("source", str)
      ...     .add_metadata_field("page_number", int, default=0)
      ...     .with_metadata_spec()
      ...     .build()
      ... )
      >>> config = SpannerVectorWriterConfig(
      ...     project_id="my-project",
      ...     instance_id="my-instance",
      ...     database_id="my-db",
      ...     table_name="embeddings",
      ...     column_specs=specs
      ... )
      
      With emulator:
      >>> config = SpannerVectorWriterConfig(
      ...     project_id="test-project",
      ...     instance_id="test-instance",
      ...     database_id="test-db",
      ...     table_name="embeddings",
      ...     emulator_host="http://localhost:9010"
      ... )
  """
  def __init__(
      self,
      project_id: str,
      instance_id: str,
      database_id: str,
      table_name: str,
      *,
      # Schema configuration
      column_specs: Optional[List[SpannerColumnSpec]] = None,
      # Write operation type
      write_mode: Literal["INSERT", "UPDATE", "REPLACE",
                          "INSERT_OR_UPDATE"] = "INSERT_OR_UPDATE",
      # Batching configuration
      max_batch_size_bytes: Optional[int] = None,
      max_number_mutations: Optional[int] = None,
      max_number_rows: Optional[int] = None,
      grouping_factor: Optional[int] = None,
      # Networking
      host: Optional[str] = None,
      emulator_host: Optional[str] = None,
      expansion_service: Optional[str] = None,
      # Retry/deadline configuration
      commit_deadline: Optional[int] = None,
      max_cumulative_backoff: Optional[int] = None,
      # Error handling
      failure_mode: Optional[
          spanner.FailureMode] = spanner.FailureMode.REPORT_FAILURES,
      high_priority: bool = False,
      # Additional Spanner arguments
      **spanner_kwargs):
    """Initialize Spanner vector writer configuration.
    
    Args:
        project_id: GCP project ID
        instance_id: Spanner instance ID
        database_id: Spanner database ID
        table_name: Target table name
        column_specs: Schema configuration using SpannerColumnSpecsBuilder.
            If None, uses default schema (id, embedding, content, metadata)
        write_mode: Spanner write operation type:
            - INSERT: Fail if row exists
            - UPDATE: Fail if row doesn't exist
            - REPLACE: Delete then insert
            - INSERT_OR_UPDATE: Insert or update if exists (default)
        max_batch_size_bytes: Maximum bytes per mutation batch (default: 1MB)
        max_number_mutations: Maximum cell mutations per batch (default: 5000)
        max_number_rows: Maximum rows per batch (default: 500)
        grouping_factor: Multiple of max mutation for sorting (default: 1000)
        host: Spanner host URL (usually not needed)
        emulator_host: Spanner emulator host (e.g., "http://localhost:9010")
        expansion_service: Java expansion service address (host:port)
        commit_deadline: Commit API deadline in seconds (default: 15)
        max_cumulative_backoff: Max retry backoff seconds (default: 900)
        failure_mode: Error handling strategy:
            - FAIL_FAST: Throw exception for any failure
            - REPORT_FAILURES: Continue processing (default)
        high_priority: Use high priority for operations (default: False)
        **spanner_kwargs: Additional keyword arguments to pass to the
            underlying Spanner write transform. Use this to pass any
            Spanner-specific parameters not explicitly exposed by this config.
    """
    self.project_id = project_id
    self.instance_id = instance_id
    self.database_id = database_id
    self.table_name = table_name
    self.write_mode = write_mode
    self.max_batch_size_bytes = max_batch_size_bytes
    self.max_number_mutations = max_number_mutations
    self.max_number_rows = max_number_rows
    self.grouping_factor = grouping_factor
    self.host = host
    self.emulator_host = emulator_host
    self.expansion_service = expansion_service
    self.commit_deadline = commit_deadline
    self.max_cumulative_backoff = max_cumulative_backoff
    self.failure_mode = failure_mode
    self.high_priority = high_priority
    self.spanner_kwargs = spanner_kwargs

    # Use defaults if not provided
    specs = column_specs or SpannerColumnSpecsBuilder.with_defaults().build()

    # Create schema builder (NamedTuple + RowCoder registration)
    self.schema_builder = _SpannerSchemaBuilder(table_name, specs)

  def create_write_transform(self) -> beam.PTransform:
    """Create the Spanner write PTransform.
    
    Returns:
        PTransform for writing to Spanner
    """
    return _WriteToSpannerVectorDatabase(self)


class _WriteToSpannerVectorDatabase(beam.PTransform):
  """Internal: PTransform for writing to Spanner vector database."""
  def __init__(self, config: SpannerVectorWriterConfig):
    """Initialize write transform.
    
    Args:
        config: Spanner writer configuration
    """
    self.config = config
    self.schema_builder = config.schema_builder

  def expand(self, pcoll: beam.PCollection[Chunk]):
    """Expand the transform.
    
    Args:
        pcoll: PCollection of Chunks to write
    """
    # Select appropriate Spanner write transform based on write_mode
    write_transform_class = {
        "INSERT": spanner.SpannerInsert,
        "UPDATE": spanner.SpannerUpdate,
        "REPLACE": spanner.SpannerReplace,
        "INSERT_OR_UPDATE": spanner.SpannerInsertOrUpdate,
    }[self.config.write_mode]

    return (
        pcoll
        | "Convert to Records" >> beam.Map(
            self.schema_builder.create_converter()).with_output_types(
                self.schema_builder.record_type)
        | "Write to Spanner" >> write_transform_class(
            project_id=self.config.project_id,
            instance_id=self.config.instance_id,
            database_id=self.config.database_id,
            table=self.config.table_name,
            max_batch_size_bytes=self.config.max_batch_size_bytes,
            max_number_mutations=self.config.max_number_mutations,
            max_number_rows=self.config.max_number_rows,
            grouping_factor=self.config.grouping_factor,
            host=self.config.host,
            emulator_host=self.config.emulator_host,
            commit_deadline=self.config.commit_deadline,
            max_cumulative_backoff=self.config.max_cumulative_backoff,
            failure_mode=self.config.failure_mode,
            expansion_service=self.config.expansion_service,
            high_priority=self.config.high_priority,
            **self.config.spanner_kwargs))
