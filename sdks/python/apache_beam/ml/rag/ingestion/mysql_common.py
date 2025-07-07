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
from typing import Optional
from typing import Type

from apache_beam.ml.rag.types import Chunk


def chunk_embedding_fn(chunk: Chunk) -> str:
  """Convert embedding to MySQL vector string format.
    
    Formats dense embedding as a MySQL-compatible vector string.
    Example: [1.0, 2.0] -> '[1.0,2.0]'
    
    Args:
        chunk: Input Chunk object.
    
    Returns:
        str: MySQL vector string representation of the embedding.
    
    Raises:
        ValueError: If chunk has no dense embedding.
    """
  if chunk.embedding is None or chunk.embedding.dense_embedding is None:
    raise ValueError(f'Expected chunk to contain embedding. {chunk}')
  return '[' + ','.join(str(x) for x in chunk.embedding.dense_embedding) + ']'


@dataclass
class ColumnSpec:
  """Specification for mapping Chunk fields to MySQL columns for insertion.
    
    Defines how to extract and format values from Chunks into MySQL database
    columns, handling the full pipeline from Python value to SQL insertion.

    The insertion process works as follows:
    - value_fn extracts a value from the Chunk and formats it as needed
    - The value is stored in a NamedTuple field with the specified python_type
    - During SQL insertion, the value is bound to a ? placeholder

    Attributes:
        column_name: The column name in the database table.
        python_type: Python type for the NamedTuple field that will hold the
            value. Must be compatible with 
            :class:`~apache_beam.coders.row_coder.RowCoder`.
        value_fn: Function to extract and format the value from a Chunk.
            Takes a Chunk and returns a value of python_type.
        placeholder: Optional placeholder to apply typecasts or functions to
            value ? placeholder e.g. "string_to_vector(?)" for vector columns.
    
    Examples:

        Basic text column (uses standard JDBC type mapping):

        >>> ColumnSpec.text(
        ...     column_name="content",
        ...     value_fn=lambda chunk: chunk.content.text
        ... )
        ... # Results in: INSERT INTO table (content) VALUES (?)

        Timestamp from metadata:

        >>> ColumnSpec(
        ...     column_name="created_at",
        ...     python_type=str,
        ...     value_fn=lambda chunk: chunk.metadata.get("timestamp")
        ... )
        ... # Results in: INSERT INTO table (created_at) VALUES (?)


    Factory Methods:
        text: Creates a text column specification.
        integer: Creates an integer column specification.
        float: Creates a float column specification.
        vector: Creates a vector column specification with string_to_vector().
        json: Creates a JSON column specification.
    """
  column_name: str
  python_type: Type
  value_fn: Callable[[Chunk], Any]
  placeholder: str = '?'

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
    """Create a vector column specification with string_to_vector() function."""
    return cls(column_name, str, value_fn, "string_to_vector(?)")

  @classmethod
  def json(
      cls, column_name: str, value_fn: Callable[[Chunk], Any]) -> 'ColumnSpec':
    """Create a JSON column specification."""
    return cls(column_name, str, value_fn)


def embedding_to_string(embedding: List[float]) -> str:
  """Convert embedding to MySQL vector string format."""
  return '[' + ','.join(str(x) for x in embedding) + ']'


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
      convert_fn: Optional[Callable[[str],
                                    Any]] = None) -> 'ColumnSpecsBuilder':
    """Add ID :class:`.ColumnSpec` with optional type and conversion.
        
        Args:
            column_name: Name for the ID column (defaults to "id")
            python_type: Python type for the column (defaults to str)
            convert_fn: Optional function to convert the chunk ID
                       If None, uses ID as-is
        
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
            column_name=column_name, python_type=python_type,
            value_fn=value_fn))
    return self

  def with_content_spec(
      self,
      column_name: str = "content",
      python_type: Type = str,
      convert_fn: Optional[Callable[[str],
                                    Any]] = None) -> 'ColumnSpecsBuilder':
    """Add content :class:`.ColumnSpec` with optional type and conversion.
      
      Args:
          column_name: Name for the content column (defaults to "content")
          python_type: Python type for the column (defaults to str)
          convert_fn: Optional function to convert the content text
                      If None, uses content text as-is
      
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
            column_name=column_name, python_type=python_type,
            value_fn=value_fn))
    return self

  def with_metadata_spec(
      self,
      column_name: str = "metadata",
      python_type: Type = str,
      convert_fn: Optional[Callable[[Dict[str, Any]], Any]] = None
  ) -> 'ColumnSpecsBuilder':
    """Add metadata :class:`.ColumnSpec` with optional type and conversion.
      
      Args:
          column_name: Name for the metadata column (defaults to "metadata")
          python_type: Python type for the column (defaults to str)
          convert_fn: Optional function to convert the metadata dictionary
                      If None and python_type is str, converts to JSON string
      
      Returns:
          Self for method chaining
      
      Example:
          >>> builder.with_metadata_spec(
          ...     column_name="meta_tags",
          ...     python_type=str,
          ...     convert_fn=lambda meta: ','.join(meta.keys())
          ... )
      """
    def value_fn(chunk: Chunk) -> Any:
      if convert_fn:
        return convert_fn(chunk.metadata)
      return json.dumps(
          chunk.metadata) if python_type == str else chunk.metadata

    self._specs.append(
        ColumnSpec(
            column_name=column_name, python_type=python_type,
            value_fn=value_fn))
    return self

  def with_embedding_spec(
      self,
      column_name: str = "embedding",
      convert_fn: Callable[[List[float]], Any] = embedding_to_string
  ) -> 'ColumnSpecsBuilder':
    """Add embedding :class:`.ColumnSpec` with optional conversion.
      
      Args:
          column_name: Name for the embedding column (defaults to "embedding")
          convert_fn: Optional function to convert the dense embedding values
                      If None, uses default MySQL vector format
      
      Returns:
          Self for method chaining
      
      Example:
          >>> builder.with_embedding_spec(
          ...     column_name="embedding_vector",
          ...     convert_fn=lambda values: '[' + ','.join(f"{x:.4f}" 
          ...       for x in values) + ']'
          ... )
      """
    def value_fn(chunk: Chunk) -> Any:
      if chunk.embedding is None or chunk.embedding.dense_embedding is None:
        raise ValueError(f'Expected chunk to contain embedding. {chunk}')
      values = chunk.embedding.dense_embedding
      return convert_fn(values)

    self._specs.append(
        ColumnSpec.vector(column_name=column_name, value_fn=value_fn))
    return self

  def add_metadata_field(
      self,
      field: str,
      python_type: Type,
      column_name: Optional[str] = None,
      convert_fn: Optional[Callable[[Any], Any]] = None,
      default: Any = None) -> 'ColumnSpecsBuilder':
    """Add a :class:`.ColumnSpec` that extracts and converts a field from
        chunk metadata.

        Args:
            field: Key to extract from chunk metadata
            python_type: Python type for the column (e.g. str, int, float)
            column_name: Name for the column (defaults to metadata field name)
            convert_fn: Optional function to convert the extracted value to
                      desired type. If None, value is used as-is
            default: Default value if field is missing from metadata
        
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
            ...     python_type=float,
            ...     convert_fn=lambda x: round(float(x), 2),
            ...     default=0.0
            ... )

            Timestamp with conversion:
            >>> builder.add_metadata_field(
            ...     field="created_at",
            ...     python_type=str,
            ...     convert_fn=lambda ts: ts.replace('T', ' ')
            ... )
        """
    name = column_name or field

    def value_fn(chunk: Chunk) -> Any:
      value = chunk.metadata.get(field, default)
      if value is not None and convert_fn is not None:
        value = convert_fn(value)
      return value

    spec = ColumnSpec(
        column_name=name, python_type=python_type, value_fn=value_fn)

    self._specs.append(spec)
    return self

  def add_custom_column_spec(self, spec: ColumnSpec) -> 'ColumnSpecsBuilder':
    """Add a custom :class:`.ColumnSpec` to the builder.
    
    Use this method when you need complete control over the
    :class:`.ColumnSpec`, including custom value extraction and type handling.
    
    Args:
        spec: A :class:`.ColumnSpec` instance defining the column name, type,
            value extraction, and optional MySQL function.
    
    Returns:
        Self for method chaining
    
    Examples:
        Custom text column from chunk metadata:
        >>> builder.add_custom_column_spec(
        ...     ColumnSpec.text(
        ...         column_name="source_and_id",
        ...         value_fn=lambda chunk: 
        ...             f"{chunk.metadata.get('source')}_{chunk.id}"
        ...     )
        ... )
    """
    self._specs.append(spec)
    return self

  def build(self) -> List[ColumnSpec]:
    """Build the final list of column specifications."""
    return self._specs.copy()


@dataclass
class ConflictResolution:
  """Specification for how to handle conflicts during insert.

    Configures conflict handling behavior when inserting records that may
    violate unique constraints using MySQL's ON DUPLICATE KEY UPDATE syntax.
    
    MySQL automatically detects conflicts based on PRIMARY KEY or UNIQUE 
    constraints defined on the table.

    Attributes:
        action: How to handle conflicts - either "UPDATE" or "IGNORE".
            UPDATE: Updates existing record with new values.
            IGNORE: Skips conflicting records (uses no-op update).
        update_fields: Optional list of fields to update on conflict. If None,
            all fields are updated (for UPDATE action only).
        primary_key_field: Required for IGNORE action. The primary key field
            name to use for the no-op update.
        
    Examples:
        Update all fields on conflict:
        >>> ConflictResolution(action="UPDATE")
        
        Update specific fields on conflict:
        >>> ConflictResolution(
        ...     action="UPDATE",
        ...     update_fields=["embedding", "content"]
        ... )
        
        Ignore conflicts with explicit primary key:
        >>> ConflictResolution(
        ...     action="IGNORE", 
        ...     primary_key_field="id"
        ... )
        
        Ignore conflicts with custom primary key:
        >>> ConflictResolution(
        ...     action="IGNORE",
        ...     primary_key_field="custom_id"
        ... )
    """
  action: Literal["UPDATE", "IGNORE"] = "UPDATE"
  update_fields: Optional[List[str]] = None
  primary_key_field: Optional[str] = None

  def __post_init__(self):
    """Validate configuration after initialization."""
    if self.action == "IGNORE" and self.primary_key_field is None:
      raise ValueError("primary_key_field is required when action='IGNORE'")
