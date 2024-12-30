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

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import beam_row_from_dict
from apache_beam.io.gcp.bigquery_tools import get_beam_typehints_from_tableschema
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.types import Chunk
from apache_beam.typehints.row_type import RowTypeConstraint

ChunkToDictFn = Callable[[Chunk], Dict[str, any]]


@dataclass
class SchemaConfig:
  """Configuration for custom BigQuery schema and row conversion.
  
  Allows overriding the default schema and row conversion logic for BigQuery
  vector storage. This enables custom table schemas beyond the default
  id/embedding/content/metadata structure.

  Attributes:
      schema: BigQuery TableSchema dict defining the table structure.
          Example:
          >>> {
          ...   'fields': [
          ...     {'name': 'id', 'type': 'STRING'},
          ...     {'name': 'embedding', 'type': 'FLOAT64', 'mode': 'REPEATED'},
          ...     {'name': 'custom_field', 'type': 'STRING'}
          ...   ]
          ... }
      chunk_to_dict_fn: Function that converts a Chunk to a dict matching the
          schema. Takes a Chunk and returns Dict[str, Any] with keys matching
          schema fields.
          Example:
          >>> def chunk_to_dict(chunk: Chunk) -> Dict[str, Any]:
          ...     return {
          ...         'id': chunk.id,
          ...         'embedding': chunk.embedding.dense_embedding,
          ...         'custom_field': chunk.metadata.get('custom_field')
          ...     }
  """
  schema: Dict
  chunk_to_dict_fn: ChunkToDictFn


class BigQueryVectorWriterConfig(VectorDatabaseWriteConfig):
  def __init__(
      self,
      write_config: Dict[str, Any],
      *,  # Force keyword arguments
      schema_config: Optional[SchemaConfig] = None
      ):
    """Configuration for writing vectors to BigQuery using managed transforms.
    
    Supports both default schema (id, embedding, content, metadata columns) and
    custom schemas through SchemaConfig.

    Example with default schema:
      >>> config = BigQueryVectorWriterConfig(
      ...     write_config={'table': 'project.dataset.embeddings'})

    Example with custom schema:
      >>> schema_config = SchemaConfig(
      ...   schema={
      ...     'fields': [
      ...       {'name': 'id', 'type': 'STRING'},
      ...       {'name': 'embedding', 'type': 'FLOAT64', 'mode': 'REPEATED'},
      ...       {'name': 'source_url', 'type': 'STRING'}
      ...     ]
      ...   },
      ...   chunk_to_dict_fn=lambda chunk: {
      ...       'id': chunk.id,
      ...       'embedding': chunk.embedding.dense_embedding,
      ...       'source_url': chunk.metadata.get('url')
      ...   }
      ... )
      >>> config = BigQueryVectorWriterConfig(
      ...   write_config={'table': 'project.dataset.embeddings'},
      ...   schema_config=schema_config
      ... )

    Args:
        write_config: BigQuery write configuration dict. Must include 'table'.
            Other options like create_disposition, write_disposition can be
            specified.
        schema_config: Optional configuration for custom schema and row
            conversion.
            If not provided, uses default schema with id, embedding, content and
            metadata columns.
    
    Raises:
        ValueError: If write_config doesn't include table specification.
    """
    if 'table' not in write_config:
      raise ValueError("write_config must be provided with 'table' specified")

    self.write_config = write_config
    self.schema_config = schema_config

  def create_write_transform(self) -> beam.PTransform:
    """Creates transform to write to BigQuery."""
    return _WriteToBigQueryVectorDatabase(self)


def _default_chunk_to_dict_fn(chunk: Chunk):
  return {
      'id': chunk.id,
      'embedding': chunk.embedding.dense_embedding,
      'content': chunk.content.text,
      'metadata': [
          {
              "key": k, "value": str(v)
          } for k, v in chunk.metadata.items()
      ]
  }


def _default_schema():
  return {
      'fields': [{
          'name': 'id', 'type': 'STRING'
      }, {
          'name': 'embedding', 'type': 'FLOAT64', 'mode': 'REPEATED'
      }, {
          'name': 'content', 'type': 'STRING'
      },
                 {
                     'name': 'metadata',
                     'type': 'RECORD',
                     'mode': 'REPEATED',
                     'fields': [{
                         'name': 'key', 'type': 'STRING'
                     }, {
                         'name': 'value', 'type': 'STRING'
                     }]
                 }]
  }


class _WriteToBigQueryVectorDatabase(beam.PTransform):
  """Implementation of BigQuery vector database write. """
  def __init__(self, config: BigQueryVectorWriterConfig):
    self.config = config

  def expand(self, pcoll: beam.PCollection[Chunk]):
    schema = (
        self.config.schema_config.schema
        if self.config.schema_config else _default_schema())
    chunk_to_dict_fn = (
        self.config.schema_config.chunk_to_dict_fn
        if self.config.schema_config else _default_chunk_to_dict_fn)
    return (
        pcoll
        | "Chunk to dict" >> beam.Map(chunk_to_dict_fn)
        | "Chunk dict to schema'd row" >> beam.Map(
            lambda chunk_dict: beam_row_from_dict(
                row=chunk_dict, schema=schema)).with_output_types(
                    RowTypeConstraint.from_fields(
                        get_beam_typehints_from_tableschema(schema)))
        | "Write to BigQuery" >> beam.managed.Write(
            beam.managed.BIGQUERY, config=self.config.write_config))
