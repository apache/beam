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

import warnings
from collections.abc import Callable
from typing import Any
from typing import Dict
from typing import Optional

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import beam_row_from_dict
from apache_beam.io.gcp.bigquery_tools import get_beam_typehints_from_tableschema
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.typehints.row_type import RowTypeConstraint

EmbeddableToDictFn = Callable[[EmbeddableItem], Dict[str, any]]
# Backward compatibility alias.
ChunkToDictFn = EmbeddableToDictFn


class SchemaConfig:
  def __init__(
      self,
      schema: Dict,
      embeddable_to_dict_fn: Optional[EmbeddableToDictFn] = None,
      **kwargs):
    """Configuration for custom BigQuery schema and row conversion.

    Allows overriding the default schema and row conversion logic for BigQuery
    vector storage. This enables custom table schemas beyond the default
    id/embedding/content/metadata structure.

    Args:
        schema: BigQuery TableSchema dict defining the table structure.
        embeddable_to_dict_fn: Function that converts an EmbeddableItem to a
            dict matching the schema. Takes an EmbeddableItem and returns
            Dict[str, Any] with keys matching schema fields.

    Example with custom schema:
      >>> schema_config = SchemaConfig(
      ...   schema={
      ...     'fields': [
      ...       {'name': 'id', 'type': 'STRING'},
      ...       {'name': 'embedding', 'type': 'FLOAT64', 'mode': 'REPEATED'},
      ...       {'name': 'source_url', 'type': 'STRING'}
      ...     ]
      ...   },
      ...   embeddable_to_dict_fn=lambda item: {
      ...       'id': item.id,
      ...       'embedding': item.embedding.dense_embedding,
      ...       'source_url': item.metadata.get('url')
      ...   }
      ... )
    """
    self.schema = schema
    if 'chunk_to_dict_fn' in kwargs:
      warnings.warn(
          "chunk_to_dict_fn is deprecated, use embeddable_to_dict_fn",
          DeprecationWarning,
          stacklevel=2)
      embeddable_to_dict_fn = kwargs.pop('chunk_to_dict_fn')
    if kwargs:
      raise TypeError(f"Unexpected keyword arguments: {', '.join(kwargs)}")
    if embeddable_to_dict_fn is None:
      raise TypeError("SchemaConfig requires embeddable_to_dict_fn")
    self.embeddable_to_dict_fn = embeddable_to_dict_fn


class BigQueryVectorWriterConfig(VectorDatabaseWriteConfig):
  def __init__(
      self,
      write_config: Dict[str, Any],
      *,  # Force keyword arguments
      schema_config: Optional[SchemaConfig] = None):
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
      ...   embeddable_to_dict_fn=lambda item: {
      ...       'id': item.id,
      ...       'embedding': item.embedding.dense_embedding,
      ...       'source_url': item.metadata.get('url')
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


def _default_embeddable_to_dict_fn(item: EmbeddableItem):
  if item.embedding is None or item.embedding.dense_embedding is None:
    raise ValueError("EmbeddableItem must contain dense embedding")
  return {
      'id': item.id,
      'embedding': item.embedding.dense_embedding,
      'content': item.content_string,
      'metadata': [{
          "key": k, "value": str(v)
      } for k, v in item.metadata.items()]
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

  def expand(self, pcoll: beam.PCollection[EmbeddableItem]):
    schema = (
        self.config.schema_config.schema
        if self.config.schema_config else _default_schema())
    embeddable_to_dict_fn = (
        self.config.schema_config.embeddable_to_dict_fn
        if self.config.schema_config else _default_embeddable_to_dict_fn)
    return (
        pcoll
        | "EmbeddableItem to dict" >> beam.Map(embeddable_to_dict_fn)
        | "EmbeddableItem dict to schema'd row" >> beam.Map(
            lambda embeddable_item_dict: beam_row_from_dict(
                row=embeddable_item_dict, schema=schema)).with_output_types(
                    RowTypeConstraint.from_fields(
                        get_beam_typehints_from_tableschema(schema)))
        | "Write to BigQuery" >> beam.managed.Write(
            beam.managed.BIGQUERY, config=self.config.write_config))
