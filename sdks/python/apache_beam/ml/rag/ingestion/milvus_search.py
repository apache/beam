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
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional

from pymilvus import MilvusClient
from pymilvus.exceptions import MilvusException

import apache_beam as beam
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.ingestion.jdbc_common import WriteConfig
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpec
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpecsBuilder
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.utils import DEFAULT_WRITE_BATCH_SIZE
from apache_beam.ml.rag.utils import MilvusConnectionParameters
from apache_beam.ml.rag.utils import MilvusHelpers
from apache_beam.ml.rag.utils import retry_with_backoff
from apache_beam.ml.rag.utils import unpack_dataclass_with_kwargs
from apache_beam.transforms import DoFn

_LOGGER = logging.getLogger(__name__)


@dataclass
class MilvusWriteConfig:
  """Configuration parameters for writing data to Milvus collections.

  This class defines the parameters needed to write data to a Milvus collection,
  including collection targeting, batching behavior, and operation timeouts.

  Args:
    collection_name: Name of the target Milvus collection to write data to.
      Must be a non-empty string.
    partition_name: Name of the specific partition within the collection to
      write to. If empty, writes to the default partition.
    timeout: Maximum time in seconds to wait for write operations to complete.
      If None, uses the client's default timeout.
    write_config: Configuration for write operations including batch size and
      other write-specific settings.
    kwargs: Additional keyword arguments for write operations. Enables forward
      compatibility with future Milvus client parameters.
  """
  collection_name: str
  partition_name: str = ""
  timeout: Optional[float] = None
  write_config: WriteConfig = field(default_factory=WriteConfig)
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.collection_name:
      raise ValueError("Collection name must be provided")

  @property
  def write_batch_size(self):
    """Returns the batch size for write operations.

    Returns:
      The configured batch size, or DEFAULT_WRITE_BATCH_SIZE if not specified.
    """
    return self.write_config.write_batch_size or DEFAULT_WRITE_BATCH_SIZE


@dataclass
class MilvusVectorWriterConfig(VectorDatabaseWriteConfig):
  """Configuration for writing vector data to Milvus collections.

  This class extends VectorDatabaseWriteConfig to provide Milvus-specific
  configuration for ingesting vector embeddings and associated metadata.
  It defines how Apache Beam chunks are converted to Milvus records and
  handles the write operation parameters.

  The configuration includes connection parameters, write settings, and
  column specifications that determine how chunk data is mapped to Milvus
  fields.

  Args:
    connection_params: Configuration for connecting to the Milvus server,
      including URI, credentials, and connection options.
    write_config: Configuration for write operations including collection name,
      partition, batch size, and timeouts.
    column_specs: List of column specifications defining how chunk fields are
      mapped to Milvus collection fields. Defaults to standard RAG fields
      (id, embedding, sparse_embedding, content, metadata).

  Example:
    config = MilvusVectorWriterConfig(
      connection_params=MilvusConnectionParameters(
        uri="http://localhost:19530"),
      write_config=MilvusWriteConfig(collection_name="my_collection"),
      column_specs=MilvusVectorWriterConfig.default_column_specs())
  """
  connection_params: MilvusConnectionParameters
  write_config: MilvusWriteConfig
  column_specs: List[ColumnSpec] = field(
      default_factory=lambda: MilvusVectorWriterConfig.default_column_specs())

  def create_converter(self) -> Callable[[Chunk], Dict[str, Any]]:
    """Creates a function to convert Apache Beam Chunks to Milvus records.

    Returns:
      A function that takes a Chunk and returns a dictionary representing
      a Milvus record with fields mapped according to column_specs.
    """
    def convert(chunk: Chunk) -> Dict[str, Any]:
      result = {}
      for col in self.column_specs:
        result[col.column_name] = col.value_fn(chunk)
      return result

    return convert

  def create_write_transform(self) -> beam.PTransform:
    """Creates the Apache Beam transform for writing to Milvus.

    Returns:
      A PTransform that can be applied to a PCollection of Chunks to write
      them to the configured Milvus collection.
    """
    return _WriteToMilvusVectorDatabase(self)

  @staticmethod
  def default_column_specs() -> List[ColumnSpec]:
    """Returns default column specifications for RAG use cases.

    Creates column mappings for standard RAG fields: id, dense embedding,
    sparse embedding, content text, and metadata. These specifications
    define how Chunk fields are converted to Milvus-compatible formats.

    Returns:
      List of ColumnSpec objects defining the default field mappings.
    """
    column_specs = ColumnSpecsBuilder()
    return column_specs\
      .with_id_spec()\
      .with_embedding_spec(convert_fn=lambda values: list(values))\
      .with_sparse_embedding_spec(conv_fn=MilvusHelpers.sparse_embedding)\
      .with_content_spec()\
      .with_metadata_spec(convert_fn=lambda values: dict(values))\
      .build()


class _WriteToMilvusVectorDatabase(beam.PTransform):
  """Apache Beam PTransform for writing vector data to Milvus.

  This transform handles the conversion of Apache Beam Chunks to Milvus records
  and coordinates the write operations. It applies the configured converter
  function and uses a DoFn for batched writes to optimize performance.

  Args:
    config: MilvusVectorWriterConfig containing all necessary parameters for
      the write operation.
  """
  def __init__(self, config: MilvusVectorWriterConfig):
    self.config = config

  def expand(self, pcoll: beam.PCollection[Chunk]):
    """Expands the PTransform to convert chunks and write to Milvus.

    Args:
      pcoll: PCollection of Chunk objects to write to Milvus.

    Returns:
      PCollection of dictionaries representing the records written to Milvus.
    """
    return (
        pcoll
        | "Convert to Records" >> beam.Map(self.config.create_converter())
        | beam.ParDo(
            _WriteMilvusFn(
                self.config.connection_params, self.config.write_config)))


class _WriteMilvusFn(DoFn):
  """DoFn that handles batched writes to Milvus.

  This DoFn accumulates records in batches and flushes them to Milvus when
  the batch size is reached or when the bundle finishes. This approach
  optimizes performance by reducing the number of individual write operations.

  Args:
    connection_params: Configuration for connecting to the Milvus server.
    write_config: Configuration for write operations including batch size
      and collection details.
  """
  def __init__(
      self,
      connection_params: MilvusConnectionParameters,
      write_config: MilvusWriteConfig):
    self._connection_params = connection_params
    self._write_config = write_config
    self.batch = []

  def process(self, element, *args, **kwargs):
    """Processes individual records, batching them for efficient writes.

    Args:
      element: A dictionary representing a Milvus record to write.
      *args: Additional positional arguments.
      **kwargs: Additional keyword arguments.

    Yields:
      The original element after adding it to the batch.
    """
    _ = args, kwargs  # Unused parameters
    self.batch.append(element)
    if len(self.batch) >= self._write_config.write_batch_size:
      self._flush()
    yield element

  def finish_bundle(self):
    """Called when a bundle finishes processing.

    Flushes any remaining records in the batch to ensure all data is written.
    """
    self._flush()

  def _flush(self):
    """Flushes the current batch of records to Milvus.

    Creates a MilvusSink connection and writes all batched records,
    then clears the batch for the next set of records.
    """
    if len(self.batch) == 0:
      return
    with _MilvusSink(self._connection_params, self._write_config) as sink:
      sink.write(self.batch)
      self.batch = []

  def display_data(self):
    """Returns display data for monitoring and debugging.

    Returns:
      Dictionary containing database, collection, and batch size information
      for display in the Apache Beam monitoring UI.
    """
    res = super().display_data()
    res["database"] = self._connection_params.db_name
    res["collection"] = self._write_config.collection_name
    res["batch_size"] = self._write_config.write_batch_size
    return res


class _MilvusSink:
  """Low-level sink for writing data directly to Milvus.

  This class handles the direct interaction with the Milvus client for
  upsert operations. It manages the connection lifecycle and provides
  context manager support for proper resource cleanup.

  Args:
    connection_params: Configuration for connecting to the Milvus server.
    write_config: Configuration for write operations including collection
      and partition targeting.
  """
  def __init__(
      self,
      connection_params: MilvusConnectionParameters,
      write_config: MilvusWriteConfig):
    self._connection_params = connection_params
    self._write_config = write_config
    self._client = None

  def write(self, documents):
    """Writes a batch of documents to the Milvus collection.

    Performs an upsert operation to insert new documents or update existing
    ones based on primary key. After the upsert, flushes the collection to
    ensure data persistence.

    Args:
      documents: List of dictionaries representing Milvus records to write.
        Each dictionary should contain fields matching the collection schema.
    """
    self._client = MilvusClient(
        **unpack_dataclass_with_kwargs(self._connection_params))

    resp = self._client.upsert(
        collection_name=self._write_config.collection_name,
        partition_name=self._write_config.partition_name,
        data=documents,
        timeout=self._write_config.timeout,
        **self._write_config.kwargs)

    _LOGGER.debug(
        "Upserted into Milvus: upsert_count=%d, cost=%d",
        resp.get("upsert_count", 0),
        resp.get("cost", 0))

  def __enter__(self):
    """Enters the context manager and establishes Milvus connection.

    Returns:
      Self, enabling use in 'with' statements.
    """
    if not self._client:
      connection_params = unpack_dataclass_with_kwargs(self._connection_params)

      # Extract retry parameters from connection_params.
      max_retries = connection_params.pop('max_retries', 3)
      retry_delay = connection_params.pop('retry_delay', 1.0)
      retry_backoff_factor = connection_params.pop('retry_backoff_factor', 2.0)

      def create_client():
        return MilvusClient(**connection_params)

      self._client = retry_with_backoff(
          create_client,
          max_retries=max_retries,
          retry_delay=retry_delay,
          retry_backoff_factor=retry_backoff_factor,
          operation_name="Milvus connection",
          exception_types=(MilvusException, ))
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    """Exits the context manager and closes the Milvus connection.

    Args:
      exc_type: Exception type if an exception was raised.
      exc_val: Exception value if an exception was raised.
      exc_tb: Exception traceback if an exception was raised.
    """
    _ = exc_type, exc_val, exc_tb  # Unused parameters
    if self._client:
      self._client.close()
