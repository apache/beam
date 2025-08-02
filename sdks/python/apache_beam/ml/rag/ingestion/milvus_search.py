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

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, NamedTuple, Optional

from pymilvus import MilvusClient

import logging

import apache_beam as beam
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpec
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpecsBuilder
from apache_beam.ml.rag.ingestion.jdbc_common import WriteConfig
from apache_beam.ml.rag.types import Chunk
from apache_beam.transforms import DoFn

_LOGGER = logging.getLogger(__name__)


@dataclass
class MilvusConnectionConfig:
  """Configurations for establishing connections to Milvus servers.

  Args:
    uri: URI endpoint for connecting to Milvus server in the format
      "http(s)://hostname:port".
    user: Username for authentication. Required if authentication is enabled and
      not using token authentication.
    password: Password for authentication. Required if authentication is enabled
      and not using token authentication.
    db_id: Database ID to connect to. Specifies which Milvus database to use.
      Defaults to 'default'.
    token: Authentication token as an alternative to username/password.
    timeout: Connection timeout in seconds. Uses client default if None.
    kwargs: Optional keyword arguments for additional connection parameters.
      Enables forward compatibility.
  """
  uri: str
  user: str = field(default_factory=str)
  password: str = field(default_factory=str)
  db_id: str = "default"
  token: str = field(default_factory=str)
  timeout: Optional[float] = None
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.uri:
      raise ValueError("URI must be provided for Milvus connection")

@dataclass
class MilvusWriteConfig:
  collection_name: str
  partition_name: str = ""
  timeout: Optional[float] = None
  write_config: WriteConfig = WriteConfig(),
  kwargs: Dict[str, Any] = field(default_factory=dict)

  def __post_init__(self):
    if not self.collection_name:
      raise ValueError("Collection name must be provided")

  @property
  def write_batch_size(self):
    return self.write_config.write_batch_size

@dataclass
class MilvusVectorWriterConfig(VectorDatabaseWriteConfig):
  connection_params: MilvusConnectionConfig
  write_config: MilvusWriteConfig
  column_specs: List[ColumnSpec] = field(
    default_factory=lambda: ColumnSpecsBuilder.with_defaults().build())

  def __post_init__(self):
    if not self.connection_params:
      raise ValueError("")
    if not self.write_config:
      raise ValueError("")

  def create_converter(self) -> Callable[[Chunk], NamedTuple]:
    """Creates a function to convert Chunks to records."""
    def convert(chunk: Chunk) -> Dict[str, Any]:
      result = []
      for col in self.column_specs:
        result[col.column_name] = col.value_fn(chunk)
      return result
    return convert

  def create_write_transform(self) -> beam.PTransform:
    return _WriteToMilvusVectorDatabase(self)

class _WriteToMilvusVectorDatabase(beam.PTransform):
  """Implementation of Milvus vector database write."""
  def __init__(self, config: MilvusVectorWriterConfig):
    self.config = config

  def expand(self, pcoll: beam.PCollection[Chunk]):
    return (
        pcoll
        |
        "Convert to Records" >> beam.Map(self.config.create_converter())
        | "Write to Milvus" >> beam.ParDo(
          _WriteMilvusFn(
            self.config.connection_params,self.config.write_config)))

class _WriteMilvusFn(DoFn):
  def __init__(
      self,
      connection_params: MilvusConnectionConfig,
      write_config: MilvusWriteConfig):
    self._connection_params = connection_params
    self._write_config = write_config
    self.batch = []

  def process(self, element, *args, **kwargs):
    self.batch.append(element)
    if len(self.batch) >= self._write_config.write_batch_size:
      self._flush()

  def finish_bundle(self):
    self._flush()

  def _flush(self):
    if len(self.batch) == 0:
      return
    with _MilvusSink(self._connection_params, self._write_config) as sink:
      sink.write(self.batch)
      self.batch = []

  def display_data(self):
    pass

class _MilvusSink:
  def __init__(
      self,
      connection_params: MilvusConnectionConfig,
      write_config: MilvusWriteConfig):
    self._connection_params = connection_params
    self._write_config = write_config
    self._client = None

  def write(self, documents):
    if not self._client:
      self.client = MilvusClient(**self._connection_params.__dict__)
    resp = self.client.upsert(
      collection_name=self._write_config.collection_name,
      partition_name=self._write_config.partition_name,
      data = documents,
      timeout=self._write_config.timeout,
      **self._write_config.kwargs)
    _LOGGER.debug(
        "Upserted into Milvus: upsert_count=%d, cost=%d",
        resp.get("upsert_count", 0),
        resp.get("cost", 0)
    )

  def __enter__(self):
    if not self.client:
      self.client = MilvusClient(**self._connection_params)
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self.client:
      self.client.close()
