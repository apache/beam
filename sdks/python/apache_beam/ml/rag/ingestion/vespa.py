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

"""Write EmbeddableItems to a Vespa schema.

Example:
  >>> config = VespaWriteConfig(
  ...     connection_params=VespaConnectionParameters(
  ...         url="http://localhost", port=8080),
  ...     schema="doc",
  ...     sparse_embedding_field="sparse")
  >>> items | VectorDatabaseWriteTransform(config)

The Vespa application package (schema with the target fields) must already be
deployed; this module only feeds documents.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Optional

try:
  from vespa.application import Vespa
  from vespa.io import VespaResponse
except ImportError:
  logging.warning("pyvespa is not installed.")

import apache_beam as beam
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.types import EmbeddableItem

DEFAULT_WRITE_BATCH_SIZE = 1000
_SUPPORTED_OPERATIONS = ("feed", "update")
_MAX_REPORTED_FAILURES = 5


@dataclass
class VespaConnectionParameters:
  """Connection settings for a Vespa container (data plane) endpoint.

  Args:
    url: Endpoint URL, e.g. ``http://localhost`` or a Vespa Cloud endpoint.
    port: Optional port. Leave unset when ``url`` already includes it.
    cert: Path to the mTLS certificate (Vespa Cloud data plane).
    key: Path to the mTLS private key. Must be set together with ``cert``.
    vespa_cloud_secret_token: Data-plane token for Vespa Cloud. Mutually
      exclusive with ``cert``/``key``.
    additional_headers: Extra HTTP headers sent with every request.
  """
  url: str
  port: Optional[int] = None
  cert: Optional[str] = None
  key: Optional[str] = None
  vespa_cloud_secret_token: Optional[str] = None
  additional_headers: dict[str, str] = field(default_factory=dict)

  def __post_init__(self):
    if not self.url:
      raise ValueError("url must be provided for Vespa")
    if bool(self.cert) != bool(self.key):
      raise ValueError("cert and key must be provided together")
    if self.vespa_cloud_secret_token and self.cert:
      raise ValueError(
          "Use either mTLS (cert/key) or vespa_cloud_secret_token, not both")


@dataclass
class VespaWriteConfig(VectorDatabaseWriteConfig):
  """Configuration for writing EmbeddableItems to Vespa.

  Field mapping (unless ``fields_fn`` is given):
    - ``EmbeddableItem.id`` -> user part of ``id:<namespace>:<schema>::<id>``
    - dense embedding -> ``dense_embedding_field`` as a JSON array (short form
      of an indexed tensor such as ``tensor<float>(x[384])``)
    - sparse embedding -> ``sparse_embedding_field`` as ``{"<index>": value}``
      (short form of a mapped tensor such as ``tensor<float>(token{})``)
    - ``content_string`` -> ``content_field``
    - metadata -> top-level fields, renamed with ``metadata_fields``

  Every written field must exist in the deployed schema. Unlike Qdrant
  payloads, Vespa rejects unknown fields with HTTP 400, which fails the bundle.
  A 400 is not transient, so the runner's retries fail the same way (in
  streaming the pipeline keeps retrying). Set ``metadata_fields`` when item
  metadata may contain keys the schema does not define.

  Args:
    connection_params: VespaConnectionParameters.
    schema: Vespa schema (document type) to write to.
    namespace: Document id namespace. Defaults to ``schema``.
    content_field: Field for the item's content. ``None`` skips content.
    dense_embedding_field: Field for the dense embedding. ``None`` skips it.
    sparse_embedding_field: Field for the sparse embedding. ``None`` skips it.
    metadata_fields: Mapping of metadata key -> Vespa field. Only listed keys
      are written. ``None`` writes every metadata key under its own name, so
      the schema must define a field for every key that can occur.
    fields_fn: Optional function that builds the whole ``fields`` dict from an
      item. Overrides all of the field options above.
    operation_type: ``"feed"`` (put, replaces the document) or ``"update"``
      (partial update with ``create=true``).
    batch_size: Number of documents buffered before a flush. Each flush
      (including the one at the end of every bundle) opens a new connection
      pool, so very small bundles pay connection setup more often.
    max_queue_size, max_workers, max_connections, compress, num_retries_429:
      Passed to ``Vespa.feed_iterable``.
  """
  connection_params: VespaConnectionParameters
  schema: str
  namespace: Optional[str] = None
  content_field: Optional[str] = "content"
  dense_embedding_field: Optional[str] = "embedding"
  sparse_embedding_field: Optional[str] = None
  metadata_fields: Optional[dict[str, str]] = None
  fields_fn: Optional[Callable[[EmbeddableItem], dict[str, Any]]] = None
  operation_type: str = "feed"
  batch_size: int = DEFAULT_WRITE_BATCH_SIZE
  max_queue_size: int = 1000
  max_workers: int = 8
  max_connections: int = 16
  compress: Any = "auto"
  num_retries_429: int = 10

  def __post_init__(self):
    if not self.schema:
      raise ValueError("schema must be provided")
    if self.operation_type not in _SUPPORTED_OPERATIONS:
      raise ValueError(
          f"operation_type must be one of {_SUPPORTED_OPERATIONS}, "
          f"got {self.operation_type!r}")
    if self.batch_size <= 0:
      raise ValueError("batch_size must be a positive integer")
    for name in ("content_field",
                 "dense_embedding_field",
                 "sparse_embedding_field"):
      value = getattr(self, name)
      if value is not None and not value:
        raise ValueError(f"{name} must be a non-empty string or None")
    if self.fields_fn is None and not (self.dense_embedding_field or
                                       self.sparse_embedding_field):
      raise ValueError(
          "At least one of dense_embedding_field or sparse_embedding_field "
          "must be set (or provide fields_fn)")
    reserved = [
        f for f in (
            self.content_field, self.dense_embedding_field,
            self.sparse_embedding_field) if f
    ]
    if len(reserved) != len(set(reserved)):
      raise ValueError(
          f"content and embedding fields must be distinct, got {reserved}")

  def create_write_transform(self) -> beam.PTransform[EmbeddableItem, Any]:
    return _VespaWriteTransform(self)

  def create_converter(self) -> Callable[[EmbeddableItem], dict[str, Any]]:
    """Returns a function mapping an EmbeddableItem to a feed_iterable doc."""
    def convert(item: EmbeddableItem) -> dict[str, Any]:
      if item.id is None or item.id == "":
        raise ValueError("EmbeddableItem.id is required for Vespa")
      doc_id = str(item.id)
      if self.fields_fn is not None:
        custom = self.fields_fn(item)
        if not isinstance(custom, dict):
          raise TypeError(
              f"fields_fn must return a dict, got {type(custom)} "
              f"for item {doc_id}")
        return {"id": doc_id, "fields": custom}

      fields: dict[str, Any] = {}
      wrote_embedding = False
      if self.dense_embedding_field and item.dense_embedding is not None:
        fields[self.dense_embedding_field] = [
            float(v) for v in item.dense_embedding
        ]
        wrote_embedding = True
      if self.sparse_embedding_field and item.sparse_embedding is not None:
        indices, values = item.sparse_embedding
        if len(indices) != len(values):
          raise ValueError(
              f"Sparse embedding of item {doc_id} has {len(indices)} indices "
              f"but {len(values)} values")
        fields[self.sparse_embedding_field] = {
            str(i): float(v)
            for i, v in zip(indices, values)
        }
        wrote_embedding = True
      if not wrote_embedding:
        raise ValueError(
            f"Item {doc_id} has no embedding for the configured fields "
            f"(dense={self.dense_embedding_field!r}, "
            f"sparse={self.sparse_embedding_field!r})")
      if self.content_field:
        fields[self.content_field] = item.content_string

      metadata = item.metadata or {}
      if self.metadata_fields is None:
        mapped = dict(metadata)
      else:
        mapped = {
            vespa_field: metadata[key]
            for key, vespa_field in self.metadata_fields.items()
            if key in metadata
        }
      for vespa_field, value in mapped.items():
        if vespa_field in fields:
          raise ValueError(
              f"Metadata field '{vespa_field}' collides with a content or "
              f"embedding field for item {doc_id}")
        fields[vespa_field] = value
      return {"id": doc_id, "fields": fields}

    return convert


class _VespaWriteTransform(beam.PTransform):
  def __init__(self, config: VespaWriteConfig):
    self.config = config

  def expand(self, input_or_inputs: beam.PCollection[EmbeddableItem]):
    return (
        input_or_inputs
        | "Convert to Vespa docs" >> beam.Map(self.config.create_converter())
        | "Write to Vespa" >> beam.ParDo(_VespaWriteFn(self.config)))


class VespaFeedError(RuntimeError):
  """Raised when one or more documents in a flush were not accepted."""


class _VespaWriteFn(beam.DoFn):
  def __init__(self, config: VespaWriteConfig):
    self.config = config
    self._app: "Optional[Vespa]" = None
    self._batch: list[dict[str, Any]] = []

  def setup(self):
    params = self.config.connection_params
    self._app = Vespa(
        url=params.url,
        port=params.port,
        cert=params.cert,
        key=params.key,
        vespa_cloud_secret_token=params.vespa_cloud_secret_token,
        additional_headers=params.additional_headers or None,
    )

  def start_bundle(self):
    self._batch = []

  def process(self, element, *args, **kwargs):
    self._batch.append(element)
    if len(self._batch) >= self.config.batch_size:
      self._flush()

  def finish_bundle(self):
    self._flush()

  def teardown(self):
    # feed_iterable opens and closes its own HTTP session per call.
    self._app = None

  def _flush(self):
    if not self._batch:
      return
    if self._app is None:
      raise RuntimeError("Vespa client is not initialized")

    failures: list[tuple[str, int, Any]] = []

    def callback(response: "VespaResponse", doc_id: str):
      if not response.is_successful():
        failures.append(
            (doc_id, response.get_status_code(), response.get_json()))

    extra: dict[str, Any] = {}
    if self.config.operation_type == "update":
      extra["create"] = True
    self._app.feed_iterable(
        self._batch,
        schema=self.config.schema,
        namespace=self.config.namespace,
        callback=callback,
        operation_type=self.config.operation_type,
        max_queue_size=self.config.max_queue_size,
        max_workers=self.config.max_workers,
        max_connections=self.config.max_connections,
        compress=self.config.compress,
        num_retries_429=self.config.num_retries_429,
        **extra,
    )
    if failures:
      sample = failures[:_MAX_REPORTED_FAILURES]
      # Raising fails the bundle so the runner retries it. Puts and
      # create-updates are idempotent per document id, so retries are safe.
      raise VespaFeedError(
          f"{len(failures)} of {len(self._batch)} documents failed to feed "
          f"to schema '{self.config.schema}'. First failures "
          f"(id, status, body): {sample}")
    self._batch = []

  def display_data(self):
    res = super().display_data()
    res["schema"] = self.config.schema
    res["operation_type"] = self.config.operation_type
    res["batch_size"] = self.config.batch_size
    return res
