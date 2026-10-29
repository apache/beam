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
deployed; this module only feeds documents. Documents are written through
Vespa's ``/document/v1`` HTTP API with ``requests``, so no Vespa client library
is needed.
"""

import logging
import os
import random
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Optional
from urllib.parse import quote

import requests
from requests.adapters import HTTPAdapter

import apache_beam as beam
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteConfig
from apache_beam.ml.rag.types import EmbeddableItem

_LOGGER = logging.getLogger(__name__)

DEFAULT_WRITE_BATCH_SIZE = 1000
_SUPPORTED_OPERATIONS = ("put", "update")
_MAX_REPORTED_FAILURES = 5
_INITIAL_BACKOFF_S = 0.5
_MAX_BACKOFF_S = 30.0
_MAX_ERROR_MESSAGE_CHARS = 500


@dataclass
class VespaConnectionParameters:
  """Connection settings for a Vespa container (data plane) endpoint.

  Args:
    url: Endpoint URL, e.g. ``http://localhost`` or a Vespa Cloud endpoint.
    port: Optional port. Leave unset when ``url`` already includes it.
    cert: Path to the mTLS certificate (Vespa Cloud data plane). The file is
      opened on the workers, so it must exist there (for example in a custom
      container), not only on the machine that submits the job.
    key: Path to the mTLS private key. Must be set together with ``cert``.
    auth_token: Data-plane token for Vespa Cloud, sent as
      ``Authorization: Bearer <token>``. Mutually exclusive with
      ``cert``/``key``. The token is serialized with the pipeline.
    additional_headers: Extra HTTP headers sent with every request.
  """
  url: str
  port: Optional[int] = None
  cert: Optional[str] = None
  key: Optional[str] = None
  auth_token: Optional[str] = None
  additional_headers: dict[str, str] = field(default_factory=dict)

  def __post_init__(self):
    if not self.url:
      raise ValueError("url must be provided for Vespa")
    if bool(self.cert) != bool(self.key):
      raise ValueError("cert and key must be provided together")
    if self.auth_token and self.cert:
      raise ValueError("Use either mTLS (cert/key) or auth_token, not both")

  def base_url(self) -> str:
    base = self.url.rstrip("/")
    return f"{base}:{self.port}" if self.port is not None else base


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
  payloads, Vespa rejects unknown fields with HTTP 400. A 400 is not retried
  and fails the bundle; the runner's retries fail the same way (in streaming
  the pipeline keeps retrying). Set ``metadata_fields`` when item metadata may
  contain keys the schema does not define.

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
    operation_type: ``"put"`` (replaces the document) or ``"update"``
      (partial update that assigns the written fields, with ``create=true``).
    batch_size: Number of documents buffered before a flush, as in
      ``QdrantWriteConfig``.
    max_concurrent_requests: Number of documents sent in parallel per DoFn
      instance during a flush.
    max_retries: Retries per document for 5xx responses and connection
      errors. HTTP 429 (Vespa backpressure) is retried with backoff and does
      not count against this limit.
    timeout_s: Timeout in seconds for each HTTP request.
  """
  connection_params: VespaConnectionParameters
  schema: str
  namespace: Optional[str] = None
  content_field: Optional[str] = "content"
  dense_embedding_field: Optional[str] = "embedding"
  sparse_embedding_field: Optional[str] = None
  metadata_fields: Optional[dict[str, str]] = None
  fields_fn: Optional[Callable[[EmbeddableItem], dict[str, Any]]] = None
  operation_type: str = "put"
  batch_size: int = DEFAULT_WRITE_BATCH_SIZE
  max_concurrent_requests: int = 16
  max_retries: int = 3
  timeout_s: float = 60.0

  def __post_init__(self):
    if not self.schema:
      raise ValueError("schema must be provided")
    if self.operation_type not in _SUPPORTED_OPERATIONS:
      raise ValueError(
          f"operation_type must be one of {_SUPPORTED_OPERATIONS}, "
          f"got {self.operation_type!r}")
    if self.batch_size <= 0:
      raise ValueError("batch_size must be a positive integer")
    if self.max_concurrent_requests <= 0:
      raise ValueError("max_concurrent_requests must be a positive integer")
    if self.max_retries < 0:
      raise ValueError("max_retries must be non-negative")
    if self.timeout_s <= 0:
      raise ValueError("timeout_s must be positive")
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
    """Returns a function mapping an EmbeddableItem to ``{id, fields}``."""
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


def _backoff_s(attempt: int) -> float:
  """Exponential backoff with full jitter."""
  return random.uniform(0, min(_MAX_BACKOFF_S, _INITIAL_BACKOFF_S * 2**attempt))


def _error_message(response: requests.Response) -> str:
  try:
    message = response.json().get("message", response.text)
  except ValueError:
    message = response.text
  return str(message)[:_MAX_ERROR_MESSAGE_CHARS]


class _VespaWriteFn(beam.DoFn):
  def __init__(self, config: VespaWriteConfig):
    self.config = config
    self._session: Optional[requests.Session] = None
    self._executor: Optional[ThreadPoolExecutor] = None
    self._doc_url_prefix = ""
    self._batch: list[dict[str, Any]] = []

  def setup(self):
    params = self.config.connection_params
    session = requests.Session()
    adapter = HTTPAdapter(
        pool_connections=1, pool_maxsize=self.config.max_concurrent_requests)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    if params.cert:
      for path in (params.cert, params.key):
        if not os.path.isfile(path):
          raise FileNotFoundError(
              f"Vespa mTLS file {path!r} not found on this worker. cert and "
              "key are opened on the workers, so the files must exist there.")
      session.cert = (params.cert, params.key)
    session.headers.update(params.additional_headers)
    if params.auth_token:
      session.headers["Authorization"] = f"Bearer {params.auth_token}"
    self._session = session
    self._executor = ThreadPoolExecutor(
        max_workers=self.config.max_concurrent_requests)

    namespace = self.config.namespace or self.config.schema
    self._doc_url_prefix = (
        f"{params.base_url()}/document/v1/{quote(namespace, safe='')}/"
        f"{quote(self.config.schema, safe='')}/docid/")

  def start_bundle(self):
    self._batch = []

  def process(self, element, *args, **kwargs):
    self._batch.append(element)
    if len(self._batch) >= self.config.batch_size:
      self._flush()

  def finish_bundle(self):
    self._flush()

  def teardown(self):
    if self._executor is not None:
      self._executor.shutdown(wait=True)
      self._executor = None
    if self._session is not None:
      self._session.close()
      self._session = None

  def _flush(self):
    if not self._batch:
      return
    if self._session is None or self._executor is None:
      raise RuntimeError("Vespa client is not initialized")

    results = list(self._executor.map(self._write_one, self._batch))
    failures = [r for r in results if r is not None]
    if failures:
      sample = failures[:_MAX_REPORTED_FAILURES]
      # Raising fails the bundle so the runner retries it. Puts and
      # assign-only updates are idempotent per document id, so replaying
      # documents that already succeeded leaves the same final state.
      raise VespaFeedError(
          f"{len(failures)} of {len(self._batch)} documents failed to feed "
          f"to schema '{self.config.schema}'. First failures "
          f"(id, status, message): {sample}")
    self._batch = []

  def _write_one(self, doc: dict[str, Any]) -> Optional[tuple[str, Any, str]]:
    """Writes one document. Returns ``(id, status, message)`` on failure."""
    assert self._session is not None
    doc_id = doc["id"]
    url = self._doc_url_prefix + quote(doc_id, safe="")
    if self.config.operation_type == "update":
      method = "PUT"
      params = {"create": "true"}
      body = {
          "fields": {
              name: {
                  "assign": value
              }
              for name, value in doc["fields"].items()
          }
      }
    else:
      method = "POST"
      params = None
      body = {"fields": doc["fields"]}

    retries = 0
    throttled = 0
    while True:
      try:
        response = self._session.request(
            method,
            url,
            params=params,
            json=body,
            timeout=self.config.timeout_s)
      except (requests.ConnectionError, requests.Timeout) as e:
        if retries >= self.config.max_retries:
          return (doc_id, None, f"{type(e).__name__}: {e}")
        time.sleep(_backoff_s(retries))
        retries += 1
        continue

      status = response.status_code
      if 200 <= status < 300:
        return None
      if status == 429:
        # Backpressure from Vespa; not counted against max_retries.
        time.sleep(_backoff_s(throttled))
        throttled += 1
        continue
      if status >= 500 and retries < self.config.max_retries:
        _LOGGER.debug(
            "Retrying Vespa document %s after HTTP %d", doc_id, status)
        time.sleep(_backoff_s(retries))
        retries += 1
        continue
      return (doc_id, status, _error_message(response))

  def display_data(self):
    res = super().display_data()
    res["schema"] = self.config.schema
    res["operation_type"] = self.config.operation_type
    res["batch_size"] = self.config.batch_size
    res["max_concurrent_requests"] = self.config.max_concurrent_requests
    return res
