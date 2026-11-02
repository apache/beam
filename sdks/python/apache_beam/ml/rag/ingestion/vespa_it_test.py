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

import time
import unittest

import pytest
import requests

import apache_beam as beam
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from vespa.application import Vespa
  from vespa.package import ApplicationPackage
  from vespa.package import Document
  from vespa.package import Field
  from vespa.package import Schema

  from apache_beam.ml.rag.ingestion.vespa import VespaConnectionParameters
  from apache_beam.ml.rag.ingestion.vespa import VespaWriteConfig
  VESPA_AVAILABLE = True
except ImportError:
  VESPA_AVAILABLE = False

try:
  from testcontainers.core.container import DockerContainer
except ImportError:
  DockerContainer = None
# pylint: enable=ungrouped-imports

VESPA_IMAGE = "vespaengine/vespa:8.763.13"
SCHEMA = "doc"
_CONTAINER_PORT = 8080
_CONFIG_SERVER_PORT = 19071
_STARTUP_TIMEOUT_S = 300


def _application_package() -> "ApplicationPackage":
  schema = Schema(
      name=SCHEMA,
      document=Document(
          fields=[
              Field("content", "string", indexing=["index", "summary"]),
              Field(
                  "embedding",
                  "tensor<float>(x[2])",
                  indexing=["attribute", "summary"]),
              Field(
                  "sparse",
                  "tensor<float>(token{})",
                  indexing=["attribute", "summary"]),
              Field("source", "string", indexing=["attribute", "summary"]),
          ]))
  return ApplicationPackage(name="beamtest", schema=[schema])


def _wait_until(predicate, what: str):
  deadline = time.time() + _STARTUP_TIMEOUT_S
  while time.time() < deadline:
    try:
      if predicate():
        return
    except requests.RequestException:
      pass
    time.sleep(2)
  raise TimeoutError(f"Timed out waiting for {what}")


def _item(id, dense=None, sparse=None, metadata=None, text=None):
  return EmbeddableItem(
      id=id,
      content=Content(text=text or f"content of {id}"),
      metadata=metadata if metadata is not None else {"source": "test"},
      embedding=Embedding(dense_embedding=dense, sparse_embedding=sparse),
  )


@unittest.skipIf(not VESPA_AVAILABLE, "pyvespa not installed.")
@unittest.skipIf(not DockerContainer, "testcontainers not installed.")
@pytest.mark.require_docker_in_docker
class TestVespaIngestion(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls._container = DockerContainer(VESPA_IMAGE).with_exposed_ports(
        _CONTAINER_PORT, _CONFIG_SERVER_PORT)
    cls._container.start()
    try:
      host = cls._container.get_container_host_ip()
      config_url = (
          f"http://{host}:"
          f"{cls._container.get_exposed_port(_CONFIG_SERVER_PORT)}")
      port = int(cls._container.get_exposed_port(_CONTAINER_PORT))
      container_url = f"http://{host}:{port}"

      _wait_until(
          lambda: requests.get(f"{config_url}/state/v1/health", timeout=5).
          status_code == 200,
          "the Vespa config server")
      response = requests.post(
          f"{config_url}/application/v2/tenant/default/prepareandactivate",
          headers={"Content-Type": "application/zip"},
          data=_application_package().to_zip().getvalue(),
          timeout=120)
      response.raise_for_status()
      # A 404 for an unknown id means the content cluster is serving.
      _wait_until(
          lambda: requests.get(
              f"{container_url}/document/v1/{SCHEMA}/{SCHEMA}/docid/probe",
              timeout=5).status_code == 404,
          "the Vespa container")

      cls._connection_params = VespaConnectionParameters(
          url=f"http://{host}", port=port)
      cls._app = Vespa(url=f"http://{host}", port=port)
    except Exception:
      cls._container.stop()
      raise

  @classmethod
  def tearDownClass(cls):
    cls._container.stop()

  def _config(self, **kwargs):
    return VespaWriteConfig(
        connection_params=self._connection_params,
        schema=SCHEMA,
        sparse_embedding_field="sparse",
        **kwargs)

  def _write(self, items, **kwargs):
    with TestPipeline(is_integration_test=True) as p:
      _ = (
          p
          | beam.Create(items)
          | self._config(**kwargs).create_write_transform())

  def _get(self, doc_id):
    response = self._app.get_data(schema=SCHEMA, data_id=doc_id)
    if response.get_status_code() == 404:
      return None
    self.assertTrue(response.is_successful(), response.get_json())
    return response.get_json()["fields"]

  def test_write_dense_sparse_content_and_metadata(self):
    items = [
        _item(
            "full-1",
            dense=[1.0, 0.5],
            sparse=([3, 17], [0.25, 1.5]),
            metadata={"source": "a.pdf"}),
        _item("full-2", dense=[0.0, -1.0], metadata={"source": "b.pdf"}),
    ]
    self._write(items)

    fields = self._get("full-1")
    self.assertEqual(fields["content"], "content of full-1")
    self.assertEqual(fields["source"], "a.pdf")
    self.assertEqual(fields["embedding"]["values"], [1.0, 0.5])
    self.assertEqual(fields["sparse"]["cells"], {"3": 0.25, "17": 1.5})

    fields = self._get("full-2")
    self.assertEqual(fields["embedding"]["values"], [0.0, -1.0])
    self.assertNotIn("sparse", fields)

  def test_write_with_batching(self):
    items = [_item(f"batch-{i}", dense=[float(i), 0.0]) for i in range(7)]
    self._write(items, batch_size=3)
    for i in range(7):
      self.assertEqual(
          self._get(f"batch-{i}")["embedding"]["values"], [float(i), 0.0])

  def test_id_with_special_characters(self):
    doc_id = "dir/file name.pdf?page=1#chunk::0"
    self._write([_item(doc_id, dense=[1.0, 0.0])])
    self.assertEqual(self._get(doc_id)["content"], f"content of {doc_id}")

  def test_update_creates_missing_and_updates_existing(self):
    self._write([_item("upd-1", dense=[1.0, 0.0], metadata={"source": "a"})])
    self._write([
        _item(
            "upd-1",
            dense=[0.0, 1.0],
            metadata={"source": "b"},
            text="new content"),
        _item("upd-2", dense=[1.0, 1.0]),
    ],
                operation_type="update")

    fields = self._get("upd-1")
    self.assertEqual(fields["source"], "b")
    self.assertEqual(fields["content"], "new content")
    self.assertEqual(fields["embedding"]["values"], [0.0, 1.0])
    self.assertIsNotNone(self._get("upd-2"))

  def test_metadata_fields_drops_keys_missing_from_schema(self):
    self._write([
        _item(
            "meta-1", dense=[1.0, 0.0], metadata={
                "source": "a.pdf", "page": 3
            })
    ],
                metadata_fields={"source": "source"})
    fields = self._get("meta-1")
    self.assertEqual(fields["source"], "a.pdf")
    self.assertNotIn("page", fields)

  def test_unknown_metadata_field_fails(self):
    with self.assertRaisesRegex(Exception, "is not defined in document type"):
      self._write([
          _item(
              "unknown-1",
              dense=[1.0, 0.0],
              metadata={
                  "source": "a.pdf", "page": 3
              })
      ])
    self.assertIsNone(self._get("unknown-1"))

  def test_dense_dimension_mismatch_fails(self):
    with self.assertRaisesRegex(Exception, "Expected 2 values"):
      self._write([_item("dim-1", dense=[1.0])])

  def test_fields_fn(self):
    self._write([_item("fn-1", dense=[1.0, 0.0])],
                fields_fn=lambda item: {
                    "content": item.content_string.upper(),
                    "embedding": item.dense_embedding, })
    fields = self._get("fn-1")
    self.assertEqual(fields["content"], "CONTENT OF FN-1")
    self.assertNotIn("source", fields)


if __name__ == "__main__":
  unittest.main()
