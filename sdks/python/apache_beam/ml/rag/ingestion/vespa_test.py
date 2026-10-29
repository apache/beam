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

import unittest
from unittest import mock

try:
  from vespa.io import VespaResponse
  VESPA_AVAILABLE = True
except ImportError:
  VESPA_AVAILABLE = False

import apache_beam as beam
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteTransform
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline

if VESPA_AVAILABLE:
  from apache_beam.ml.rag.ingestion import vespa as vespa_module
  from apache_beam.ml.rag.ingestion.vespa import VespaConnectionParameters
  from apache_beam.ml.rag.ingestion.vespa import VespaFeedError
  from apache_beam.ml.rag.ingestion.vespa import VespaWriteConfig
  from apache_beam.ml.rag.ingestion.vespa import _VespaWriteFn


def _item(
    id="1",
    text="hello",
    dense=None,
    sparse=None,
    metadata=None) -> EmbeddableItem:
  return EmbeddableItem(
      id=id,
      content=Content(text=text),
      metadata=metadata or {},
      embedding=Embedding(dense_embedding=dense, sparse_embedding=sparse),
  )


def _params():
  return VespaConnectionParameters(url="http://localhost", port=8080)


def _ok(status=200):
  return VespaResponse(
      json={}, status_code=status, url="u", operation_type="feed")


@unittest.skipIf(not VESPA_AVAILABLE, "pyvespa not installed")
class TestVespaConnectionParameters(unittest.TestCase):
  def test_url_required(self):
    with self.assertRaises(ValueError):
      VespaConnectionParameters(url="")

  def test_cert_without_key_raises(self):
    with self.assertRaises(ValueError):
      VespaConnectionParameters(url="https://x", cert="c.pem")

  def test_token_and_cert_raises(self):
    with self.assertRaises(ValueError):
      VespaConnectionParameters(
          url="https://x",
          cert="c.pem",
          key="k.pem",
          vespa_cloud_secret_token="t")

  def test_mtls_ok(self):
    VespaConnectionParameters(url="https://x", cert="c.pem", key="k.pem")


@unittest.skipIf(not VESPA_AVAILABLE, "pyvespa not installed")
class TestVespaWriteConfig(unittest.TestCase):
  def test_empty_schema_raises(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(connection_params=_params(), schema="")

  def test_invalid_operation_raises(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(
          connection_params=_params(), schema="doc", operation_type="delete")

  def test_batch_size_zero_raises(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(connection_params=_params(), schema="doc", batch_size=0)

  def test_no_embedding_field_raises(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(
          connection_params=_params(), schema="doc", dense_embedding_field=None)

  def test_no_embedding_field_ok_with_fields_fn(self):
    VespaWriteConfig(
        connection_params=_params(),
        schema="doc",
        dense_embedding_field=None,
        fields_fn=lambda item: {"x": 1})

  def test_empty_field_name_raises(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(
          connection_params=_params(), schema="doc", content_field="")

  def test_duplicate_reserved_fields_raise(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(
          connection_params=_params(), schema="doc", content_field="embedding")


@unittest.skipIf(not VESPA_AVAILABLE, "pyvespa not installed")
class TestVespaConverter(unittest.TestCase):
  def _convert(self, item, **kwargs):
    config = VespaWriteConfig(
        connection_params=_params(), schema="doc", **kwargs)
    return config.create_converter()(item)

  def test_dense_only(self):
    doc = self._convert(_item(dense=[0.1, 0.2]))
    self.assertEqual(
        doc, {
            "id": "1", "fields": {
                "embedding": [0.1, 0.2], "content": "hello"
            }
        })

  def test_sparse_as_mapped_tensor(self):
    doc = self._convert(
        _item(sparse=([3, 17], [0.5, 1.5])),
        dense_embedding_field=None,
        sparse_embedding_field="sparse")
    self.assertEqual(doc["fields"]["sparse"], {"3": 0.5, "17": 1.5})
    self.assertNotIn("embedding", doc["fields"])

  def test_dense_and_sparse(self):
    doc = self._convert(
        _item(dense=[1.0], sparse=([1], [2.0])),
        sparse_embedding_field="sparse")
    self.assertEqual(doc["fields"]["embedding"], [1.0])
    self.assertEqual(doc["fields"]["sparse"], {"1": 2.0})

  def test_sparse_length_mismatch_raises(self):
    with self.assertRaises(ValueError):
      self._convert(
          _item(sparse=([1, 2], [0.5])), sparse_embedding_field="sparse")

  def test_no_matching_embedding_raises(self):
    # Item only has sparse, but only the dense field is configured.
    with self.assertRaises(ValueError):
      self._convert(_item(sparse=([1], [1.0])))

  def test_missing_id_raises(self):
    with self.assertRaises(ValueError):
      self._convert(_item(id="", dense=[1.0]))

  def test_content_skipped_when_none(self):
    doc = self._convert(_item(dense=[1.0]), content_field=None)
    self.assertNotIn("content", doc["fields"])

  def test_all_metadata_copied_by_default(self):
    doc = self._convert(
        _item(dense=[1.0], metadata={
            "source": "a.pdf", "page": 3
        }))
    self.assertEqual(doc["fields"]["source"], "a.pdf")
    self.assertEqual(doc["fields"]["page"], 3)

  def test_metadata_fields_rename_and_filter(self):
    doc = self._convert(
        _item(
            dense=[1.0], metadata={
                "source": "a.pdf", "page": 3, "skip": True
            }),
        metadata_fields={
            "source": "src", "page": "page_no", "absent": "never"
        })
    self.assertEqual(doc["fields"]["src"], "a.pdf")
    self.assertEqual(doc["fields"]["page_no"], 3)
    self.assertNotIn("skip", doc["fields"])
    self.assertNotIn("never", doc["fields"])

  def test_metadata_collision_raises(self):
    with self.assertRaises(ValueError):
      self._convert(_item(dense=[1.0], metadata={"content": "x"}))

  def test_int_id_stringified(self):
    item = _item(dense=[1.0])
    item.id = 42
    self.assertEqual(self._convert(item)["id"], "42")

  def test_fields_fn_overrides_mapping(self):
    doc = self._convert(
        _item(dense=[1.0]),
        fields_fn=lambda item: {"title": item.content_string.upper()})
    self.assertEqual(doc, {"id": "1", "fields": {"title": "HELLO"}})

  def test_fields_fn_must_return_dict(self):
    with self.assertRaises(TypeError):
      self._convert(_item(dense=[1.0]), fields_fn=lambda item: ["x"])


@unittest.skipIf(not VESPA_AVAILABLE, "pyvespa not installed")
class TestVespaWriteFn(unittest.TestCase):
  def setUp(self):
    patcher = mock.patch.object(vespa_module, "Vespa")
    self.vespa_cls = patcher.start()
    self.addCleanup(patcher.stop)
    self.app = self.vespa_cls.return_value
    self.fed = []

    def feed_iterable(docs, callback=None, **kwargs):
      self.fed.append((list(docs), kwargs))
      for d in docs:
        callback(_ok(), d["id"])

    self.app.feed_iterable.side_effect = feed_iterable

  def _fn(self, **kwargs):
    config = VespaWriteConfig(
        connection_params=VespaConnectionParameters(
            url="https://x",
            vespa_cloud_secret_token="tok",
            additional_headers={"X-A": "1"}),
        schema="doc",
        **kwargs)
    fn = _VespaWriteFn(config)
    fn.setup()
    fn.start_bundle()
    return fn

  def test_setup_passes_connection_params(self):
    self._fn()
    self.vespa_cls.assert_called_once_with(
        url="https://x",
        port=None,
        cert=None,
        key=None,
        vespa_cloud_secret_token="tok",
        additional_headers={"X-A": "1"})

  def test_flush_on_batch_size_and_finish_bundle(self):
    fn = self._fn(batch_size=2)
    for i in range(5):
      fn.process({"id": str(i), "fields": {}})
    fn.finish_bundle()
    self.assertEqual([len(docs) for docs, _ in self.fed], [2, 2, 1])

  def test_feed_kwargs(self):
    fn = self._fn(namespace="ns", max_workers=3)
    fn.process({"id": "1", "fields": {}})
    fn.finish_bundle()
    _, kwargs = self.fed[0]
    self.assertEqual(kwargs["schema"], "doc")
    self.assertEqual(kwargs["namespace"], "ns")
    self.assertEqual(kwargs["operation_type"], "feed")
    self.assertEqual(kwargs["max_workers"], 3)
    self.assertNotIn("create", kwargs)

  def test_update_uses_create(self):
    fn = self._fn(operation_type="update")
    fn.process({"id": "1", "fields": {}})
    fn.finish_bundle()
    _, kwargs = self.fed[0]
    self.assertEqual(kwargs["operation_type"], "update")
    self.assertTrue(kwargs["create"])

  def test_empty_bundle_does_not_call_vespa(self):
    fn = self._fn()
    fn.finish_bundle()
    self.app.feed_iterable.assert_not_called()

  def test_failed_documents_raise(self):
    def feed_iterable(docs, callback=None, **kwargs):
      for d in docs:
        status = 400 if d["id"] == "bad" else 200
        callback(_ok(status), d["id"])

    self.app.feed_iterable.side_effect = feed_iterable
    fn = self._fn()
    fn.process({"id": "good", "fields": {}})
    fn.process({"id": "bad", "fields": {}})
    with self.assertRaises(VespaFeedError) as ctx:
      fn.finish_bundle()
    self.assertIn("1 of 2", str(ctx.exception))
    self.assertIn("bad", str(ctx.exception))

  def test_flush_without_setup_raises(self):
    config = VespaWriteConfig(connection_params=_params(), schema="doc")
    fn = _VespaWriteFn(config)
    fn.start_bundle()
    fn.process({"id": "1", "fields": {}})
    with self.assertRaises(RuntimeError):
      fn.finish_bundle()

  def test_display_data(self):
    data = self._fn(batch_size=7).display_data()
    self.assertEqual(data["schema"], "doc")
    self.assertEqual(data["batch_size"], 7)


@unittest.skipIf(not VESPA_AVAILABLE, "pyvespa not installed")
class TestVespaWritePipeline(unittest.TestCase):
  def test_pipeline_feeds_converted_docs(self):
    fed = []

    def feed_iterable(docs, callback=None, **kwargs):
      for d in docs:
        fed.append(d)
        callback(_ok(), d["id"])

    with mock.patch.object(vespa_module, "Vespa") as vespa_cls:
      vespa_cls.return_value.feed_iterable.side_effect = feed_iterable
      config = VespaWriteConfig(
          connection_params=_params(),
          schema="doc",
          sparse_embedding_field="sparse")
      with TestPipeline() as p:
        _ = (
            p
            | beam.Create([
                _item(id="a", dense=[1.0], sparse=([2], [0.5])),
                _item(id="b", dense=[2.0], metadata={"lang": "en"}),
            ])
            | VectorDatabaseWriteTransform(config))
    by_id = {d["id"]: d["fields"] for d in fed}
    self.assertEqual(set(by_id), {"a", "b"})
    self.assertEqual(by_id["a"]["sparse"], {"2": 0.5})
    self.assertEqual(by_id["b"]["lang"], "en")


if __name__ == "__main__":
  unittest.main()
