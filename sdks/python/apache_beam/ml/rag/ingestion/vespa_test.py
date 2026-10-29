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
import os
import tempfile
import unittest
from unittest import mock

import requests
import requests_mock

import apache_beam as beam
from apache_beam.ml.rag.ingestion import vespa as vespa_module
from apache_beam.ml.rag.ingestion.base import VectorDatabaseWriteTransform
from apache_beam.ml.rag.ingestion.vespa import VespaConnectionParameters
from apache_beam.ml.rag.ingestion.vespa import VespaFeedError
from apache_beam.ml.rag.ingestion.vespa import VespaWriteConfig
from apache_beam.ml.rag.ingestion.vespa import _VespaWriteFn
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline

_DOC_URL = "http://localhost:8080/document/v1/doc/doc/docid/"


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


def _params(**kwargs):
  return VespaConnectionParameters(url="http://localhost", port=8080, **kwargs)


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
          url="https://x", cert="c.pem", key="k.pem", auth_token="t")

  def test_mtls_ok(self):
    VespaConnectionParameters(url="https://x", cert="c.pem", key="k.pem")

  def test_base_url(self):
    self.assertEqual(
        VespaConnectionParameters(url="http://h/", port=8080).base_url(),
        "http://h:8080")
    self.assertEqual(
        VespaConnectionParameters(url="https://h").base_url(), "https://h")


class TestVespaWriteConfig(unittest.TestCase):
  def test_empty_schema_raises(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(connection_params=_params(), schema="")

  def test_invalid_operation_raises(self):
    for op in ("delete", "feed"):
      with self.assertRaises(ValueError):
        VespaWriteConfig(
            connection_params=_params(), schema="doc", operation_type=op)

  def test_batch_size_zero_raises(self):
    with self.assertRaises(ValueError):
      VespaWriteConfig(connection_params=_params(), schema="doc", batch_size=0)

  def test_invalid_client_settings_raise(self):
    for kwargs in ({"max_concurrent_requests": 0}, {"max_retries": -1},
                   {"timeout_s": 0}):
      with self.assertRaises(ValueError):
        VespaWriteConfig(connection_params=_params(), schema="doc", **kwargs)

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


class TestVespaWriteFn(unittest.TestCase):
  def setUp(self):
    patcher = mock.patch.object(vespa_module.time, "sleep")
    self.sleep = patcher.start()
    self.addCleanup(patcher.stop)
    self.http = requests_mock.Mocker()
    self.http.start()
    self.addCleanup(self.http.stop)

  def _fn(self, params=None, **kwargs):
    config = VespaWriteConfig(
        connection_params=params or _params(), schema="doc", **kwargs)
    fn = _VespaWriteFn(config)
    fn.setup()
    self.addCleanup(fn.teardown)
    fn.start_bundle()
    return fn

  def _write(self, fn, *docs):
    for doc in docs:
      fn.process(doc)
    fn.finish_bundle()

  def test_put_posts_fields(self):
    self.http.post(requests_mock.ANY, status_code=200)
    self._write(self._fn(), {"id": "1", "fields": {"a": 1}})
    request = self.http.request_history[0]
    self.assertEqual(request.method, "POST")
    self.assertEqual(request.url, _DOC_URL + "1")
    self.assertEqual(request.json(), {"fields": {"a": 1}})

  def test_update_assigns_fields_with_create(self):
    self.http.put(requests_mock.ANY, status_code=200)
    self._write(
        self._fn(operation_type="update"), {
            "id": "1", "fields": {
                "a": 1, "b": [1.0]
            }
        })
    request = self.http.request_history[0]
    self.assertEqual(request.method, "PUT")
    self.assertEqual(request.qs, {"create": ["true"]})
    self.assertEqual(
        request.json(),
        {"fields": {
            "a": {
                "assign": 1
            }, "b": {
                "assign": [1.0]
            }
        }})

  def test_namespace_and_id_are_url_encoded(self):
    self.http.post(requests_mock.ANY, status_code=200)
    self._write(self._fn(namespace="ns"), {"id": "a/b c?d", "fields": {}})
    self.assertEqual(
        self.http.request_history[0].url,
        "http://localhost:8080/document/v1/ns/doc/docid/a%2Fb%20c%3Fd")

  def test_headers_and_token(self):
    self.http.post(requests_mock.ANY, status_code=200)
    fn = self._fn(
        params=_params(auth_token="tok", additional_headers={"X-A": "1"}))
    self._write(fn, {"id": "1", "fields": {}})
    headers = self.http.request_history[0].headers
    self.assertEqual(headers["Authorization"], "Bearer tok")
    self.assertEqual(headers["X-A"], "1")

  def test_missing_cert_file_fails_setup(self):
    config = VespaWriteConfig(
        connection_params=_params(cert="/nope/c.pem", key="/nope/k.pem"),
        schema="doc")
    with self.assertRaisesRegex(FileNotFoundError, "/nope/c.pem"):
      _VespaWriteFn(config).setup()

  def test_cert_files_set_on_session(self):
    with tempfile.TemporaryDirectory() as d:
      cert, key = os.path.join(d, "c.pem"), os.path.join(d, "k.pem")
      for path in (cert, key):
        open(path, "w").close()
      fn = self._fn(params=_params(cert=cert, key=key))
      self.assertEqual(fn._session.cert, (cert, key))

  def test_flush_on_batch_size_and_finish_bundle(self):
    self.http.post(requests_mock.ANY, status_code=200)
    fn = self._fn(batch_size=2)
    for i in range(3):
      fn.process({"id": str(i), "fields": {}})
    self.assertEqual(self.http.call_count, 2)
    fn.finish_bundle()
    self.assertEqual(self.http.call_count, 3)

  def test_empty_bundle_does_not_call_vespa(self):
    self._fn().finish_bundle()
    self.assertEqual(self.http.call_count, 0)

  def test_429_retried_without_counting(self):
    self.http.post(
        requests_mock.ANY, [{
            "status_code": 429
        }] * 5 + [{
            "status_code": 200
        }])
    self._write(self._fn(max_retries=0), {"id": "1", "fields": {}})
    self.assertEqual(self.http.call_count, 6)

  def test_5xx_retried_up_to_max_retries(self):
    self.http.post(requests_mock.ANY, status_code=503, json={"message": "busy"})
    fn = self._fn(max_retries=2)
    with self.assertRaisesRegex(VespaFeedError, "busy"):
      self._write(fn, {"id": "1", "fields": {}})
    self.assertEqual(self.http.call_count, 3)

  def test_5xx_then_success(self):
    self.http.post(
        requests_mock.ANY, [{
            "status_code": 500
        }, {
            "status_code": 200
        }])
    self._write(self._fn(), {"id": "1", "fields": {}})
    self.assertEqual(self.http.call_count, 2)

  def test_connection_error_retried(self):
    self.http.post(
        requests_mock.ANY, [{
            "exc": requests.ConnectionError("refused")
        }, {
            "status_code": 200
        }])
    self._write(self._fn(), {"id": "1", "fields": {}})
    self.assertEqual(self.http.call_count, 2)

  def test_connection_error_exhausts_retries(self):
    self.http.post(requests_mock.ANY, exc=requests.Timeout("slow"))
    with self.assertRaisesRegex(VespaFeedError, "Timeout"):
      self._write(self._fn(max_retries=1), {"id": "1", "fields": {}})
    self.assertEqual(self.http.call_count, 2)

  def test_400_fails_without_retry(self):
    self.http.post(_DOC_URL + "good", status_code=200)
    self.http.post(
        _DOC_URL + "bad",
        status_code=400,
        json={"message": "Field 'page' is not defined"})
    fn = self._fn()
    with self.assertRaises(VespaFeedError) as ctx:
      self._write(
          fn, {
              "id": "good", "fields": {}
          }, {
              "id": "bad", "fields": {
                  "page": 1
              }
          })
    message = str(ctx.exception)
    self.assertIn("1 of 2", message)
    self.assertIn("bad", message)
    self.assertIn("is not defined", message)
    self.assertEqual(self.http.call_count, 2)

  def test_non_json_error_body(self):
    self.http.post(requests_mock.ANY, status_code=403, text="forbidden")
    with self.assertRaisesRegex(VespaFeedError, "forbidden"):
      self._write(self._fn(), {"id": "1", "fields": {}})

  def test_flush_without_setup_raises(self):
    config = VespaWriteConfig(connection_params=_params(), schema="doc")
    fn = _VespaWriteFn(config)
    fn.start_bundle()
    fn.process({"id": "1", "fields": {}})
    with self.assertRaises(RuntimeError):
      fn.finish_bundle()

  def test_teardown_closes_session(self):
    fn = self._fn()
    fn.teardown()
    self.assertIsNone(fn._session)
    self.assertIsNone(fn._executor)

  def test_display_data(self):
    data = self._fn(batch_size=7).display_data()
    self.assertEqual(data["schema"], "doc")
    self.assertEqual(data["batch_size"], 7)


class TestVespaWritePipeline(unittest.TestCase):
  def test_pipeline_feeds_converted_docs(self):
    config = VespaWriteConfig(
        connection_params=_params(),
        schema="doc",
        sparse_embedding_field="sparse")
    with requests_mock.Mocker() as http:
      http.post(requests_mock.ANY, status_code=200)
      with TestPipeline() as p:
        _ = (
            p
            | beam.Create([
                _item(id="a", dense=[1.0], sparse=([2], [0.5])),
                _item(id="b", dense=[2.0], metadata={"lang": "en"}),
            ])
            | VectorDatabaseWriteTransform(config))
      by_id = {
          r.url.rsplit("/", 1)[-1]: json.loads(r.body)["fields"]
          for r in http.request_history
      }
    self.assertEqual(set(by_id), {"a", "b"})
    self.assertEqual(by_id["a"]["sparse"], {"2": 0.5})
    self.assertEqual(by_id["b"]["lang"], "en")


if __name__ == "__main__":
  unittest.main()
