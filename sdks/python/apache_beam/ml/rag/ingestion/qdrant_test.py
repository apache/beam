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
  from qdrant_client import models
  from qdrant_client.common.client_exceptions import ResourceExhaustedResponse
  from qdrant_client.http.exceptions import ResponseHandlingException
  from qdrant_client.http.exceptions import UnexpectedResponse

  QDRANT_AVAILABLE = True
except ImportError:
  QDRANT_AVAILABLE = False

import grpc
from objsize import get_deep_size

from apache_beam.ml.rag.ingestion.qdrant import QdrantConnectionParameters
from apache_beam.ml.rag.ingestion.qdrant import QdrantWriteConfig
from apache_beam.ml.rag.ingestion.qdrant import _QdrantWriteFn
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import EmbeddableItem
from apache_beam.ml.rag.types import Embedding


class TestQdrantConnectionParameters(unittest.TestCase):
  def test_no_params_raises_value_error(self):
    with self.assertRaises(ValueError):
      QdrantConnectionParameters()

  def test_location_is_sufficient(self):
    QdrantConnectionParameters(location=":memory:")

  def test_url_is_sufficient(self):
    QdrantConnectionParameters(url="http://localhost:6333")

  def test_host_is_sufficient(self):
    QdrantConnectionParameters(host="localhost")

  def test_path_is_sufficient(self):
    QdrantConnectionParameters(path="/tmp/qdrant")


class TestQdrantWriteConfig(unittest.TestCase):
  def test_empty_collection_name_raises_value_error(self):
    with self.assertRaises(ValueError):
      QdrantWriteConfig(
          connection_params=QdrantConnectionParameters(location=":memory:"),
          collection_name="",
      )

  def test_none_collection_name_raises_value_error(self):
    with self.assertRaises(ValueError):
      QdrantWriteConfig(
          connection_params=QdrantConnectionParameters(location=":memory:"),
          collection_name=None,
      )

  def test_batch_size_zero_raises_value_error(self):
    with self.assertRaises(ValueError):
      QdrantWriteConfig(
          connection_params=QdrantConnectionParameters(location=":memory:"),
          collection_name="test",
          batch_size=0,
      )

  def test_batch_size_negative_raises_value_error(self):
    with self.assertRaises(ValueError):
      QdrantWriteConfig(
          connection_params=QdrantConnectionParameters(location=":memory:"),
          collection_name="test",
          batch_size=-1,
      )

  def test_display_data(self):
    config = QdrantWriteConfig(
        connection_params=QdrantConnectionParameters(location=":memory:"),
        collection_name="test",
        batch_size=100,
        max_batch_byte_size=5000,
    )
    fn = _QdrantWriteFn(config)
    data = fn.display_data()
    self.assertEqual(data["collection"], "test")
    self.assertEqual(data["batch_size"], 100)
    self.assertEqual(data["max_batch_byte_size"], 5000)

  def test_for_cloud_creates_connection(self):
    params = QdrantConnectionParameters.for_cloud(
        url="https://test.cloud.qdrant.io",
        api_key="my-key",
    )
    self.assertEqual(params.url, "https://test.cloud.qdrant.io")
    self.assertEqual(params.api_key, "my-key")
    self.assertTrue(params.https)

  def test_for_host_creates_connection(self):
    params = QdrantConnectionParameters.for_host(host="localhost", port=6333)
    self.assertEqual(params.host, "localhost")
    self.assertEqual(params.port, 6333)

  def test_in_memory_creates_connection(self):
    params = QdrantConnectionParameters.in_memory()
    self.assertEqual(params.location, ":memory:")

  def test_for_url_creates_connection(self):
    params = QdrantConnectionParameters.for_url(url="http://localhost:6333")
    self.assertEqual(params.url, "http://localhost:6333")

  def test_kwargs_passthrough(self):
    config = QdrantWriteConfig(
        connection_params=QdrantConnectionParameters(location=":memory:"),
        collection_name="test",
        kwargs={"parallel": 4},
    )
    self.assertEqual(config.kwargs, {"parallel": 4})


@unittest.skipIf(not QDRANT_AVAILABLE, "qdrant dependencies not installed.")
class TestQdrantCreateConverter(unittest.TestCase):
  def setUp(self):
    self.config = QdrantWriteConfig(
        connection_params=QdrantConnectionParameters(location=":memory:"),
        collection_name="test",
    )
    self.convert = self.config.create_converter()

  def test_dense_embedding_only(self):
    item = EmbeddableItem(
        id="1",
        content=Content(text="test"),
        embedding=Embedding(dense_embedding=[1.0, 2.0]),
    )
    result = self.convert(item)
    self.assertIsInstance(result, models.PointStruct)
    self.assertEqual(result.id, 1)
    self.assertEqual(result.vector, {"dense": [1.0, 2.0]})
    self.assertIsNone(result.payload)

  def test_sparse_embedding_only(self):
    item = EmbeddableItem(
        id="2",
        content=Content(text="test"),
        embedding=Embedding(sparse_embedding=([0, 1], [0.5, 0.3])),
    )
    result = self.convert(item)
    self.assertIsInstance(result, models.PointStruct)
    self.assertIn("sparse", result.vector)
    sparse_vec = result.vector["sparse"]
    self.assertIsInstance(sparse_vec, models.SparseVector)
    self.assertEqual(sparse_vec.indices, [0, 1])
    self.assertEqual(sparse_vec.values, [0.5, 0.3])

  def test_both_dense_and_sparse(self):
    item = EmbeddableItem(
        id="3",
        content=Content(text="test"),
        embedding=Embedding(
            dense_embedding=[1.0, 2.0],
            sparse_embedding=([0], [0.9]),
        ),
    )
    result = self.convert(item)
    self.assertEqual(set(result.vector.keys()), {"dense", "sparse"})
    self.assertEqual(result.vector["dense"], [1.0, 2.0])
    self.assertEqual(result.id, 3)

  def test_raises_when_no_embedding(self):
    item = EmbeddableItem(
        id="4",
        content=Content(text="test"),
    )
    with self.assertRaises(ValueError):
      self.convert(item)

  def test_string_digit_id_converted_to_int(self):
    item = EmbeddableItem(
        id="42",
        content=Content(text="test"),
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
    )
    result = self.convert(item)
    self.assertEqual(result.id, 42)
    self.assertIsInstance(result.id, int)

  def test_non_digit_string_id_preserved(self):
    item = EmbeddableItem(
        id="abc-123",
        content=Content(text="test"),
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
    )
    result = self.convert(item)
    self.assertEqual(result.id, "abc-123")
    self.assertIsInstance(result.id, str)

  def test_integer_id_preserved(self):
    item = EmbeddableItem(
        id="99",
        content=Content(text="test"),
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
    )
    result = self.convert(item)
    self.assertEqual(result.id, 99)
    self.assertIsInstance(result.id, int)

  def test_none_metadata_becomes_none_payload(self):
    item = EmbeddableItem(
        id="1",
        content=Content(text="test"),
        embedding=Embedding(dense_embedding=[0.1, 0.2]),
        metadata={},
    )
    result = self.convert(item)
    self.assertIsNone(result.payload)

  def test_custom_vector_keys(self):
    config = QdrantWriteConfig(
        connection_params=QdrantConnectionParameters(location=":memory:"),
        collection_name="test",
        dense_embedding_key="my_dense",
        sparse_embedding_key="my_sparse",
    )
    convert = config.create_converter()
    item = EmbeddableItem(
        id="1",
        content=Content(text="test"),
        embedding=Embedding(
            dense_embedding=[1.0],
            sparse_embedding=([0], [0.5]),
        ),
    )
    result = convert(item)
    self.assertIn("my_dense", result.vector)
    self.assertIn("my_sparse", result.vector)
    self.assertNotIn("dense", result.vector)
    self.assertNotIn("sparse", result.vector)

  def test_payload_includes_metadata(self):
    item = EmbeddableItem(
        id="1",
        content=Content(text="test"),
        embedding=Embedding(dense_embedding=[1.0]),
        metadata={
            "source": "test", "score": 0.95
        },
    )
    result = self.convert(item)
    self.assertEqual(result.payload, {"source": "test", "score": 0.95})

  def test_convert_from_text_factory(self):
    item = EmbeddableItem.from_text("hello", metadata={"source": "test"})
    item.embedding = Embedding(dense_embedding=[0.5, 0.5])
    result = self.convert(item)
    self.assertIsInstance(result, models.PointStruct)
    self.assertIn("dense", result.vector)


@unittest.skipIf(not QDRANT_AVAILABLE, "qdrant dependencies not installed.")
class TestQdrantWriteFnBatching(unittest.TestCase):
  def setUp(self):
    self.config = QdrantWriteConfig(
        connection_params=QdrantConnectionParameters(location=":memory:"),
        collection_name="test",
        batch_size=3,
    )
    self.fn = _QdrantWriteFn(self.config)
    self.fn._client = mock.MagicMock()
    self.fn.start_bundle()

  def test_batch_size_triggers_flush_correctly(self):
    client = self.fn._client
    for i in range(5):
      self.fn.process(
          EmbeddableItem(
              id=str(i),
              content=Content(text="test"),
              embedding=Embedding(dense_embedding=[float(i)]),
          ))
    self.fn.finish_bundle()

    self.assertEqual(client.upsert.call_count, 2)
    first = client.upsert.call_args_list[0][1]["points"]
    second = client.upsert.call_args_list[1][1]["points"]
    self.assertEqual(len(first), 3)
    self.assertEqual(len(second), 2)
    self.assertEqual(first[0].id, "0")
    self.assertEqual(first[1].id, "1")
    self.assertEqual(first[2].id, "2")
    self.assertEqual(second[0].id, "3")
    self.assertEqual(second[1].id, "4")

  def test_partial_batch_flushed_on_finish_bundle(self):
    for i in range(2):
      self.fn.process(
          EmbeddableItem(
              id=str(i),
              content=Content(text="test"),
              embedding=Embedding(dense_embedding=[float(i)]),
          ))
    self.fn.finish_bundle()

    points = self.fn._client.upsert.call_args[1]["points"]
    self.assertEqual(len(points), 2)

  def test_byte_size_exceeded_triggers_flush(self):
    item = EmbeddableItem(
        id="1",
        content=Content(
            text="a" * 256,
            image=b"x" * 1024,
        ),
    )
    item_size = get_deep_size(item)

    config = QdrantWriteConfig(
        connection_params=QdrantConnectionParameters(location=":memory:"),
        collection_name="test",
        batch_size=10,
        max_batch_byte_size=item_size * 2,
    )
    fn = _QdrantWriteFn(config)
    fn._client = mock.MagicMock()
    fn.start_bundle()
    client = fn._client

    for i in range(3):
      fn.process(
          EmbeddableItem(
              id=str(i),
              content=Content(
                  text="a" * 256,
                  image=b"x" * 1024,
              ),
          ))
    fn.finish_bundle()

    self.assertEqual(client.upsert.call_count, 2)
    first = client.upsert.call_args_list[0][1]["points"]
    second = client.upsert.call_args_list[1][1]["points"]
    self.assertEqual(len(first), 2)
    self.assertEqual(len(second), 1)


@unittest.skipIf(not QDRANT_AVAILABLE, "qdrant dependencies not installed.")
class TestQdrantWriteFnRetries(unittest.TestCase):
  def setUp(self):
    self.config = QdrantWriteConfig(
        connection_params=QdrantConnectionParameters(location=":memory:"),
        collection_name="test",
    )
    self.fn = _QdrantWriteFn(self.config)
    self.fn._client = mock.MagicMock()
    self.fn._batch = [
        EmbeddableItem(
            id="1",
            content=Content(text="test"),
            embedding=Embedding(dense_embedding=[1.0]),
        )
    ]
    self.fn._batch_byte_size = 100

  def test_retry_on_unexpected_response(self):
    self.fn._client.upsert.side_effect = [
        UnexpectedResponse(429, "error", b"", None),
        None,
    ]
    with mock.patch("time.sleep") as mock_sleep:
      self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 2)
    mock_sleep.assert_called_once_with(2)

  def test_retry_on_response_handling_exception(self):
    self.fn._client.upsert.side_effect = [
        ResponseHandlingException(Exception("error")),
        None,
    ]
    with mock.patch("time.sleep") as mock_sleep:
      self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 2)
    mock_sleep.assert_called_once_with(2)

  def test_retry_on_grpc_error(self):
    self.fn._client.upsert.side_effect = [
        grpc.RpcError("error"),
        None,
    ]
    with mock.patch("time.sleep") as mock_sleep:
      self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 2)
    mock_sleep.assert_called_once_with(2)

  def test_rate_limit_does_not_increment_attempt(self):
    exc = ResourceExhaustedResponse("rate limited", 0)
    exc.retry_after_s = 0.01
    self.fn._client.upsert.side_effect = [exc, None]
    with mock.patch("time.sleep") as mock_sleep:
      self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 2)
    mock_sleep.assert_called_once_with(0.01)

  def test_multiple_rate_limits_dont_exhaust_retries(self):
    exc = ResourceExhaustedResponse("rate limited", 0)
    exc.retry_after_s = 0.01
    self.fn._client.upsert.side_effect = [exc, exc, exc, None]
    with mock.patch("time.sleep") as mock_sleep:
      self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 4)
    self.assertEqual(mock_sleep.call_count, 3)

  def test_rate_limit_then_error_then_success(self):
    exc_rate = ResourceExhaustedResponse("rate limited", 0)
    exc_rate.retry_after_s = 0.01
    exc_error = UnexpectedResponse(429, "error", b"", None)
    self.fn._client.upsert.side_effect = [exc_error, exc_rate, None]
    with mock.patch("time.sleep") as mock_sleep:
      self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 3)
    self.assertEqual(mock_sleep.call_args_list[0], mock.call(2))
    self.assertEqual(mock_sleep.call_args_list[1], mock.call(0.01))

  def test_exponential_backoff_values(self):
    self.fn._client.upsert.side_effect = [
        UnexpectedResponse(429, "e1", b"", None),
        UnexpectedResponse(429, "e2", b"", None),
        UnexpectedResponse(429, "e3", b"", None),
        None,
    ]
    with mock.patch("time.sleep") as mock_sleep:
      self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 4)
    self.assertEqual(mock_sleep.call_args_list[0], mock.call(2))
    self.assertEqual(mock_sleep.call_args_list[1], mock.call(4))
    self.assertEqual(mock_sleep.call_args_list[2], mock.call(8))

  def test_raises_after_max_retries(self):
    self.fn._client.upsert.side_effect = [
        UnexpectedResponse(429, "e1", b"", None),
        UnexpectedResponse(429, "e2", b"", None),
        UnexpectedResponse(429, "e3", b"", None),
        UnexpectedResponse(429, "e4", b"", None),
    ]
    with mock.patch("time.sleep") as mock_sleep:
      with self.assertRaises(UnexpectedResponse):
        self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 4)
    self.assertEqual(mock_sleep.call_count, 3)

  def test_raises_on_last_non_rate_limit_attempt(self):
    exc_rate = ResourceExhaustedResponse("rate limited", 0)
    exc_rate.retry_after_s = 0.01
    self.fn._client.upsert.side_effect = [
        exc_rate,
        UnexpectedResponse(429, "e1", b"", None),
        UnexpectedResponse(429, "e2", b"", None),
        UnexpectedResponse(429, "e3", b"", None),
        UnexpectedResponse(429, "e4", b"", None),
    ]
    with mock.patch("time.sleep") as mock_sleep:
      with self.assertRaises(UnexpectedResponse):
        self.fn._flush()
    self.assertEqual(self.fn._client.upsert.call_count, 5)


if __name__ == "__main__":
  unittest.main()
