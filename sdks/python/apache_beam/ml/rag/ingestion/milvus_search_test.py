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
#
import unittest
from unittest import mock

from parameterized import parameterized

try:
  from apache_beam.ml.rag.ingestion import milvus_search as milvus_search_module
  from apache_beam.ml.rag.ingestion.milvus_search import MilvusVectorWriterConfig
  from apache_beam.ml.rag.ingestion.milvus_search import MilvusWriteConfig
  from apache_beam.ml.rag.ingestion.milvus_search import _MilvusSink
  from apache_beam.ml.rag.utils import MilvusConnectionParameters
except ImportError as e:
  raise unittest.SkipTest(f'Milvus dependencies not installed: {str(e)}')


class TestMilvusWriteConfig(unittest.TestCase):
  """Unit tests for MilvusWriteConfig validation errors."""
  def test_empty_collection_name_raises_error(self):
    """Test that empty collection name raises ValueError."""
    with self.assertRaises(ValueError) as context:
      MilvusWriteConfig(collection_name="")

    self.assertIn("Collection name must be provided", str(context.exception))

  def test_none_collection_name_raises_error(self):
    """Test that None collection name raises ValueError."""
    with self.assertRaises(ValueError) as context:
      MilvusWriteConfig(collection_name=None)

    self.assertIn("Collection name must be provided", str(context.exception))


class TestMilvusVectorWriterConfig(unittest.TestCase):
  """Unit tests for MilvusVectorWriterConfig validation and functionality."""
  def test_valid_config_creation(self):
    """Test creation of valid MilvusVectorWriterConfig."""
    connection_params = MilvusConnectionParameters(uri="http://localhost:19530")
    write_config = MilvusWriteConfig(collection_name="test_collection")

    config = MilvusVectorWriterConfig(
        connection_params=connection_params, write_config=write_config)

    self.assertEqual(config.connection_params, connection_params)
    self.assertEqual(config.write_config, write_config)
    self.assertIsNotNone(config.column_specs)

  def test_create_converter_returns_callable(self):
    """Test that create_converter returns a callable function."""
    connection_params = MilvusConnectionParameters(uri="http://localhost:19530")
    write_config = MilvusWriteConfig(collection_name="test_collection")

    config = MilvusVectorWriterConfig(
        connection_params=connection_params, write_config=write_config)

    converter = config.create_converter()
    self.assertTrue(callable(converter))

  def test_create_write_transform_returns_ptransform(self):
    """Test that create_write_transform returns a PTransform."""
    connection_params = MilvusConnectionParameters(uri="http://localhost:19530")
    write_config = MilvusWriteConfig(collection_name="test_collection")

    config = MilvusVectorWriterConfig(
        connection_params=connection_params, write_config=write_config)

    transform = config.create_write_transform()
    self.assertIsNotNone(transform)

  def test_default_column_specs_has_expected_fields(self):
    """Test that default column specs include expected fields."""
    column_specs = MilvusVectorWriterConfig.default_column_specs()

    self.assertIsInstance(column_specs, list)
    self.assertGreater(len(column_specs), 0)

    column_names = [spec.column_name for spec in column_specs]
    expected_fields = [
        "id", "embedding", "sparse_embedding", "content", "metadata"
    ]

    for field in expected_fields:
      self.assertIn(field, column_names)

  @parameterized.expand([
      # Invalid connection parameters - empty URI.
      (
          lambda: (
              MilvusConnectionParameters(uri=""), MilvusWriteConfig(
                  collection_name="test_collection")),
          "URI must be provided"),
      # Invalid write config - empty collection name.
      (
          lambda: (
              MilvusConnectionParameters(uri="http://localhost:19530"),
              MilvusWriteConfig(collection_name="")),
          "Collection name must be provided"),
  ])
  def test_invalid_configuration_parameters(
      self, create_params, expected_error_msg):
    """Test validation errors for invalid configuration parameters."""
    with self.assertRaises(ValueError) as context:
      connection_params, write_config = create_params()
      MilvusVectorWriterConfig(
          connection_params=connection_params, write_config=write_config)

    self.assertIn(expected_error_msg, str(context.exception))


class TestMilvusSinkClientReuse(unittest.TestCase):
  """Unit tests for Milvus sink client reuse.

  Verifies that ``_MilvusSink.write`` reuses the client established in
  ``__enter__`` instead of constructing (and leaking) a new client on every
  write.
  """
  def _sink(self):
    connection_params = MilvusConnectionParameters(uri="http://localhost:19530")
    write_config = MilvusWriteConfig(collection_name="test_collection")
    return _MilvusSink(connection_params, write_config)

  def test_write_reuses_enter_client(self):
    """write() must not construct a new MilvusClient per call."""
    with mock.patch.object(milvus_search_module,
                           'MilvusClient') as mock_client_cls:
      with self._sink() as sink:
        # One client created on __enter__.
        self.assertEqual(mock_client_cls.call_count, 1)

        sink.write([{"id": 1}])
        sink.write([{"id": 2}])

        # write() reuses that client; no new construction.
        self.assertEqual(mock_client_cls.call_count, 1)
        self.assertEqual(mock_client_cls.return_value.upsert.call_count, 2)

      # Client closed once on __exit__.
      self.assertEqual(mock_client_cls.return_value.close.call_count, 1)


if __name__ == '__main__':
  unittest.main()
