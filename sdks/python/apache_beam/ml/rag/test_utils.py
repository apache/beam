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

import contextlib
import logging
import os
import socket
import tempfile
import unittest
from dataclasses import dataclass
from typing import Callable
from typing import List
from typing import Optional
from typing import cast

from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.utils import retry_with_backoff

# pylint: disable=ungrouped-imports
try:
  import yaml
  from pymilvus import CollectionSchema
  from pymilvus import FieldSchema
  from pymilvus import MilvusClient
  from pymilvus.exceptions import MilvusException
  from pymilvus.milvus_client import IndexParams
  from testcontainers.core.config import testcontainers_config
  from testcontainers.core.generic import DbContainer
  from testcontainers.milvus import MilvusContainer

  from apache_beam.ml.rag.enrichment.milvus_search import MilvusConnectionParameters
except ImportError as e:
  raise unittest.SkipTest(f'RAG test util dependencies not installed: {str(e)}')

_LOGGER = logging.getLogger(__name__)


@dataclass
class VectorDBContainerInfo:
  """Container information for vector database test instances.

  Holds connection details and container reference for testing with
  vector databases like Milvus in containerized environments.
  """
  container: DbContainer
  host: str
  port: int
  user: str = ""
  password: str = ""
  token: str = ""
  id: str = "default"

  @property
  def uri(self) -> str:
    return f"http://{self.host}:{self.port}"


class TestHelpers:
  @staticmethod
  def find_free_port():
    """Find a free port on the local machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
      # Bind to port 0, which asks OS to assign a free port.
      s.bind(('', 0))
      s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
      # Return the port number assigned by OS.
      return s.getsockname()[1]


class CustomMilvusContainer(MilvusContainer):
  """Custom Milvus container with configurable ports and environment setup.

  Extends MilvusContainer to provide custom port binding and environment
  configuration for testing with standalone Milvus instances.
  """
  def __init__(
      self,
      image: str,
      service_container_port,
      healthcheck_container_port,
      **kwargs,
  ) -> None:
    # Skip the parent class's constructor and go straight to
    # GenericContainer.
    super(MilvusContainer, self).__init__(image=image, **kwargs)
    self.port = service_container_port
    self.healthcheck_port = healthcheck_container_port
    self.with_exposed_ports(service_container_port, healthcheck_container_port)

    # Get free host ports.
    service_host_port = TestHelpers.find_free_port()
    healthcheck_host_port = TestHelpers.find_free_port()

    # Bind container and host ports.
    self.with_bind_ports(service_container_port, service_host_port)
    self.with_bind_ports(healthcheck_container_port, healthcheck_host_port)
    self.cmd = "milvus run standalone"

    # Set environment variables needed for Milvus.
    envs = {
        "ETCD_USE_EMBED": "true",
        "ETCD_DATA_DIR": "/var/lib/milvus/etcd",
        "COMMON_STORAGETYPE": "local",
        "METRICS_PORT": str(healthcheck_container_port)
    }
    for env, value in envs.items():
      self.with_env(env, value)


class MilvusTestHelpers:
  """Helper utilities for testing Milvus vector database operations.

  Provides static methods for managing test containers, configuration files,
  and chunk comparison utilities for Milvus-based integration tests.
  """
  # IMPORTANT: When upgrading the Milvus server version, ensure the pymilvus
  # Python SDK client in setup.py is updated to match. Referring to the Milvus
  # release notes compatibility matrix at
  # https://milvus.io/docs/release_notes.md or PyPI at
  # https://pypi.org/project/pymilvus/ for version compatibility.
  # Example: Milvus v2.6.0 requires pymilvus==2.6.0 (exact match required).
  @staticmethod
  def start_db_container(
      image="milvusdb/milvus:v2.5.10",
      max_vec_fields=5,
      vector_client_max_retries=3,
      tc_max_retries=None) -> Optional[VectorDBContainerInfo]:
    service_container_port = TestHelpers.find_free_port()
    healthcheck_container_port = TestHelpers.find_free_port()
    user_yaml_creator = MilvusTestHelpers.create_user_yaml
    with user_yaml_creator(service_container_port, max_vec_fields) as cfg:
      info = None
      original_tc_max_tries = testcontainers_config.max_tries
      if tc_max_retries is not None:
        testcontainers_config.max_tries = tc_max_retries
      for i in range(vector_client_max_retries):
        try:
          vector_db_container = CustomMilvusContainer(
              image=image,
              service_container_port=service_container_port,
              healthcheck_container_port=healthcheck_container_port)
          vector_db_container = vector_db_container.with_volume_mapping(
              cfg, "/milvus/configs/user.yaml")
          vector_db_container.start()
          host = vector_db_container.get_container_host_ip()
          port = vector_db_container.get_exposed_port(service_container_port)
          info = VectorDBContainerInfo(vector_db_container, host, port)
          _LOGGER.info(
              "milvus db container started successfully on %s.", info.uri)
        except Exception as e:
          stdout_logs, stderr_logs = vector_db_container.get_logs()
          stdout_logs = stdout_logs.decode("utf-8")
          stderr_logs = stderr_logs.decode("utf-8")
          _LOGGER.warning(
              "Retry %d/%d: Failed to start Milvus DB container. Reason: %s. "
              "STDOUT logs:\n%s\nSTDERR logs:\n%s",
              i + 1,
              vector_client_max_retries,
              e,
              stdout_logs,
              stderr_logs)
          if i == vector_client_max_retries - 1:
            _LOGGER.error(
                "Unable to start milvus db container for I/O tests after %d "
                "retries. Tests cannot proceed. STDOUT logs:\n%s\n"
                "STDERR logs:\n%s",
                vector_client_max_retries,
                stdout_logs,
                stderr_logs)
            raise e
        finally:
          testcontainers_config.max_tries = original_tc_max_tries
      return info

  @staticmethod
  def stop_db_container(db_info: VectorDBContainerInfo):
    if db_info is None:
      _LOGGER.warning("Milvus db info is None. Skipping stop operation.")
      return
    _LOGGER.debug("Stopping milvus db container.")
    db_info.container.stop()
    _LOGGER.info("milvus db container stopped successfully.")

  @staticmethod
  def initialize_db_with_data(
      connc_params: MilvusConnectionParameters, config: dict):
    # Open the connection to the milvus db with retry.
    def create_client():
      return MilvusClient(**connc_params.__dict__)

    client = retry_with_backoff(
        create_client,
        max_retries=3,
        retry_delay=1.0,
        operation_name="Test Milvus client connection",
        exception_types=(MilvusException, ))

    # Configure schema.
    field_schemas: List[FieldSchema] = cast(List[FieldSchema], config["fields"])
    schema = CollectionSchema(
        fields=field_schemas, functions=config["functions"])

    # Create collection with the schema.
    collection_name = config["collection_name"]
    index_function: Callable[[], IndexParams] = cast(
        Callable[[], IndexParams], config["index"])
    client.create_collection(
        collection_name=collection_name,
        schema=schema,
        index_params=index_function())

    # Assert that collection was created.
    collection_error = f"Expected collection '{collection_name}' to be created."
    assert client.has_collection(collection_name), collection_error

    # Gather all fields we have excluding 'sparse_embedding_bm25' special field.
    fields = list(map(lambda field: field.name, field_schemas))

    # Prep data for indexing. Currently we can't insert sparse vectors for BM25
    # sparse embedding field as it would be automatically generated by Milvus
    # through the registered BM25 function.
    data_ready_to_index = []
    for doc in config["corpus"]:
      item = {}
      for field in fields:
        if field.startswith("dense_embedding"):
          item[field] = doc["dense_embedding"]
        elif field == "sparse_embedding_inner_product":
          item[field] = doc["sparse_embedding"]
        elif field == "sparse_embedding_bm25":
          # It is automatically generated by Milvus from the content field.
          continue
        else:
          item[field] = doc[field]
      data_ready_to_index.append(item)

    # Index data.
    result = client.insert(
        collection_name=collection_name, data=data_ready_to_index)

    # Assert that the intended data has been properly indexed.
    insertion_err = f'failed to insert the {result["insert_count"]} data points'
    assert result["insert_count"] == len(data_ready_to_index), insertion_err

    # Release the collection from memory. It will be loaded lazily when the
    # enrichment handler is invoked.
    client.release_collection(collection_name)

    # Close the connection to the Milvus database, as no further preparation
    # operations are needed  before executing the enrichment handler.
    client.close()

    return collection_name

  @staticmethod
  @contextlib.contextmanager
  def create_user_yaml(service_port: int, max_vector_field_num=5):
    """Creates a temporary user.yaml file for Milvus configuration.

      This user yaml file overrides Milvus default configurations. It sets
      the Milvus service port to the specified container service port. The
      default for maxVectorFieldNum is 4, but we need 5
      (one unique field for each metric).

      Args:
        service_port: Port number for the Milvus service.
        max_vector_field_num: Max number of vec fields allowed per collection.

      Yields:
          str: Path to the created temporary yaml file.
      """
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml',
                                     delete=False) as temp_file:
      # Define the content for user.yaml.
      user_config = {
          'proxy': {
              'maxVectorFieldNum': max_vector_field_num, 'port': service_port
          },
          'etcd': {
              'use': {
                  'embed': True
              }, 'data': {
                  'dir': '/var/lib/milvus/etcd'
              }
          }
      }

      # Write the content to the file.
      yaml.dump(user_config, temp_file, default_flow_style=False)
      path = temp_file.name

    try:
      yield path
    finally:
      if os.path.exists(path):
        os.remove(path)

  @staticmethod
  def assert_chunks_equivalent(
      actual_chunks: List[Chunk], expected_chunks: List[Chunk]):
    """assert_chunks_equivalent checks for presence rather than exact match"""
    # Sort both lists by ID to ensure consistent ordering.
    actual_sorted = sorted(actual_chunks, key=lambda c: c.id)
    expected_sorted = sorted(expected_chunks, key=lambda c: c.id)

    actual_len = len(actual_sorted)
    expected_len = len(expected_sorted)
    err_msg = (
        f"Different number of chunks, actual: {actual_len}, "
        f"expected: {expected_len}")
    assert actual_len == expected_len, err_msg

    for actual, expected in zip(actual_sorted, expected_sorted):
      # Assert that IDs match.
      assert actual.id == expected.id

      # Assert that dense embeddings match.
      err_msg = f"Dense embedding mismatch for chunk {actual.id}"
      assert actual.dense_embedding == expected.dense_embedding, err_msg

      # Assert that sparse embeddings match.
      err_msg = f"Sparse embedding mismatch for chunk {actual.id}"
      assert actual.sparse_embedding == expected.sparse_embedding, err_msg

      # Assert that text content match.
      err_msg = f"Text Content mismatch for chunk {actual.id}"
      assert actual.content.text == expected.content.text, err_msg

      # For enrichment_data, be more flexible.
      # If "expected" has values for enrichment_data but actual doesn't, that's
      # acceptable since vector search results can vary based on many factors
      # including implementation details, vector database state, and slight
      # variations in similarity calculations.

      # First ensure the enrichment data key exists.
      err_msg = f"Missing enrichment_data key in chunk {actual.id}"
      assert 'enrichment_data' in actual.metadata, err_msg

      # For enrichment_data, ensure consistent ordering of results.
      actual_data = actual.metadata['enrichment_data']
      expected_data = expected.metadata['enrichment_data']

      # If actual has enrichment data, then perform detailed validation.
      if actual_data:
        # Ensure the id key exist.
        err_msg = f"Missing id key in metadata {actual.id}"
        assert 'id' in actual_data, err_msg

        # Validate IDs have consistent ordering.
        actual_ids = sorted(actual_data['id'])
        expected_ids = sorted(expected_data['id'])
        err_msg = f"IDs in enrichment_data don't match for chunk {actual.id}"
        assert actual_ids == expected_ids, err_msg

        # Ensure the distance key exist.
        err_msg = f"Missing distance key in metadata {actual.id}"
        assert 'distance' in actual_data, err_msg

        # Validate distances exist and have same length as IDs.
        actual_distances = actual_data['distance']
        expected_distances = expected_data['distance']
        err_msg = (
            "Number of distances doesn't match number of IDs for "
            f"chunk {actual.id}")
        assert len(actual_distances) == len(expected_distances), err_msg

        # Ensure the fields key exist.
        err_msg = f"Missing fields key in metadata {actual.id}"
        assert 'fields' in actual_data, err_msg

        # Validate fields have consistent content.
        # Sort fields by 'id' to ensure consistent ordering.
        actual_fields_sorted = sorted(
            actual_data['fields'], key=lambda f: f.get('id', 0))
        expected_fields_sorted = sorted(
            expected_data['fields'], key=lambda f: f.get('id', 0))

        # Compare field IDs.
        actual_field_ids = [f.get('id') for f in actual_fields_sorted]
        expected_field_ids = [f.get('id') for f in expected_fields_sorted]
        err_msg = f"Field IDs don't match for chunk {actual.id}"
        assert actual_field_ids == expected_field_ids, err_msg

        # Compare field content.
        for a_f, e_f in zip(actual_fields_sorted, expected_fields_sorted):
          # Ensure the id key exist.
          err_msg = f"Missing id key in metadata.fields {actual.id}"
          assert 'id' in a_f, err_msg

          err_msg = f"Field ID mismatch chunk {actual.id}"
          assert a_f['id'] == e_f['id'], err_msg

          # Validate field metadata.
          err_msg = f"Field Metadata doesn't match for chunk {actual.id}"
          assert a_f['metadata'] == e_f['metadata'], err_msg


if __name__ == '__main__':
  unittest.main()
