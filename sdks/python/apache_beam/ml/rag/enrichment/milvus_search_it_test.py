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

import logging
from dataclasses import dataclass
from testcontainers.core.generic import DbContainer
from testcontainers.milvus import MilvusContainer
from typing import Optional

import unittest
import pytest

from pymilvus.milvus_client import IndexParams

_LOGGER = logging.getLogger(__name__)


@dataclass
class MilvusSearchDBContainerInfo:
  container: DbContainer
  host: str
  port: int
  user: Optional[str] = ""
  password: Optional[str] = ""
  id: Optional[str] = "default"

  @property
  def address(self) -> str:
    return f"http://{self.host}:{self.port}"


class MilvusEnrichmentTestHelper:
  @staticmethod
  def start_milvus_search_db_container(
      image="milvusdb/milvus:v2.5.10",
      vector_client_retries=3) -> MilvusSearchDBContainerInfo:
    info = None
    for i in range(vector_client_retries):
      try:
        vector_db_container = MilvusContainer(image=image, port=19530)
        vector_db_container.start()
        host = vector_db_container.get_container_host_ip()
        port = vector_db_container.get_exposed_port(19530)

        info = MilvusSearchDBContainerInfo(
            container=vector_db_container, host=host, port=port)
        _LOGGER.info(
            "milvus db container started successfully on %s.", info.address)
        break
      except Exception as e:
        _LOGGER.warning(
            "Retry %d/%d: Failed to start milvus db container. Reason: %s",
            i + 1,
            vector_client_retries,
            e)
        if i == vector_client_retries - 1:
          _LOGGER.error(
              "Unable to start milvus db container for I/O tests after %d "
              "retries. Tests cannot proceed.",
              vector_client_retries)
          raise e
    return info

  @staticmethod
  def stop_milvus_search_db_container(db_info: MilvusSearchDBContainerInfo):
    try:
      _LOGGER.debug("Stopping milvus db container.")
      db_info.container.stop()
      _LOGGER.info("milvus db container stopped successfully.")
    except Exception as e:
      _LOGGER.warning(
          "Error encountered while stopping milvus db container: %s", e)


# Define a session-level fixture to manage the container.
@pytest.fixture(scope="session")
def milvus_container():
  # Start the container before any tests run.
  container = MilvusEnrichmentTestHelper.start_milvus_search_db_container()

  # Yield the container to the tests.
  yield container

  # Clean up after all tests are done.
  MilvusEnrichmentTestHelper.stop_milvus_search_db_container(container)


@pytest.mark.uses_testcontainer
class TestMilvusSearchEnrichment(unittest.TestCase):
  """Tests for general search functionality across all search strategies"""
  @pytest.fixture(autouse=True)
  def setup_milvus(self, milvus_container):
    self.db = milvus_container

  def test_filtered_search(self):
    pass

  def test_chunks_batching(self):
    pass

  def test_invalid_query(self):
    pass

  def test_empty_input_chunks(self):
    pass


# Use the fixture in your test classes.
@pytest.mark.uses_testcontainer
class TestMilvusVectorSearchEnrichment(unittest.TestCase):
  """Tests specific to vector search functionality"""
  @pytest.fixture(autouse=True)
  def setup_milvus(self, milvus_container):
    self.db = milvus_container

  def test_vector_search_COSINE(self):
    pass

  def test_vector_search_L2(self):
    pass

  def test_vector_search_IP(self):
    pass

  def test_missing_dense_embedding(self):
    pass


@pytest.mark.uses_testcontainer
class TestMilvusKeywordSearchEnrichment(unittest.TestCase):
  """Tests specific to keyword search functionality"""
  @pytest.fixture(autouse=True)
  def setup_milvus(self, milvus_container):
    self.db = milvus_container

  def test_keyword_search_BM25(self):
    pass

  def test_missing_content_and_sparse_embedding(self):
    pass

  def test_missing_content_only(self):
    pass

  def test_missing_sparse_embedding_only(self):
    pass


@pytest.mark.uses_testcontainer
class TestMilvusHybridSearchEnrichment(unittest.TestCase):
  """Tests specific to hybrid search functionality"""
  @pytest.fixture(autouse=True)
  def setup_milvus(self, milvus_container):
    self.db = milvus_container

  def test_hybrid_search(self):
    pass

  def test_missing_dense_embedding_for_vector_search(self):
    pass

  def test_missing_content_and_sparse_embedding_for_keyword_search(self):
    pass

  def test_missing_content_and_sparse_embedding_for_keyword_search(self):
    pass

  def test_missing_content_only_for_keyword_search(self):
    pass

  def test_missing_sparse_embedding_only_for_keyword_search(self):
    pass
