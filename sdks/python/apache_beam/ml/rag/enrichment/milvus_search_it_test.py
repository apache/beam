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


@pytest.mark.uses_testcontainer
class TestMilvusSearchEnrichment(unittest.TestCase):
  """Tests for search functionality across all search strategies"""

  _db: MilvusSearchDBContainerInfo
  _milvus_db_version = "milvusdb/milvus:v2.5.10"

  @classmethod
  def setUpClass(cls):
    cls._db = MilvusEnrichmentTestHelper.start_milvus_search_db_container(
        image=cls._milvus_db_version)

  @classmethod
  def tearDownClass(cls):
    MilvusEnrichmentTestHelper.stop_milvus_search_db_container(cls._db)
    cls._db = None

  def test_invalid_query(self):
    pass

  def test_empty_input_chunks(self):
    pass

  def test_filtered_search(self):
    pass

  def test_chunks_batching(self):
    pass

  def test_vector_search_COSINE(self):
    pass

  def test_vector_search_EUCLIDEAN_DISTANCE(self):
    pass

  def test_vector_search_INNER_PRODUCT(self):
    pass

  def test_keyword_search_BM25(self):
    pass

  def test_hybrid_search(self):
    pass
