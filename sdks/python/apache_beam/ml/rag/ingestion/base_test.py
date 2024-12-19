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
import apache_beam as beam
from apache_beam.ml.rag.types import Chunk, Embedding, Content
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from apache_beam.ml.rag.ingestion.base import (
    VectorDatabaseConfig, VectorDatabaseWriteTransform)


class MockWriteTransform(beam.PTransform):
  """Mock transform that returns element."""
  def expand(self, pcoll):
    return pcoll | beam.Map(lambda x: x)


class MockDatabaseConfig(VectorDatabaseConfig):
  """Mock database config for testing."""
  def __init__(self):
    self.write_transform = MockWriteTransform()

  def create_write_transform(self) -> beam.PTransform:
    return self.write_transform


class VectorDatabaseBaseTest(unittest.TestCase):
  def test_write_transform_creation(self):
    """Test that write transform is created correctly."""
    config = MockDatabaseConfig()
    transform = VectorDatabaseWriteTransform(config)
    self.assertEqual(transform.database_config, config)

  def test_pipeline_integration(self):
    """Test writing through pipeline."""
    test_data = [
        Chunk(
            content=Content(text="foo"),
            id="1",
            embedding=Embedding(dense_embedding=[0.1, 0.2])),
        Chunk(
            content=Content(text="bar"),
            id="2",
            embedding=Embedding(dense_embedding=[0.3, 0.4]))
    ]

    with TestPipeline() as p:
      result = (
          p
          | beam.Create(test_data)
          | VectorDatabaseWriteTransform(MockDatabaseConfig()))

      # Verify data was written
      assert_that(result, equal_to(test_data))

  def test_invalid_config(self):
    """Test error handling for invalid config."""
    with self.assertRaises(TypeError):
      VectorDatabaseWriteTransform(None)


if __name__ == '__main__':
  unittest.main()
