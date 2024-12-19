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

from abc import ABC, abstractmethod

import apache_beam as beam
from apache_beam.ml.rag.types import Chunk


class VectorDatabaseWriteConfig(ABC):
  """Abstract base class for vector database configurations in RAG pipelines.
  
  VectorDatabaseWriteConfig defines the interface for configuring vector
  database writes in RAG pipelines. Implementations should provide
  database-specific configuration and create appropriate write transforms.

  The configuration flow:
  1. Subclass provides database-specific configuration (table names, etc)
  2. create_write_transform() creates appropriate PTransform for writing
  3. Transform handles converting Chunks to database-specific format

  Example implementation:
    ```python
    class BigQueryVectorWriterConfig(VectorDatabaseWriteConfig):
        def __init__(self, table: str):
            self.embedding_column = embedding_column
            
        def create_write_transform(self):
            return beam.io.WriteToBigQuery(
                table=self.table
            )
    ```
  """
  @abstractmethod
  def create_write_transform(self) -> beam.PTransform:
    """Creates a PTransform that writes embeddings to the vector database.
    
    Returns:
        A PTransform that accepts PCollection[Chunk] and writes the chunks'
        embeddings and metadata to the configured vector database.
        The transform should handle:
        - Converting Chunk format to database schema
        - Setting up database connection/client
        - Writing with appropriate batching/error handling
    """
    pass


class VectorDatabaseWriteTransform(beam.PTransform):
  """A PTransform for writing embedded chunks to vector databases.
  
  This transform uses a VectorDatabaseWriteConfig to write chunks with
  embeddings to vector database. It handles validating the config and applying
  the database-specific write transform.

  Example usage:
    ```python
    config = BigQueryVectorConfig(
        table='project.dataset.embeddings',
        embedding_column='embedding'
    )

    with beam.Pipeline() as p:
        chunks = p | beam.Create([...])  # PCollection[Chunk]
        chunks | VectorDatabaseWriteTransform(config)
    ```

  Args:
      database_config: Configuration for the target vector database.
          Must be a subclass of VectorDatabaseWriteConfig that implements
          create_write_transform().
  
  Raises:
      TypeError: If database_config is not a VectorDatabaseWriteConfig instance.
  """
  def __init__(self, database_config: VectorDatabaseWriteConfig):
    """Initialize transform with database config.
        
        Args:
            database_config: Configuration for target vector database.
        """
    if not isinstance(database_config, VectorDatabaseWriteConfig):
      raise TypeError(
          f"database_config must be VectorDatabaseWriteConfig, "
          f"got {type(database_config)}")
    self.database_config = database_config

  def expand(self, pcoll: beam.PCollection[Chunk]):
    """Creates and applies the database-specific write transform.
    
    Args:
        pcoll: PCollection of Chunks with embeddings to write to the
            vector database. Each Chunk must have:
            - An embedding
            - An ID
            - Metadata used to filter results as specified by database config
            
    Returns:
        Result of writing to database (implementation specific).
    """
    write_transform = self.database_config.create_write_transform()
    return pcoll | write_transform
