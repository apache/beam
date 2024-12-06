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

class VectorDatabaseConfig(ABC):
    """Abstract base class for vector database configurations.
    
    Implementations should provide database-specific configuration and
    create appropriate write transforms.
    """
    @abstractmethod
    def create_write_transform(self) -> beam.PTransform:
        """Creates a PTransform that writes to the vector database.
        
        Returns:
            A PTransform that writes embeddings to the configured database.
        """
        pass

class VectorDatabaseWriteTransform(beam.PTransform):
    """Generic transform for writing to vector databases.
    
    Uses the provided database config to create an appropriate write transform.
    """
    def __init__(self, database_config: VectorDatabaseConfig):
        """Initialize transform with database config.
        
        Args:
            database_config: Configuration for target vector database.
        """
        if not isinstance(database_config, VectorDatabaseConfig):
            raise TypeError(
                f"database_config must be VectorDatabaseConfig, "
                f"got {type(database_config)}"
            )
        self.database_config = database_config

    def expand(self, pcoll):
        """Create and apply database-specific write transform.
        
        Args:
            pcoll: PCollection of embeddings to write.
            
        Returns:
            Result of writing to database.
        """
        write_transform = self.database_config.create_write_transform()
        return pcoll | write_transform