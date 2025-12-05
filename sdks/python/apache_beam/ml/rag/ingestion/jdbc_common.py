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

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


@dataclass
class ConnectionConfig:
  """Configuration for connecting to a JDBC database.
    
    Provides connection details and options for connecting to a database
    instance.
    
    Attributes:
        jdbc_url: JDBC URL for the database instance.
            Example: 'jdbc:postgresql://host:port/database'
        username: Database username.
        password: Database password.
        connection_properties: Optional JDBC connection properties dict.
            Example: {'ssl': 'true'}
        connection_init_sqls: Optional list of SQL statements to execute when
            connection is established.
        additional_jdbc_args: Additional arguments that will be passed to
            WriteToJdbc. These may include 'driver_jars', 'expansion_service',
            'classpath', etc. See full set of args at
            :class:`~apache_beam.io.jdbc.WriteToJdbc`
    
    Example:
        >>> config = AlloyDBConnectionConfig(
        ...     jdbc_url='jdbc:postgresql://localhost:5432/mydb',
        ...     username='user',
        ...     password='pass',
        ...     connection_properties={'ssl': 'true'},
        ...     max_connections=10
        ... )
    """
  jdbc_url: str
  username: str
  password: str
  connection_properties: Optional[Dict[str, str]] = None
  connection_init_sqls: Optional[List[str]] = None
  additional_jdbc_args: Dict[str, Any] = field(default_factory=dict)


@dataclass
class WriteConfig:
  """Configuration writing to JDBC database.
    
    Modifies the write behavior when writing via JdbcIO.
    
    Attributes:
        autosharding: Enable automatic re-sharding of bundles to scale the
            number of shards with workers.
        max_connections: Optional number of connections in the pool.
            Use negative for no limit.
        write_batch_size: Optional write batch size for bulk operations.
  """
  autosharding: Optional[bool] = None
  max_connections: Optional[int] = None
  write_batch_size: Optional[int] = None
