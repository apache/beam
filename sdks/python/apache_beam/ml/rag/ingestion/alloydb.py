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
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from apache_beam.ml.rag.ingestion.jdbc_common import ConnectionConfig
from apache_beam.ml.rag.ingestion.jdbc_common import WriteConfig
from apache_beam.ml.rag.ingestion.postgres import ColumnSpecsBuilder
from apache_beam.ml.rag.ingestion.postgres import PostgresVectorWriterConfig
from apache_beam.ml.rag.ingestion.postgres_common import ColumnSpec
from apache_beam.ml.rag.ingestion.postgres_common import ConflictResolution


@dataclass
class AlloyDBLanguageConnectorConfig:
  """Configuration options for AlloyDB language connector.
    
    Contains all parameters needed to configure a connection using the AlloyDB
    Java connector via JDBC. For details see
    https://github.com/GoogleCloudPlatform/alloydb-java-connector/blob/main/docs/jdbc.md
    
    Attributes:
        username: Database username.
        password: Database password. Can be empty string when using IAM.
        database_name: Name of the database to connect to.
        instance_name: Fullly qualified instance. Format: 
            'projects/<PROJECT>/locations/<REGION>/clusters/<CLUSTER>/instances
            /<INSTANCE>'
        ip_type: IP type to use for connection. Either 'PRIVATE' (default), 
            'PUBLIC' 'PSC.
        enable_iam_auth: Whether to enable IAM authentication. Default is False
        target_principal: Optional service account to impersonate for
            connection.
        delegates: Optional comma-separated list of service accounts for
            delegated impersonation.
        admin_service_endpoint: Optional custom API service endpoint.
        quota_project: Optional project ID for quota and billing.
        connection_properties: Optional JDBC connection properties dict.
            Example: {'ssl': 'true'}
        additional_properties: Additional properties to be added to the JDBC
            url. Example: {'someProperty': 'true'}
    """
  username: str
  password: str
  database_name: str
  instance_name: str
  ip_type: str = "PRIVATE"
  enable_iam_auth: bool = False
  target_principal: Optional[str] = None
  delegates: Optional[List[str]] = None
  admin_service_endpoint: Optional[str] = None
  quota_project: Optional[str] = None
  connection_properties: Optional[Dict[str, str]] = None
  additional_properties: Optional[Dict[str, Any]] = None

  def to_jdbc_url(self) -> str:
    """Convert options to a properly formatted JDBC URL.
      
      Returns:
          JDBC URL string configured with all options.
      """
    # Base URL with database name
    url = f"jdbc:postgresql:///{self.database_name}?"

    # Add required properties
    properties = {
        "socketFactory": "com.google.cloud.alloydb.SocketFactory",
        "alloydbInstanceName": self.instance_name,
        "alloydbIpType": self.ip_type
    }

    if self.enable_iam_auth:
      properties["alloydbEnableIAMAuth"] = "true"

    if self.target_principal:
      properties["alloydbTargetPrincipal"] = self.target_principal

    if self.delegates:
      properties["alloydbDelegates"] = ",".join(self.delegates)

    if self.admin_service_endpoint:
      properties["alloydbAdminServiceEndpoint"] = self.admin_service_endpoint

    if self.quota_project:
      properties["alloydbQuotaProject"] = self.quota_project

    if self.additional_properties:
      properties.update(self.additional_properties)

    property_string = "&".join(f"{k}={v}" for k, v in properties.items())
    return url + property_string

  def to_connection_config(self):
    return ConnectionConfig(
        jdbc_url=self.to_jdbc_url(),
        username=self.username,
        password=self.password,
        connection_properties=self.connection_properties,
        additional_jdbc_args=self.additional_jdbc_args())

  def additional_jdbc_args(self) -> Dict[str, List[Any]]:
    return {
        'classpath': [
            "org.postgresql:postgresql:42.2.16",
            "com.google.cloud:alloydb-jdbc-connector:1.2.0"
        ]
    }


class AlloyDBVectorWriterConfig(PostgresVectorWriterConfig):
  def __init__(
      self,
      connection_config: AlloyDBLanguageConnectorConfig,
      table_name: str,
      *,
      # pylint: disable=dangerous-default-value
      write_config: WriteConfig = WriteConfig(),
      column_specs: List[ColumnSpec] = ColumnSpecsBuilder.with_defaults().build(
      ),
      conflict_resolution: Optional[ConflictResolution] = ConflictResolution(
          on_conflict_fields=[], action='IGNORE')):
    """Configuration for writing vectors to AlloyDB.
    
    Supports flexible schema configuration through column specifications and
    conflict resolution strategies.

    Args:
        connection_config: AlloyDB connection configuration.
        table_name: Target table name.
        write_config: JdbcIO :class:`~.jdbc_common.WriteConfig` to control
          batch sizes, authosharding, etc.
        column_specs:
            Use :class:`~.postgres_common.ColumnSpecsBuilder` to configure how
            embeddings and metadata are written a database
            schema. If None, uses default Chunk schema.
        conflict_resolution: Optional
            :class:`~.postgres_common.ConflictResolution`
            strategy for handling insert conflicts. ON CONFLICT DO NOTHING by
            default.
    
    Examples:
        Basic usage with default schema:

        >>> config = AlloyDBVectorWriterConfig(
        ...     connection_config=AlloyDBConnectionConfig(...),
        ...     table_name='embeddings'
        ... )

        Simple case with default schema:

        >>> config = PostgresVectorWriterConfig(
        ...     connection_config=ConnectionConfig(...),
        ...     table_name='embeddings'
        ... )

        Custom schema with metadata fields:

        >>> specs = (ColumnSpecsBuilder()
        ...         .with_id_spec(column_name="my_id_column")
        ...         .with_embedding_spec(column_name="embedding_vec")
        ...         .add_metadata_field(field="source", column_name="src")
        ...         .add_metadata_field(
        ...             "timestamp",
        ...             column_name="created_at",
        ...             sql_typecast="::timestamp"
        ...         )
        ...         .build())

        Minimal schema (only ID + embedding written)

        >>> column_specs = (ColumnSpecsBuilder()
        ...     .with_id_spec()
        ...     .with_embedding_spec()
        ...     .build())

        >>> config = AlloyDBVectorWriterConfig(
        ...     connection_config=AlloyDBConnectionConfig(...),
        ...     table_name='embeddings',
        ...     column_specs=specs
        ... )
    """
    super().__init__(
        connection_config=connection_config.to_connection_config(),
        write_config=write_config,
        table_name=table_name,
        column_specs=column_specs,
        conflict_resolution=conflict_resolution)
