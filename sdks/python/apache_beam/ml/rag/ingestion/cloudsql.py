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

from dataclasses import asdict
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from apache_beam.ml.rag.ingestion import mysql
from apache_beam.ml.rag.ingestion import mysql_common
from apache_beam.ml.rag.ingestion import postgres
from apache_beam.ml.rag.ingestion import postgres_common
from apache_beam.ml.rag.ingestion.jdbc_common import ConnectionConfig
from apache_beam.ml.rag.ingestion.jdbc_common import WriteConfig


@dataclass
class LanguageConnectorConfig:
  """Configuration options for CloudSQL Java language connector.
    
    Set parameters to connect connection to a CloudSQL instance using
    Java language connector connector. For details see
    https://github.com/GoogleCloudPlatform/cloud-sql-jdbc-socket-factory/blob/main/docs/jdbc.md
    
    Attributes:
        username: Database username.
        password: Database password. Can be empty string when using IAM.
        database_name: Name of the database to connect to.
        instance_name: Instance connection name. Format: 
            '<PROJECT>:<REGION>:<INSTANCE>'
        ip_type: Preferred order of IP types used to connect via a comma
            list of strings.
        enable_iam_auth: Whether to enable IAM authentication. Default is False
        target_principal: Optional service account to impersonate for
            connection.
        delegates: Optional list of service accounts for delegated
            impersonation.
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
  ip_types: Optional[List[str]] = None
  enable_iam_auth: bool = False
  target_principal: Optional[str] = None
  delegates: Optional[List[str]] = None
  quota_project: Optional[str] = None
  connection_properties: Optional[Dict[str, str]] = None
  additional_properties: Optional[Dict[str, Any]] = None

  def _base_jdbc_properties(self) -> Dict[str, Any]:
    properties = {"cloudSqlInstance": self.instance_name}

    if self.ip_types:
      properties["ipTypes"] = ",".join(self.ip_types)

    if self.enable_iam_auth:
      properties["enableIamAuth"] = "true"

    if self.target_principal:
      properties["cloudSqlTargetPrincipal"] = self.target_principal

    if self.delegates:
      properties["cloudSqlDelegates"] = ",".join(self.delegates)

    if self.quota_project:
      properties["cloudSqlAdminQuotaProject"] = self.quota_project

    if self.additional_properties:
      properties.update(self.additional_properties)

    return properties

  def _build_jdbc_url(self, socketFactory, database_type):
    url = f"jdbc:{database_type}:///{self.database_name}?"

    properties = self._base_jdbc_properties()
    properties['socketFactory'] = socketFactory

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
    return {}


@dataclass
class _PostgresConnectorConfig(LanguageConnectorConfig):
  def to_jdbc_url(self) -> str:
    """Convert options to a properly formatted JDBC URL.
      
      Returns:
          JDBC URL string configured with all options.
      """
    return self._build_jdbc_url(
        socketFactory="com.google.cloud.sql.postgres.SocketFactory",
        database_type="postgresql")

  def additional_jdbc_args(self) -> Dict[str, List[Any]]:
    return {
        'classpath': [
            "org.postgresql:postgresql:42.2.16",
            "com.google.cloud.sql:postgres-socket-factory:1.25.0"
        ]
    }

  @classmethod
  def from_base_config(cls, config: LanguageConnectorConfig):
    return cls(**asdict(config))


class CloudSQLPostgresVectorWriterConfig(postgres.PostgresVectorWriterConfig):
  def __init__(
      self,
      connection_config: LanguageConnectorConfig,
      table_name: str,
      *,
      # pylint: disable=dangerous-default-value
      write_config: WriteConfig = WriteConfig(),
      column_specs: List[postgres_common.ColumnSpec] = postgres_common.
      ColumnSpecsBuilder.with_defaults().build(),
      conflict_resolution: Optional[
          postgres_common.ConflictResolution] = postgres_common.
      ConflictResolution(on_conflict_fields=[], action='IGNORE')):
    """Configuration for writing vectors to ClouSQL Postgres.
    
    Supports flexible schema configuration through column specifications and
    conflict resolution strategies.

    Args:
        connection_config: :class:`LanguageConnectorConfig`.
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

        >>> config = PostgresVectorWriterConfig(
        ...     connection_config=PostgresConnectionConfig(...),
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

        >>> config = CloudSQLPostgresVectorWriterConfig(
        ...     connection_config=PostgresConnectionConfig(...),
        ...     table_name='embeddings',
        ...     column_specs=specs
        ... )
    """
    self.connector_config = _PostgresConnectorConfig.from_base_config(
        connection_config)
    super().__init__(
        connection_config=self.connector_config.to_connection_config(),
        write_config=write_config,
        table_name=table_name,
        column_specs=column_specs,
        conflict_resolution=conflict_resolution)


@dataclass
class _MySQLConnectorConfig(LanguageConnectorConfig):
  def to_jdbc_url(self) -> str:
    """Convert options to a properly formatted MySQL JDBC URL."""
    return self._build_jdbc_url(
        socketFactory="com.google.cloud.sql.mysql.SocketFactory",
        database_type="mysql")

  def additional_jdbc_args(self) -> Dict[str, List[Any]]:
    return {
        'classpath': [
            "mysql:mysql-connector-java:8.0.22",
            "com.google.cloud.sql:mysql-socket-factory-connector-j-8:1.25.0"
        ]
    }

  @classmethod
  def from_base_config(cls, config: LanguageConnectorConfig):
    return cls(**asdict(config))


class CloudSQLMySQLVectorWriterConfig(mysql.MySQLVectorWriterConfig):
  def __init__(
      self,
      connection_config: LanguageConnectorConfig,
      table_name: str,
      *,
      write_config: WriteConfig = WriteConfig(),
      # pylint: disable=dangerous-default-value
      column_specs: List[mysql_common.ColumnSpec] = mysql_common.
      ColumnSpecsBuilder.with_defaults().build(),
      conflict_resolution: Optional[mysql_common.ConflictResolution] = None):
    self.connector_config = _MySQLConnectorConfig.from_base_config(
        connection_config)
    super().__init__(
        connection_config=self.connector_config.to_connection_config(),
        write_config=write_config,
        table_name=table_name,
        column_specs=column_specs,
        conflict_resolution=conflict_resolution)
