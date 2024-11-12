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

"""PTransforms for supporting Jdbc in Python pipelines.

  These transforms are currently supported by Beam portable
  Flink, Spark, and Dataflow v2 runners.

  **Setup**

  Transforms provided in this module are cross-language transforms
  implemented in the Beam Java SDK. During the pipeline construction, Python SDK
  will connect to a Java expansion service to expand these transforms.
  To facilitate this, a small amount of setup is needed before using these
  transforms in a Beam Python pipeline.

  There are several ways to setup cross-language Jdbc transforms.

  * Option 1: use the default expansion service
  * Option 2: specify a custom expansion service

  See below for details regarding each of these options.

  *Option 1: Use the default expansion service*

  This is the recommended and easiest setup option for using Python Jdbc
  transforms. This option is only available for Beam 2.24.0 and later.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Install Java runtime in the computer from where the pipeline is constructed
    and make sure that 'java' command is available.

  In this option, Python SDK will either download (for released Beam version) or
  build (when running from a Beam Git clone) a expansion service jar and use
  that to expand transforms. Currently Jdbc transforms use the
  'beam-sdks-java-io-expansion-service' jar for this purpose.

  The transforms in this file support an extra `classpath` argument, where one
  can specify any extra JARs to be included in the classpath for the expansion
  service. Users will need to specify this option to include the JDBC driver
  for the database that you're trying to use. **By default, a Postgres JDBC
  driver** is included (i.e. the Java package
  `"org.postgresql:postgresql:42.2.16"`).

  *Option 2: specify a custom expansion service*

  In this option, you startup your own expansion service and provide that as
  a parameter when using the transforms provided in this module.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Startup your own expansion service.
  * Update your pipeline to provide the expansion service address when
    initiating Jdbc transforms provided in this module.

  Flink Users can use the built-in Expansion Service of the Flink Runner's
  Job Server. If you start Flink's Job Server, the expansion service will be
  started on port 8097. For a different address, please set the
  expansion_service parameter.

  **More information**

  For more information regarding cross-language transforms see:
  - https://beam.apache.org/roadmap/portability/

  For more information specific to Flink runner see:
  - https://beam.apache.org/documentation/runners/flink/
"""

# pytype: skip-file

import datetime
import typing

import numpy as np

from apache_beam.coders import RowCoder
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder
from apache_beam.typehints.schemas import LogicalType
from apache_beam.typehints.schemas import MillisInstant
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.utils.timestamp import Timestamp

__all__ = [
    'WriteToJdbc',
    'ReadFromJdbc',
]


def default_io_expansion_service(classpath=None):
  return BeamJarExpansionService(
      ':sdks:java:extensions:schemaio-expansion-service:shadowJar',
      classpath=classpath)


JdbcConfigSchema = typing.NamedTuple(
    'JdbcConfigSchema',
    [('location', str), ('config', bytes)],
)

Config = typing.NamedTuple(
    'Config',
    [('driver_class_name', str), ('jdbc_url', str), ('username', str),
     ('password', str), ('connection_properties', typing.Optional[str]),
     ('connection_init_sqls', typing.Optional[typing.List[str]]),
     ('read_query', typing.Optional[str]),
     ('write_statement', typing.Optional[str]),
     ('fetch_size', typing.Optional[np.int16]),
     ('disable_autocommit', typing.Optional[bool]),
     ('output_parallelization', typing.Optional[bool]),
     ('autosharding', typing.Optional[bool]),
     ('partition_column', typing.Optional[str]),
     ('partitions', typing.Optional[np.int16]),
     ('max_connections', typing.Optional[np.int16]),
     ('driver_jars', typing.Optional[str])],
)

DEFAULT_JDBC_CLASSPATH = ['org.postgresql:postgresql:42.2.16']


class WriteToJdbc(ExternalTransform):
  """A PTransform which writes Rows to the specified database via JDBC.

  This transform receives Rows defined as NamedTuple type and registered in
  the coders registry, e.g.::

    ExampleRow = typing.NamedTuple('ExampleRow',
                                   [('id', int), ('name', unicode)])
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with TestPipeline() as p:
      _ = (
          p
          | beam.Create([ExampleRow(1, 'abc')])
              .with_output_types(ExampleRow)
          | 'Write to jdbc' >> WriteToJdbc(
              table_name='jdbc_external_test_write'
              driver_class_name='org.postgresql.Driver',
              jdbc_url='jdbc:postgresql://localhost:5432/example',
              username='postgres',
              password='postgres',
          ))

  table_name is a required paramater, and by default, the write_statement is
  generated from it.

  The generated write_statement can be overridden by passing in a
  write_statment.


  Experimental; no backwards compatibility guarantees.
  """

  URN = 'beam:transform:org.apache.beam:schemaio_jdbc_write:v1'

  def __init__(
      self,
      table_name,
      driver_class_name,
      jdbc_url,
      username,
      password,
      statement=None,
      connection_properties=None,
      connection_init_sqls=None,
      autosharding=False,
      max_connections=None,
      driver_jars=None,
      expansion_service=None,
      classpath=None,
  ):
    """
    Initializes a write operation to Jdbc.

    :param driver_class_name: name of the jdbc driver class
    :param jdbc_url: full jdbc url to the database.
    :param username: database username
    :param password: database password
    :param statement: sql statement to be executed
    :param connection_properties: properties of the jdbc connection
                                  passed as string with format
                                  [propertyName=property;]*
    :param connection_init_sqls: required only for MySql and MariaDB.
                                 passed as list of strings
    :param autosharding: enable automatic re-sharding of bundles to scale the
                         number of shards with the number of workers.
    :param max_connections: sets the maximum total number of connections.
                            use a negative value for no limit.
    :param driver_jars: comma separated paths for JDBC drivers. if not
                        specified, the default classloader is used to load the
                        driver jars.
    :param expansion_service: The address (host:port) of the ExpansionService.
    :param classpath: A list of JARs or Java packages to include in the
                      classpath for the expansion service. This option is
                      usually needed for `jdbc` to include extra JDBC driver
                      packages.
                      The packages can be in these three formats: (1) A local
                      file, (2) A URL, (3) A gradle-style identifier of a Maven
                      package (e.g. "org.postgresql:postgresql:42.3.1").
                      By default, this argument includes a Postgres SQL JDBC
                      driver.
    """
    classpath = classpath or DEFAULT_JDBC_CLASSPATH
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            JdbcConfigSchema(
                location=table_name,
                config=RowCoder(
                    typing_to_runner_api(Config).row_type.schema).encode(
                        Config(
                            driver_class_name=driver_class_name,
                            jdbc_url=jdbc_url,
                            username=username,
                            password=password,
                            connection_properties=connection_properties,
                            connection_init_sqls=connection_init_sqls,
                            write_statement=statement,
                            read_query=None,
                            fetch_size=None,
                            disable_autocommit=None,
                            output_parallelization=None,
                            autosharding=autosharding,
                            max_connections=max_connections,
                            driver_jars=driver_jars,
                            partitions=None,
                            partition_column=None))),
        ),
        expansion_service or default_io_expansion_service(classpath),
    )


class ReadFromJdbc(ExternalTransform):
  """A PTransform which reads Rows from the specified database via JDBC.

  This transform delivers Rows defined as NamedTuple registered in
  the coders registry, e.g.::

    ExampleRow = typing.NamedTuple('ExampleRow',
                                   [('id', int), ('name', unicode)])
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

    with TestPipeline() as p:
      result = (
          p
          | 'Read from jdbc' >> ReadFromJdbc(
              table_name='jdbc_external_test_read'
              driver_class_name='org.postgresql.Driver',
              jdbc_url='jdbc:postgresql://localhost:5432/example',
              username='postgres',
              password='postgres',
          ))

  table_name is a required paramater, and by default, the read_query is
  generated from it.

  The generated read_query can be overridden by passing in a read_query.

  Experimental; no backwards compatibility guarantees.
  """

  URN = 'beam:transform:org.apache.beam:schemaio_jdbc_read:v1'

  def __init__(
      self,
      table_name,
      driver_class_name,
      jdbc_url,
      username,
      password,
      query=None,
      disable_autocommit=None,
      output_parallelization=None,
      fetch_size=None,
      partition_column=None,
      partitions=None,
      connection_properties=None,
      connection_init_sqls=None,
      max_connections=None,
      driver_jars=None,
      expansion_service=None,
      classpath=None,
  ):
    """
    Initializes a read operation from Jdbc.

    :param driver_class_name: name of the jdbc driver class
    :param jdbc_url: full jdbc url to the database.
    :param username: database username
    :param password: database password
    :param query: sql query to be executed
    :param disable_autocommit: disable autocommit on read
    :param output_parallelization: is output parallelization on
    :param fetch_size: how many rows to fetch
    :param partition_column: enable partitioned reads by splitting on this
                             column
    :param partitions: override the default number of splits when using
                       partition_column
    :param connection_properties: properties of the jdbc connection
                                  passed as string with format
                                  [propertyName=property;]*
    :param connection_init_sqls: required only for MySql and MariaDB.
                                 passed as list of strings
    :param max_connections: sets the maximum total number of connections.
                            use a negative value for no limit.
    :param driver_jars: comma separated paths for JDBC drivers. if not
                        specified, the default classloader is used to load the
                        driver jars.
    :param expansion_service: The address (host:port) of the ExpansionService.
    :param classpath: A list of JARs or Java packages to include in the
                      classpath for the expansion service. This option is
                      usually needed for `jdbc` to include extra JDBC driver
                      packages.
                      The packages can be in these three formats: (1) A local
                      file, (2) A URL, (3) A gradle-style identifier of a Maven
                      package (e.g. "org.postgresql:postgresql:42.3.1").
                      By default, this argument includes a Postgres SQL JDBC
                      driver.
    """
    classpath = classpath or DEFAULT_JDBC_CLASSPATH
    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            JdbcConfigSchema(
                location=table_name,
                config=RowCoder(
                    typing_to_runner_api(Config).row_type.schema).encode(
                        Config(
                            driver_class_name=driver_class_name,
                            jdbc_url=jdbc_url,
                            username=username,
                            password=password,
                            connection_properties=connection_properties,
                            connection_init_sqls=connection_init_sqls,
                            write_statement=None,
                            read_query=query,
                            fetch_size=fetch_size,
                            disable_autocommit=disable_autocommit,
                            output_parallelization=output_parallelization,
                            autosharding=None,
                            max_connections=max_connections,
                            driver_jars=driver_jars,
                            partition_column=partition_column,
                            partitions=partitions))),
        ),
        expansion_service or default_io_expansion_service(classpath),
    )


@LogicalType.register_logical_type
class JdbcDateType(LogicalType[datetime.date, MillisInstant, str]):
  """
  For internal use only; no backwards-compatibility guarantees.

  Support of Legacy JdbcIO DATE logical type. Deemed to change when Java JDBCIO
  has been migrated to Beam portable logical types.
  """
  def __init__(self, argument=""):
    pass

  @classmethod
  def representation_type(cls) -> type:
    return Timestamp

  @classmethod
  def urn(cls):
    return "beam:logical_type:javasdk_date:v1"

  @classmethod
  def language_type(cls):
    return datetime.date

  def to_representation_type(self, value: datetime.date) -> Timestamp:
    return Timestamp.from_utc_datetime(
        datetime.datetime.combine(
            value, datetime.datetime.min.time(), tzinfo=datetime.timezone.utc))

  def to_language_type(self, value: Timestamp) -> datetime.date:

    return value.to_utc_datetime().date()

  @classmethod
  def argument_type(cls):
    return str

  def argument(self):
    return ""

  @classmethod
  def _from_typing(cls, typ):
    return cls()


@LogicalType.register_logical_type
class JdbcTimeType(LogicalType[datetime.time, MillisInstant, str]):
  """
  For internal use only; no backwards-compatibility guarantees.

  Support of Legacy JdbcIO TIME logical type. . Deemed to change when Java
  JDBCIO has been migrated to Beam portable logical types.
  """
  def __init__(self, argument=""):
    pass

  @classmethod
  def representation_type(cls) -> type:
    return Timestamp

  @classmethod
  def urn(cls):
    return "beam:logical_type:javasdk_time:v1"

  @classmethod
  def language_type(cls):
    return datetime.time

  def to_representation_type(self, value: datetime.date) -> Timestamp:
    return Timestamp.from_utc_datetime(
        datetime.datetime.combine(
            datetime.datetime.utcfromtimestamp(0),
            value,
            tzinfo=datetime.timezone.utc))

  def to_language_type(self, value: Timestamp) -> datetime.date:

    return value.to_utc_datetime().time()

  @classmethod
  def argument_type(cls):
    return str

  def argument(self):
    return ""

  @classmethod
  def _from_typing(cls, typ):
    return cls()
