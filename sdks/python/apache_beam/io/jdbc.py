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
  Flink and Spark runners.

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

from __future__ import absolute_import

import typing

from past.builtins import unicode

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = [
    'WriteToJdbc',
    'ReadFromJdbc',
]


def default_io_expansion_service():
  return BeamJarExpansionService('sdks:java:io:expansion-service:shadowJar')


WriteToJdbcSchema = typing.NamedTuple(
    'WriteToJdbcSchema',
    [
        ('driver_class_name', unicode),
        ('jdbc_url', unicode),
        ('username', unicode),
        ('password', unicode),
        ('connection_properties', typing.Optional[unicode]),
        ('connection_init_sqls', typing.Optional[typing.List[unicode]]),
        ('statement', unicode),
    ],
)


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
              driver_class_name='org.postgresql.Driver',
              jdbc_url='jdbc:postgresql://localhost:5432/example',
              username='postgres',
              password='postgres',
              statement='INSERT INTO example_table VALUES(?, ?)',
          ))

  Experimental; no backwards compatibility guarantees.
  """

  URN = 'beam:external:java:jdbc:write:v1'

  def __init__(
      self,
      driver_class_name,
      jdbc_url,
      username,
      password,
      statement,
      connection_properties=None,
      connection_init_sqls=None,
      expansion_service=None,
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
    :param expansion_service: The address (host:port) of the ExpansionService.
    """

    super(WriteToJdbc, self).__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            WriteToJdbcSchema(
                driver_class_name=driver_class_name,
                jdbc_url=jdbc_url,
                username=username,
                password=password,
                statement=statement,
                connection_properties=connection_properties,
                connection_init_sqls=connection_init_sqls,
            ),
        ),
        expansion_service or default_io_expansion_service(),
    )


ReadFromJdbcSchema = typing.NamedTuple(
    'ReadFromJdbcSchema',
    [
        ('driver_class_name', unicode),
        ('jdbc_url', unicode),
        ('username', unicode),
        ('password', unicode),
        ('connection_properties', typing.Optional[unicode]),
        ('connection_init_sqls', typing.Optional[typing.List[unicode]]),
        ('query', unicode),
        ('fetch_size', typing.Optional[int]),
        ('output_parallelization', typing.Optional[bool]),
    ],
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
              driver_class_name='org.postgresql.Driver',
              jdbc_url='jdbc:postgresql://localhost:5432/example',
              username='postgres',
              password='postgres',
              query='SELECT * FROM example_table',
          ))

  Experimental; no backwards compatibility guarantees.
  """

  URN = 'beam:external:java:jdbc:read_rows:v1'

  def __init__(
      self,
      driver_class_name,
      jdbc_url,
      username,
      password,
      query,
      output_parallelization=None,
      fetch_size=None,
      connection_properties=None,
      connection_init_sqls=None,
      expansion_service=None,
  ):
    """
    Initializes a read operation from Jdbc.

    :param driver_class_name: name of the jdbc driver class
    :param jdbc_url: full jdbc url to the database.
    :param username: database username
    :param password: database password
    :param query: sql query to be executed
    :param output_parallelization: is output parallelization on
    :param fetch_size: how many rows to fetch
    :param connection_properties: properties of the jdbc connection
                                  passed as string with format
                                  [propertyName=property;]*
    :param connection_init_sqls: required only for MySql and MariaDB.
                                 passed as list of strings
    :param expansion_service: The address (host:port) of the ExpansionService.
    """
    super(ReadFromJdbc, self).__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            ReadFromJdbcSchema(
                driver_class_name=driver_class_name,
                jdbc_url=jdbc_url,
                username=username,
                password=password,
                query=query,
                output_parallelization=output_parallelization,
                fetch_size=fetch_size,
                connection_properties=connection_properties,
                connection_init_sqls=connection_init_sqls,
            ),
        ),
        expansion_service or default_io_expansion_service(),
    )
