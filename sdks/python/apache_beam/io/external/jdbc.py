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

"""
  PTransforms for supporting Jdbc in Python pipelines. These transforms do not
  run a Jdbc client in Python. Instead, they expand to ExternalTransforms
  which the Expansion Service resolves to the Java SDK's JdbcIO.

  Note: To use these transforms, you need to start a Java Expansion Service.
  Please refer to the portability documentation on how to do that. Flink Users
  can use the built-in Expansion Service of the Flink Runner's Job Server. The
  expansion service address has to be provided when instantiating the
  transforms.

  If you start Flink's Job Server, the expansion service will be started on
  port 8097. This is also the configured default for this transform. For a
  different address, please set the expansion_service parameter.

  For more information see:
  - https://beam.apache.org/documentation/runners/flink/
  - https://beam.apache.org/roadmap/portability/
"""

# pytype: skip-file

from __future__ import absolute_import

import typing

from past.builtins import unicode

from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = ['WriteToJdbc']


def default_io_expansion_service():
  return BeamJarExpansionService('sdks:java:io:expansion-service:shadowJar')


WriteToJdbcSchema = typing.NamedTuple(
    'WriteToJdbcSchema',
    [
        ('driver_class_name', unicode),
        ('jdbc_url', unicode),
        ('username', unicode),
        ('password', unicode),
        ('statement', unicode),
        ('connection_properties', unicode),
        ('connection_init_sqls', typing.Optional[typing.List[unicode]]),
    ],
)


class WriteToJdbc(ExternalTransform):
  """
  An external PTransform which writes Rows to the specified database.

  This transform receives Rows defined as NamedTuple type and registered in
  the coders registry to use RowCoder, e.g.

    import typing
    from apache_beam import coders

    ExampleRow = typing.NamedTuple(
      "ExampleRow",
      [
        ("id", int),
        ("name", unicode),
        ("budget", float),
      ],
    )
    coders.registry.register_coder(ExampleRow, coders.RowCoder)

  Experimental; no backwards compatibility guarantees.  It requires special
  preparation of the Java SDK.  See BEAM-7870.
  """

  URN = 'beam:external:java:jdbc:write:v1'

  def __init__(
      self,
      driver_class_name,
      jdbc_url,
      username,
      password,
      statement,
      connection_properties='',
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
    :param connection_init_sqls: required only for MySql and MariaDB
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
