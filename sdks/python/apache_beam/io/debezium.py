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

""" Unbounded source transform for
    `Debezium <href="https://debezium.io/"/>`_.

  This transform is currently supported by Beam portable
  Flink, Spark, and Dataflow v2 runners.

  **Setup**

  Transform provided in this module is cross-language transform
  implemented in the Beam Java SDK. During the pipeline construction, Python SDK
  will connect to a Java expansion service to expand this transform.
  To facilitate this, a small amount of setup is needed before using this
  transform in a Beam Python pipeline.

  There are several ways to setup cross-language Debezium transform.

  * Option 1: use the default expansion service
  * Option 2: specify a custom expansion service

  See below for details regarding each of these options.

  *Option 1: Use the default expansion service*

  This is the recommended and easiest setup option for using Python Debezium
  transform. This option requires following pre-requisites
  before running the Beam pipeline.

  * Install Java runtime in the computer from where the pipeline is constructed
    and make sure that 'java' command is available.

  In this option, Python SDK will either download (for released Beam version) or
  build (when running from a Beam Git clone) a expansion service jar and use
  that to expand transforms. Currently Debezium transform use the
  'beam-sdks-java-io-debezium-expansion-service' jar for this purpose.

  *Option 2: specify a custom expansion service*

  In this option, you startup your own expansion service and provide that as
  a parameter when using the transform provided in this module.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Startup your own expansion service.
  * Update your pipeline to provide the expansion service address when
    initiating Debezium transform provided in this module.

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

import json
from enum import Enum
from typing import List
from typing import NamedTuple
from typing import Optional

from apache_beam.transforms import DoFn
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = ['ReadFromDebezium', 'DriverClassName']


def default_io_expansion_service():
  return BeamJarExpansionService(
      'sdks:java:io:debezium:expansion-service:shadowJar')


class DriverClassName(Enum):
  MYSQL = 'MySQL'
  POSTGRESQL = 'PostgreSQL'
  ORACLE = 'Oracle'
  DB2 = 'Db2'


ReadFromDebeziumSchema = NamedTuple(
    'ReadFromDebeziumSchema',
    [('connector_class', str), ('username', str), ('password', str),
     ('host', str), ('port', str), ('max_number_of_records', Optional[int]),
     ('connection_properties', List[str])])


class _JsonStringToDictionaries(DoFn):
  """ A DoFn that consumes a JSON string and yields a python dictionary """
  def process(self, json_string):
    obj = json.loads(json_string)
    yield obj


class ReadFromDebezium(PTransform):
  """
        An external PTransform which reads from Debezium and returns
        a Dictionary for each item in the specified database
        connection.

        Experimental; no backwards compatibility guarantees.
    """
  URN = 'beam:transform:org.apache.beam:debezium_read:v1'

  def __init__(
      self,
      connector_class,
      username,
      password,
      host,
      port,
      max_number_of_records=None,
      connection_properties=None,
      expansion_service=None):
    """
        Initializes a read operation from Debezium.

        :param connector_class: name of the jdbc driver class
        :param username: database username
        :param password: database password
        :param host: database host
        :param port: database port
        :param max_number_of_records: maximum number of records
                                      to be fetched before stop.
        :param connection_properties: properties of the debezium
                                      connection passed as string
                                      with format
                                      [propertyName=property;]*
        :param expansion_service: The address (host:port)
                                  of the ExpansionService.
    """
    self.params = ReadFromDebeziumSchema(
        connector_class=connector_class.value,
        username=username,
        password=password,
        host=host,
        port=port,
        max_number_of_records=max_number_of_records,
        connection_properties=connection_properties)
    self.expansion_service = expansion_service or default_io_expansion_service()

  def expand(self, pbegin):
    return (
        pbegin | ExternalTransform(
            self.URN,
            NamedTupleBasedPayloadBuilder(self.params),
            self.expansion_service,
        ) | ParDo(_JsonStringToDictionaries()))
