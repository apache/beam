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

# TODO(BEAM-11838)

# TODO(BEAM-11838) DocString
import typing
from enum import Enum

from apache_beam.coders import RowCoder
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder
from apache_beam.typehints.schemas import typing_to_runner_api

# TODO(BEAM-11838) List all methods
__all__ = [
    'ReadFromDebezium',
    'Connector'
]


def default_io_expansion_service():
    return BeamJarExpansionService('sdks:java:io:expansion-service:shadowJar')


class DriverClassName(Enum):
    MYSQL = 'MySQL'
    POSTGRESQL = 'PostgreSQL'
    ORACLE = 'Oracle'
    DB2 = 'Db2'


ReadFromDebeziumSchema = typing.NamedTuple(
    'ReadFromDebeziumSchema',
    [('connector_class', str),
     ('username', str),
     ('password', str),
     ('host', str),
     ('port', str),
     ('max_number_of_records', typing.Optional[int]),
     ('connection_properties', typing.List[str])]
)


class ReadFromDebezium(ExternalTransform):
    """
        An external PTransform which reads from Debezium and returns a JSON string

        Experimental; no backwards compatibility guarantees.
    """
    URN = 'beam:external:java:debezium:read:v1'

    def __init__(
            self,
            connector_class,
            username,
            password,
            host,
            port,
            max_number_of_records=None,
            connection_properties=[],
            expansion_service=None):
        """
        Initializes a read operation to Debezium.

        :param connector_class: name of the jdbc driver class
        :param username: database username
        :param password: database password
        :param host: database host
        :param port: database port
        :param max_number_of_records: maximum number of records to be fetched before stop.
        :param connection_properties: properties of the debezium connection
                                      passed as string with format
                                      [propertyName=property;]*
        :param expansion_service: The address (host:port) of the ExpansionService.
        """
        super(ReadFromDebezium, self).__init__(
            self.URN,
            NamedTupleBasedPayloadBuilder(
                ReadFromDebeziumSchema(
                    connector_class=connector_class.value,
                    username=username,
                    password=password,
                    host=host,
                    port=port,
                    max_number_of_records=max_number_of_records,
                    connection_properties=connection_properties
                )),
            #     TODO(BEAM-11838) Add connection properties as param
            expansion_service or default_io_expansion_service())
