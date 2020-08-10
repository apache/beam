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

"""Snowflake transforms tested against Flink portable runner.

  **Setup**

  Transforms provided in this module are cross-language transforms
  implemented in the Beam Java SDK. During the pipeline construction, Python SDK
  will connect to a Java expansion service to expand these transforms.
  To facilitate this, a small amount of setup is needed before using these
  transforms in a Beam Python pipeline.

  There are several ways to setup cross-language Snowflake transforms.

  * Option 1: use the default expansion service
  * Option 2: specify a custom expansion service

  See below for details regarding each of these options.

  *Option 1: Use the default expansion service*

  This is the recommended and easiest setup option for using Python Snowflake
  transforms.This option requires following pre-requisites
  before running the Beam pipeline.

  * Install Java runtime in the computer from where the pipeline is constructed
    and make sure that 'java' command is available.

  In this option, Python SDK will either download (for released Beam version) or
  build (when running from a Beam Git clone) a expansion service jar and use
  that to expand transforms. Currently Snowflake transforms use the
  'beam-sdks-java-io-snowflake-expansion-service' jar for this purpose.

  *Option 2: specify a custom expansion service*

  In this option, you startup your own expansion service and provide that as
  a parameter when using the transforms provided in this module.

  This option requires following pre-requisites before running the Beam
  pipeline.

  * Startup your own expansion service.
  * Update your pipeline to provide the expansion service address when
    initiating Snowflake transforms provided in this module.

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

from typing import List
from typing import NamedTuple
from typing import Optional

from past.builtins import unicode

import apache_beam as beam
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = [
    'ReadFromSnowflake',
    'WriteToSnowflake',
    'WriteDisposition',
    'CreateDisposition',
]


def default_io_expansion_service():
  return BeamJarExpansionService(
      'sdks:java:io:snowflake:expansion-service:shadowJar')


ReadFromSnowflakeSchema = NamedTuple(
    'ReadFromSnowflakeSchema',
    [
        ('server_name', unicode),
        ('schema', unicode),
        ('database', unicode),
        ('staging_bucket_name', unicode),
        ('storage_integration_name', unicode),
        ('username', Optional[unicode]),
        ('password', Optional[unicode]),
        ('private_key_path', Optional[unicode]),
        ('private_key_passphrase', Optional[unicode]),
        ('o_auth_token', Optional[unicode]),
        ('table', Optional[unicode]),
        ('query', Optional[unicode]),
        ('role', Optional[unicode]),
        ('warehouse', Optional[unicode]),
    ])


class ReadFromSnowflake(beam.PTransform):
  """
    An external PTransform which reads from Snowflake.
  """

  URN = 'beam:external:java:snowflake:read:v1'

  def __init__(
      self,
      server_name,
      schema,
      database,
      staging_bucket_name,
      storage_integration_name,
      csv_mapper,
      username=None,
      password=None,
      private_key_path=None,
      private_key_passphrase=None,
      o_auth_token=None,
      table=None,
      query=None,
      role=None,
      warehouse=None,
      expansion_service=None):
    """
    Initializes a read operation from Snowflake.

    Required parameters:

    :param server_name: full Snowflake server name with the following format
         https://account.region.gcp.snowflakecomputing.com.
    :param schema: name of the Snowflake schema in the database to use.
    :param database: name of the Snowflake database to use.
    :param staging_bucket_name: name of the Google Cloud Storage bucket.
        Bucket will be used as a temporary location for storing CSV files.
        Those temporary directories will be named
        'sf_copy_csv_DATE_TIME_RANDOMSUFFIX'
        and they will be removed automatically once Read operation finishes.
    :param storage_integration_name: is the name of storage integration
        object created according to Snowflake documentation.
    :param csv_mapper: specifies a function which must translate
        user-defined object to array of strings.
        SnowflakeIO uses a COPY INTO <location> statement to move data from
        a Snowflake table to Google Cloud Storage as CSV files.These files
        are then downloaded via FileIO and processed line by line.
        Each line is split into an array of Strings using the OpenCSV
        The csv_mapper function job is to give the user the possibility to
        convert the array of Strings to a user-defined type,
        ie. GenericRecord for Avro or Parquet files, or custom objects.
        Example:
        def csv_mapper(strings_array)
        return User(strings_array[0], int(strings_array[1])))
    :param table: specifies a Snowflake table name.
    :param query: specifies a Snowflake custom SQL query.
    :param role: specifies a Snowflake role.
    :param warehouse: specifies a Snowflake warehouse name.
    :param expansion_service: specifies URL of expansion service.

    Authentication parameters:

    :param username: specifies username for
        username/password authentication method.
    :param password: specifies password for
        username/password authentication method.
    :param private_key_path: specifies a private key file for
        key/ pair authentication method.
    :param private_key_passphrase: specifies password for
        key/ pair authentication method.
    :param o_auth_token: specifies access token for
        OAuth authentication method.
    """
    verify_credentials(
        username,
        password,
        private_key_path,
        private_key_passphrase,
        o_auth_token,
    )

    self.params = ReadFromSnowflakeSchema(
        server_name=server_name,
        schema=schema,
        database=database,
        staging_bucket_name=staging_bucket_name,
        storage_integration_name=storage_integration_name,
        username=username,
        password=password,
        private_key_path=private_key_path,
        private_key_passphrase=private_key_passphrase,
        o_auth_token=o_auth_token,
        table=table,
        query=query,
        role=role,
        warehouse=warehouse,
    )
    self.csv_mapper = csv_mapper
    self.expansion_service = expansion_service or default_io_expansion_service()

  def expand(self, pbegin):
    return (
        pbegin
        | ExternalTransform(
            self.URN,
            NamedTupleBasedPayloadBuilder(self.params),
            self.expansion_service,
        )
        | 'CSV to array mapper' >> beam.Map(lambda csv: csv.split(b','))
        | 'CSV mapper' >> beam.Map(self.csv_mapper))


WriteToSnowflakeSchema = NamedTuple(
    'WriteToSnowflakeSchema',
    [
        ('server_name', unicode),
        ('schema', unicode),
        ('database', unicode),
        ('staging_bucket_name', unicode),
        ('storage_integration_name', unicode),
        ('create_disposition', unicode),
        ('write_disposition', unicode),
        ('table_schema', unicode),
        ('username', Optional[unicode]),
        ('password', Optional[unicode]),
        ('private_key_path', Optional[unicode]),
        ('private_key_passphrase', Optional[unicode]),
        ('o_auth_token', Optional[unicode]),
        ('table', Optional[unicode]),
        ('query', Optional[unicode]),
        ('role', Optional[unicode]),
        ('warehouse', Optional[unicode]),
    ],
)


class WriteToSnowflake(beam.PTransform):
  """
    An external PTransform which writes to Snowflake.
  """

  URN = 'beam:external:java:snowflake:write:v1'

  def __init__(
      self,
      server_name,
      schema,
      database,
      staging_bucket_name,
      storage_integration_name,
      create_disposition,
      write_disposition,
      table_schema,
      user_data_mapper,
      username=None,
      password=None,
      private_key_path=None,
      private_key_passphrase=None,
      o_auth_token=None,
      table=None,
      query=None,
      role=None,
      warehouse=None,
      expansion_service=None,
  ):
    # pylint: disable=line-too-long

    """
    Initializes a write operation to Snowflake.

    Required parameters:

    :param server_name: full Snowflake server name with the following format
        https://account.region.gcp.snowflakecomputing.com.
    :param schema: name of the Snowflake schema in the database to use.
    :param database: name of the Snowflake database to use.
    :param staging_bucket_name: name of the Google Cloud Storage bucket.
        Bucket will be used as a temporary location for storing CSV files.
    :param storage_integration_name: is the name of a Snowflake storage
        integration object created according to Snowflake documentation for the
        GCS bucket.
    :param user_data_mapper: specifies a function which  maps data from
        a PCollection to an array of String values before the write operation
        saves the data to temporary .csv files.
        Example:
        def user_data_mapper(user):
        return [user.name, str(user.age)]
    :param table: specifies a Snowflake table name
    :param query: specifies a custom SQL query
    :param role: specifies a Snowflake role.
    :param warehouse: specifies a Snowflake warehouse name.
    :param expansion_service: specifies URL of expansion service.

    Authentication parameters:

    :param username: specifies username for
        username/password authentication method.
    :param password: specifies password for
        username/password authentication method.
    :param private_key_path: specifies a private key file for
        key/ pair authentication method.
    :param private_key_passphrase: specifies password for
        key/ pair authentication method.
    :param o_auth_token: specifies access token for
        OAuth authentication method.

    Additional parameters:

    :param create_disposition: Defines the behaviour of the write operation if
        the target table does not exist. The following values are supported:
        CREATE_IF_NEEDED - default behaviour. The write operation checks whether
        the specified target table exists; if it does not, the write operation
        attempts to create the table Specify the schema for the target table
        using the table_schema parameter.
        CREATE_NEVER -  The write operation fails if the target table does not
        exist.
    :param write_disposition: Defines the write behaviour based on the table
        where data will be written to. The following values are supported:
        APPEND - Default behaviour. Written data is added to the existing rows
        in the table,
        EMPTY - The target table must be empty;  otherwise, the write operation
        fails,
        TRUNCATE - The write operation deletes all rows from the target table
        before writing to it.
    :param table_schema: When the create_disposition parameter is set to
        CREATE_IF_NEEDED, the table_schema  parameter  enables specifying the
        schema for the created target table. A table schema is as JSON with the
        following structure:
        {"schema":[
        {
        "dataType":{"type":"<COLUMN DATA TYPE>"},
        "name":"<COLUMN  NAME> ",
        "nullable": <NULLABLE>
        },
        ]}

        All supported data types:
        {"schema":[
        {"dataType":{"type":"date"},"name":"","nullable":false},
        {"dataType":{"type":"datetime"},"name":"","nullable":false},
        {"dataType":{"type":"time"},"name":"","nullable":false},
        {"dataType":{"type":"timestamp"},"name":"","nullable":false},
        {"dataType":{"type":"timestamp_ltz"},"name":"","nullable":false},
        {"dataType":{"type":"timestamp_ntz"},"name":"","nullable":false},
        {"dataType":{"type":"timestamp_tz"},"name":"","nullable":false},
        {"dataType":{"type":"boolean"},"name":"","nullable":false},
        {"dataType":{"type":"decimal","precision":38,"scale":1},"name":"","nullable":true},
        {"dataType":{"type":"double"},"name":"","nullable":false},
        {"dataType":{"type":"float"},"name":"","nullable":false},
        {"dataType":{"type":"integer","precision":38,"scale":0},"name":"","nullable":false},
        {"dataType":{"type":"number","precision":38,"scale":1},"name":"","nullable":false},
        {"dataType":{"type":"numeric","precision":38,"scale":2},"name":"","nullable":false},
        {"dataType":{"type":"real"},"name":"","nullable":false},
        {"dataType":{"type":"array"},"name":"","nullable":false},
        {"dataType":{"type":"object"},"name":"","nullable":false},
        {"dataType":{"type":"variant"},"name":"","nullable":true},
        {"dataType":{"type":"binary","size":null},"name":"","nullable":false},
        {"dataType":{"type":"char","length":1},"name":"","nullable":false},
        {"dataType":{"type":"string","length":null},"name":"","nullable":false},
        {"dataType":{"type":"text","length":null},"name":"","nullable":false},
        {"dataType":{"type":"varbinary","size":null},"name":"","nullable":false},
        {"dataType":{"type":"varchar","length":100},"name":"","nullable":false}]
        }
    """
    verify_credentials(
        username,
        password,
        private_key_path,
        private_key_passphrase,
        o_auth_token,
    )
    WriteDisposition.VerifyParam(write_disposition)
    CreateDisposition.VerifyParam(create_disposition)

    self.params = WriteToSnowflakeSchema(
        server_name=server_name,
        schema=schema,
        database=database,
        staging_bucket_name=staging_bucket_name,
        storage_integration_name=storage_integration_name,
        create_disposition=create_disposition,
        write_disposition=write_disposition,
        table_schema=table_schema,
        username=username,
        password=password,
        private_key_path=private_key_path,
        private_key_passphrase=private_key_passphrase,
        o_auth_token=o_auth_token,
        table=table,
        query=query,
        role=role,
        warehouse=warehouse,
    )
    self.user_data_mapper = user_data_mapper
    self.expansion_service = expansion_service or default_io_expansion_service()

  def expand(self, pbegin):
    return (
        pbegin
        | 'User data mapper' >> beam.Map(
            self.user_data_mapper).with_output_types(List[bytes])
        | ExternalTransform(
            self.URN,
            NamedTupleBasedPayloadBuilder(self.params),
            self.expansion_service))


class CreateDisposition:
  """
  Enum class for possible values of create dispositions:
  CREATE_IF_NEEDED: default behaviour. The write operation checks whether
  the specified target table exists; if it does not, the write operation
  attempts to create the table Specify the schema for the target table
  using the table_schema parameter.
  CREATE_NEVER: The write operation fails if the target table does not exist.
  """
  CREATE_IF_NEEDED = 'CREATE_IF_NEEDED'
  CREATE_NEVER = 'CREATE_NEVER'

  @staticmethod
  def VerifyParam(field):
    if field and not hasattr(CreateDisposition, field):
      raise RuntimeError(
          'Create disposition has to be one of the following values:'
          'CREATE_IF_NEEDED, CREATE_NEVER. Got: {}'.format(field))


class WriteDisposition:
  """
  Enum class for possible values of write dispositions:
  APPEND: Default behaviour. Written data is added to the existing rows
  in the table,
  EMPTY: The target table must be empty;  otherwise, the write operation fails,
  TRUNCATE: The write operation deletes all rows from the target table
  before writing to it.
  """
  APPEND = 'APPEND'
  EMPTY = 'EMPTY'
  TRUNCATE = 'TRUNCATE'

  @staticmethod
  def VerifyParam(field):
    if field and not hasattr(WriteDisposition, field):
      raise RuntimeError(
          'Write disposition has to be one of the following values:'
          'APPEND, EMPTY, TRUNCATE. Got: {}'.format(field))


def verify_credentials(
    username, password, private_key_path, private_key_passphrase, o_auth_token):
  if not (o_auth_token or (username and password) or
          (username and private_key_path and private_key_passphrase)):
    raise RuntimeError('Snowflake credentials are not set correctly.')
