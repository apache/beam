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

from typing import NamedTuple
from typing import Optional

from past.builtins import unicode

import apache_beam as beam
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = [
    'ReadFromSnowflake',
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
         account.region.gcp.snowflakecomputing.com.
    :param schema: name of the Snowflake schema in the database to use.
    :param database: name of the Snowflake database to use.
    :param staging_bucket_name: name of the Google Cloud Storage bucket.::
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


def verify_credentials(
    username, password, private_key_path, private_key_passphrase, o_auth_token):
  if not (o_auth_token or (username and password) or
          (username and private_key_path and private_key_passphrase)):
    raise RuntimeError('Snowflake credentials are not set correctly.')
