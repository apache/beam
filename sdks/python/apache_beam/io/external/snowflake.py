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

# pytype: skip-file

from __future__ import absolute_import

import typing

from past.builtins import unicode

import apache_beam as beam
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

__all__ = ['ReadFromSnowflake']


def default_io_expansion_service():
  return BeamJarExpansionService('sdks:java:io:expansion-service:shadowJar')


ReadFromSnowflakeSchema = typing.NamedTuple(
    'WriteToSnowflakeSchema',
    [
        ('server_name', unicode),
        ('schema', unicode),
        ('database', unicode),
        ('staging_bucket_name', unicode),
        ('storage_integration_name', unicode),
        ('username', typing.Optional[unicode]),
        ('password', typing.Optional[unicode]),
        ('private_key_path', typing.Optional[unicode]),
        ('private_key_passphrase', typing.Optional[unicode]),
        ('o_auth_token', typing.Optional[unicode]),
        ('table', typing.Optional[unicode]),
        ('query', typing.Optional[unicode]),
    ])


class ReadFromSnowflake(beam.PTransform):
  """An external PTransform which reads from Snowflake."""

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
      expansion_service=None):
    """
    Initializes a read operation from Snowflake.

    Required parameters:
    :param server_name: full Snowflake server name with the following format
        account.region.gcp.snowflakecomputing.com.
    :param schema: name of the Snowflake schema in the database to use.
    :param database: name of the Snowflake database to use.
    :param staging_bucket_name: name of the Google Cloud Storage bucket.
        Bucket will be used as a temporary location for storing CSV files.
        Those temporary directories will be named
        `sf_copy_csv_DATE_TIME_RANDOMSUFFIX`
        and they will be removed automatically once Read operation finishes.
    :param storage_integration_name: is the name of storage integration
        object created according to Snowflake documentation.
    :param csv_mapper: specifies a function which must translate
        user-defined object to array of strings.
        SnowflakeIO uses a COPY INTO <location> statement to
        move data from a Snowflake table to Google Cloud Storage as CSV files.
        These files are then downloaded via FileIO and processed line by line.
        Each line is split into an array of Strings using the OpenCSV
        The csv_mapper function job is to give the user the possibility to
        convert the array of Strings to a user-defined type,
        ie. GenericRecord for Avro or Parquet files, or custom objects.
            Example:
                ```
                    def csv_mapper(strings_array):
 		                return User(strings_array[0], int(strings_array[1])))
                ```
    :param table or query: specifies a Snowflake table name or custom SQL query
    :param expansion_service: specifies URL of expansion service.

    Authentication parameters:
    It's required to pass one of the following combinations of valid parameters:
    :param username and password: specifies username and password
        for username/password authentication method.
    :param private_key_path and private_key_passphrase:
        specifies a private key file and password
        for key/ pair authentication method.
    :param o_auth_token: specifies access token for OAuth authentication method.
    """

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
        query=query)
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
