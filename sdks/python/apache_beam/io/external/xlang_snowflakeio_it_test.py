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
Integration test for cross-language snowflake io operations.

Example run:

python setup.py nosetests --tests=apache_beam.io.external.snowflake_test \
--test-pipeline-options="
  --server_name=<SNOWFLAKE_SERVER_NAME>
  --username=<SNOWFLAKE_USERNAME>
  --password=<SNOWFLAKE_PASSWORD>
  --private_key_path=<PATH_TO_PRIVATE_KEY>
  --raw_private_key=<RAW_PRIVATE_KEY>
  --private_key_passphrase=<PASSWORD_TO_PRIVATE_KEY>
  --o_auth_token=<TOKEN>
  --staging_bucket_name=<GCP_BUCKET_PATH>
  --storage_integration_name=<SNOWFLAKE_STORAGE_INTEGRATION_NAME>
  --database=<DATABASE>
  --schema=<SCHEMA>
  --role=<ROLE>
  --warehouse=<WAREHOUSE>
  --table=<TABLE_NAME>
  --runner=FlinkRunner"
"""

# pytype: skip-file

import argparse
import binascii
import logging
import unittest
from typing import ByteString
from typing import NamedTuple

import apache_beam as beam
from apache_beam import coders
from apache_beam.io.snowflake import CreateDisposition
from apache_beam.io.snowflake import ReadFromSnowflake
from apache_beam.io.snowflake import WriteDisposition
from apache_beam.io.snowflake import WriteToSnowflake
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from apache_beam.io.gcp.gcsfilesystem import GCSFileSystem
except ImportError:
  GCSFileSystem = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports

SCHEMA_STRING = """
{"schema":[
    {"dataType":{"type":"integer","precision":38,"scale":0},"name":"number_column","nullable":false},
    {"dataType":{"type":"boolean"},"name":"boolean_column","nullable":false},
    {"dataType":{"type":"binary","size":100},"name":"bytes_column","nullable":true}
]}
"""

TestRow = NamedTuple(
    'TestRow',
    [
        ('number_column', int),
        ('boolean_column', bool),
        ('bytes_column', ByteString),
    ])

coders.registry.register_coder(TestRow, coders.RowCoder)

NUM_RECORDS = 100


@unittest.skipIf(GCSFileSystem is None, 'GCP dependencies are not installed')
@unittest.skipIf(
    TestPipeline().get_option('server_name') is None,
    'Snowflake IT test requires external configuration to be run.')
class SnowflakeTest(unittest.TestCase):
  def test_snowflake_write_read(self):
    self.run_write()
    self.run_read()

  def run_write(self):
    def user_data_mapper(test_row):
      return [
          str(test_row.number_column).encode('utf-8'),
          str(test_row.boolean_column).encode('utf-8'),
          binascii.hexlify(test_row.bytes_column),
      ]

    with TestPipeline(options=PipelineOptions(self.pipeline_args)) as p:
      p.not_use_test_runner_api = True
      _ = (
          p
          | 'Impulse' >> beam.Impulse()
          | 'Generate' >> beam.FlatMap(lambda x: range(NUM_RECORDS))  # pylint: disable=bad-option-value
          | 'Map to TestRow' >> beam.Map(
              lambda num: TestRow(
                  num, num % 2 == 0, b"test" + str(num).encode()))
          | WriteToSnowflake(
              server_name=self.server_name,
              username=self.username,
              password=self.password,
              o_auth_token=self.o_auth_token,
              private_key_path=self.private_key_path,
              raw_private_key=self.raw_private_key,
              private_key_passphrase=self.private_key_passphrase,
              schema=self.schema,
              database=self.database,
              role=self.role,
              warehouse=self.warehouse,
              staging_bucket_name=self.staging_bucket_name,
              storage_integration_name=self.storage_integration_name,
              create_disposition=CreateDisposition.CREATE_IF_NEEDED,
              write_disposition=WriteDisposition.TRUNCATE,
              table_schema=SCHEMA_STRING,
              user_data_mapper=user_data_mapper,
              table=self.table,
              query=None,
              expansion_service=self.expansion_service,
          ))

  def run_read(self):
    def csv_mapper(bytes_array):
      return TestRow(
          int(bytes_array[0]),
          bytes_array[1] == b'true',
          binascii.unhexlify(bytes_array[2]))

    with TestPipeline(options=PipelineOptions(self.pipeline_args)) as p:
      result = (
          p
          | ReadFromSnowflake(
              server_name=self.server_name,
              username=self.username,
              password=self.password,
              o_auth_token=self.o_auth_token,
              private_key_path=self.private_key_path,
              raw_private_key=self.raw_private_key,
              private_key_passphrase=self.private_key_passphrase,
              schema=self.schema,
              database=self.database,
              role=self.role,
              warehouse=self.warehouse,
              staging_bucket_name=self.staging_bucket_name,
              storage_integration_name=self.storage_integration_name,
              csv_mapper=csv_mapper,
              table=self.table,
              query=None,
              expansion_service=self.expansion_service,
          ).with_output_types(TestRow))

      assert_that(
          result,
          equal_to([
              TestRow(i, i % 2 == 0, b'test' + str(i).encode())
              for i in range(NUM_RECORDS)
          ]))

  @classmethod
  def tearDownClass(cls):
    GCSFileSystem(pipeline_options=PipelineOptions()) \
        .delete([cls.staging_bucket_name])

  @classmethod
  def setUpClass(cls):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--server_name',
        required=True,
        help=(
            'Snowflake server name of the form '
            'https://<SNOWFLAKE_ACCOUNT_NAME>.snowflakecomputing.com'),
    )
    parser.add_argument(
        '--username',
        help='Snowflake username',
    )
    parser.add_argument(
        '--password',
        help='Snowflake password',
    )
    parser.add_argument(
        '--private_key_path',
        help='Path to private key',
    )
    parser.add_argument(
        '--raw_private_key',
        help='Raw private key',
    )
    parser.add_argument(
        '--private_key_passphrase',
        help='Password to private key',
    )
    parser.add_argument(
        '--o_auth_token',
        help='OAuth token',
    )
    parser.add_argument(
        '--staging_bucket_name',
        required=True,
        help='GCP staging bucket name (must end with backslash)',
    )
    parser.add_argument(
        '--storage_integration_name',
        required=True,
        help='Snowflake integration name',
    )
    parser.add_argument(
        '--database',
        required=True,
        help='Snowflake database name',
    )
    parser.add_argument(
        '--schema',
        required=True,
        help='Snowflake schema name',
    )
    parser.add_argument(
        '--table',
        required=True,
        help='Snowflake table name',
    )
    parser.add_argument(
        '--role',
        help='Snowflake role',
    )
    parser.add_argument(
        '--warehouse',
        help='Snowflake warehouse name',
    )
    parser.add_argument(
        '--expansion_service',
        help='Url to externally launched expansion service.',
    )

    pipeline = TestPipeline()
    argv = pipeline.get_full_options_as_args()

    known_args, cls.pipeline_args = parser.parse_known_args(argv)

    cls.server_name = known_args.server_name
    cls.database = known_args.database
    cls.schema = known_args.schema
    cls.table = known_args.table
    cls.username = known_args.username
    cls.password = known_args.password
    cls.private_key_path = known_args.private_key_path
    cls.raw_private_key = known_args.raw_private_key
    cls.private_key_passphrase = known_args.private_key_passphrase
    cls.o_auth_token = known_args.o_auth_token
    cls.staging_bucket_name = known_args.staging_bucket_name
    cls.storage_integration_name = known_args.storage_integration_name
    cls.role = known_args.role
    cls.warehouse = known_args.warehouse
    cls.expansion_service = known_args.expansion_service


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
