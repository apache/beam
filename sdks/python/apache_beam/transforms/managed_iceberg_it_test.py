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

import datetime
import os
import unittest
import uuid

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@pytest.mark.uses_io_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class ManagedIcebergIT(unittest.TestCase):
  WAREHOUSE = "gs://temp-storage-for-end-to-end-tests"

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.args = self.test_pipeline.get_full_options_as_args()
    self.args.extend([
        # '--experiments=enable_managed_transforms',
    ])

  def _create_row(self, num: int):
    return beam.Row(
        int_=num,
        str_=str(num),
        bytes_=bytes(num),
        bool_=(num % 2 == 0),
        float_=(num + float(num) / 100),
        arr_=[num, num, num],
        date_=datetime.date.today() - datetime.timedelta(days=num))

  def test_write_read_pipeline(self):
    biglake_catalog_props = {
        'type': 'rest',
        'uri': 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
        'warehouse': self.WAREHOUSE,
        'header.x-goog-user-project': 'apache-beam-testing',
        'rest.auth.type': 'google',
        'io-impl': 'org.apache.iceberg.gcp.gcs.GCSFileIO',
        'header.X-Iceberg-Access-Delegation': 'vended-credentials',
        'rest-metrics-reporting-enabled': 'false'
    }
    iceberg_config = {
        "table": "test_iceberg_write_read.test_" + uuid.uuid4().hex,
        "catalog_name": "default",
        "catalog_properties": biglake_catalog_props
    }

    rows = [self._create_row(i) for i in range(100)]
    expected_dicts = [row.as_dict() for row in rows]

    with beam.Pipeline(argv=self.args) as write_pipeline:
      _ = (
          write_pipeline
          | beam.Create(rows)
          | beam.managed.Write(beam.managed.ICEBERG, config=iceberg_config))

    with beam.Pipeline(argv=self.args) as read_pipeline:
      output_dicts = (
          read_pipeline
          | beam.managed.Read(beam.managed.ICEBERG, config=iceberg_config)
          | beam.Map(lambda row: row._asdict()))

      assert_that(output_dicts, equal_to(expected_dicts))


if __name__ == '__main__':
  unittest.main()
