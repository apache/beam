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

import os
import time
import unittest

import pytest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.testing.test_pipeline import TestPipeline


@pytest.mark.uses_io_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class ManagedIcebergIT(unittest.TestCase):
  WAREHOUSE = "gs://temp-storage-for-end-to-end-tests/xlang-python-using-java"
  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.args = self.test_pipeline.get_full_options_as_args()
    self.args.extend([
        '--experiments=enable_managed_transforms',
        '--dataflow_endpoint=https://dataflow-staging.sandbox.googleapis.com',
    ])

  def _create_row(self, num: int):
    return beam.Row(
        int_=num,
        str_=str(num),
        bytes_=bytes(num),
        bool_=(num % 2 == 0),
        float_=(num + float(num) / 100))

  def test_write_read_pipeline(self):
    iceberg_config = {
        "table": "test_iceberg_write_read.test_" + str(int(time.time())),
        "catalog_name": "default",
        "catalog_properties": {
            "type": "hadoop",
            "warehouse": self.WAREHOUSE,
        }
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
