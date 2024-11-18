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
import secrets
import shutil
import tempfile
import time
import unittest

import pytest

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


@pytest.mark.uses_io_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class ManagedIcebergIT(unittest.TestCase):
  def setUp(self):
    self._tempdir = tempfile.mkdtemp()
    if not os.path.exists(self._tempdir):
      os.mkdir(self._tempdir)
    test_warehouse_name = 'test_warehouse_%d_%s' % (
        int(time.time()), secrets.token_hex(3))
    self.warehouse_path = os.path.join(self._tempdir, test_warehouse_name)
    os.mkdir(self.warehouse_path)

  def tearDown(self):
    shutil.rmtree(self._tempdir, ignore_errors=False)

  def _create_row(self, num: int):
    return beam.Row(
        int_=num,
        str_=str(num),
        bytes_=bytes(num),
        bool_=(num % 2 == 0),
        float_=(num + float(num) / 100))

  def test_write_read_pipeline(self):
    iceberg_config = {
        "table": "test.write_read",
        "catalog_name": "default",
        "catalog_properties": {
            "type": "hadoop",
            "warehouse": f"file://{self.warehouse_path}",
        }
    }

    rows = [self._create_row(i) for i in range(100)]
    expected_dicts = [row.as_dict() for row in rows]

    with beam.Pipeline() as write_pipeline:
      _ = (
          write_pipeline
          | beam.Create(rows)
          | beam.managed.Write(beam.managed.ICEBERG, config=iceberg_config))

    with beam.Pipeline() as read_pipeline:
      output_dicts = (
          read_pipeline
          | beam.managed.Read(beam.managed.ICEBERG, config=iceberg_config)
          | beam.Map(lambda row: row._asdict()))

      assert_that(output_dicts, equal_to(expected_dicts))


if __name__ == '__main__':
  unittest.main()
