import shutil
import unittest
import apache_beam as beam
import os
import time
import secrets
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import tempfile
import pytest


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
      (
          write_pipeline
          | beam.Create(rows)
          | beam.managed.Write(beam.managed.ICEBERG, config=iceberg_config))

    with beam.Pipeline() as read_pipeline:
      output_dicts = (
          read_pipeline
          | beam.managed.Read(beam.managed.ICEBERG, config=iceberg_config)
          | beam.Map(lambda row: row._asdict()))

      assert_that(output_dicts, equal_to(expected_dicts))
