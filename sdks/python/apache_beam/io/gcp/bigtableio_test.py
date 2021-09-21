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

"""Unit tests for BigTable service."""

# pytype: skip-file
import datetime
import unittest
import uuid

from mock import MagicMock

# Protect against environments where bigtable library is not available.
try:
  from apache_beam.io.gcp import resource_identifiers, bigtableio
  from apache_beam.metrics import monitoring_infos
  from apache_beam.metrics.execution import MetricsEnvironment
  from google.api_core import exceptions
  from google.cloud.bigtable import client, row
except ImportError as e:
  client = None


@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
class TestWriteBigTable(unittest.TestCase):
  def test_write_metrics(self):
    MetricsEnvironment.process_wide_container().reset()
    TABLE_PREFIX = "python-test"
    project_id = TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
    instance_id = TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
    table_id = TABLE_PREFIX + "-" + str(uuid.uuid4())[:8]
    write_fn = bigtableio._BigTableWriteFn(project_id, instance_id, table_id)
    write_fn.table = MagicMock()
    write_fn.start_bundle()
    write_fn.batcher.mutate_rows.side_effect = [
        exceptions.DeadlineExceeded("Deadline Exceeded"), []
    ]
    direct_row = self.generate_row(1)
    write_fn.process(direct_row)
    self.verify_write_call_metric(project_id, instance_id, table_id, "ok", 1)

  def generate_row(self, row_number=1):
    value = ''.join(self.rand for i in range(100))
    for index in range(row_number):
      key = "beam_key%s" % ('{0:07}'.format(index))
      direct_row = row.DirectRow(row_key=key)
      for column_id in range(10):
        direct_row.set_cell(
            self.column_family_id, ('field%s' % column_id).encode('utf-8'),
            value,
            datetime.datetime.now())
      yield direct_row

  def verify_write_call_metric(
      self, project_id, instance_id, table_id, status, count):
    """Check if a metric was recorded for the Datastore IO write API call."""
    process_wide_monitoring_infos = list(
        MetricsEnvironment.process_wide_container().
        to_runner_api_monitoring_infos(None).values())
    resource = resource_identifiers.BigtableTable(
        project_id, instance_id, table_id)
    labels = {
        monitoring_infos.SERVICE_LABEL: 'BigTable',
        monitoring_infos.METHOD_LABEL: 'google.bigtable.v2.MutateRows',
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.BIGTABLE_PROJECT_ID_LABEL: project_id,
        monitoring_infos.INSTANCE_ID_LABEL: instance_id,
        monitoring_infos.TABLE_ID_LABEL: table_id
    }
    expected_mi = monitoring_infos.int64_counter(
        monitoring_infos.API_REQUEST_COUNT_URN, count, labels=labels)
    expected_mi.ClearField("start_time")

    found = False
    for actual_mi in process_wide_monitoring_infos:
      actual_mi.ClearField("start_time")
      if expected_mi == actual_mi:
        found = True
        break
    self.assertTrue(
        found, "Did not find write call metric with status: %s" % status)


if __name__ == '__main__':
  unittest.main()
