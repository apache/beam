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
import unittest

from parameterized import parameterized

try:
  from apache_beam.transforms.enrichment_handlers.cloudsql import CloudSQLEnrichmentHandler, DatabaseTypeAdapter
  from apache_beam.transforms.enrichment_handlers.cloudsql_it_test import _row_key_fn
except ImportError:
  raise unittest.SkipTest('Cloud SQL test dependencies are not installed.')


class TestCloudSQLEnrichmentHandler(unittest.TestCase):
  @parameterized.expand([('product_id', _row_key_fn), ('', None)])
  def test_cloud_sql_enrichment_invalid_args(self, row_key, row_key_fn):
    with self.assertRaises(ValueError):
      _ = CloudSQLEnrichmentHandler(
          project_id='apache-beam-testing',
          region_id='us-east1',
          instance_id='beam-test',
          table_id='cloudsql-enrichment-test',
          database_type_adapter=DatabaseTypeAdapter.POSTGRESQL,
          database_id='',
          database_user='',
          database_password='',
          row_key=row_key,
          row_key_fn=row_key_fn)


if __name__ == '__main__':
  unittest.main()
