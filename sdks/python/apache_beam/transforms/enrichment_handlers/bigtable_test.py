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
  from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.bigtable_it_test import _row_key_fn
except ImportError:
  raise unittest.SkipTest('Bigtable test dependencies are not installed.')


class TestBigTableEnrichmentHandler(unittest.TestCase):
  @parameterized.expand([('product_id', _row_key_fn), ('', None)])
  def test_bigtable_enrichment_invalid_args(self, row_key, row_key_fn):
    with self.assertRaises(ValueError):
      _ = BigTableEnrichmentHandler(
          project_id='apache-beam-testing',
          instance_id='beam-test',
          table_id='bigtable-enrichment-test',
          row_key=row_key,
          row_key_fn=row_key_fn)


if __name__ == '__main__':
  unittest.main()
