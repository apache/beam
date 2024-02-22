# coding=utf-8
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
# pylint: disable=line-too-long

import unittest
from io import StringIO

import mock

# pylint: disable=unused-import
try:
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import enrichment_with_bigtable
  from apache_beam.io.requestresponse import RequestResponseIO
except ImportError:
  raise unittest.SkipTest('RequestResponseIO dependencies are not installed')


def validate_enrichment_with_bigtable():
  expected = '''[START enrichment_with_bigtable]
Row(sale_id=1, customer_id=1, product_id=1, quantity=1, product={'product_id': '1', 'product_name': 'pixel 5', 'product_stock': '2'})
Row(sale_id=3, customer_id=3, product_id=2, quantity=3, product={'product_id': '2', 'product_name': 'pixel 6', 'product_stock': '4'})
Row(sale_id=5, customer_id=5, product_id=4, quantity=2, product={'product_id': '4', 'product_name': 'pixel 8', 'product_stock': '10'})
  [END enrichment_with_bigtable]'''.splitlines()[1:-1]
  return expected


@mock.patch('sys.stdout', new_callable=StringIO)
class EnrichmentTest(unittest.TestCase):
  def test_enrichment_with_bigtable(self, mock_stdout):
    enrichment_with_bigtable()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigtable()
    self.assertEqual(output, expected)


if __name__ == '__main__':
  unittest.main()
