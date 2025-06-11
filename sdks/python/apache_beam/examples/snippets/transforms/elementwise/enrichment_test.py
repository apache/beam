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
# ruff: noqa: E501

import unittest
from io import StringIO

import mock

# pylint: disable=unused-import
try:
    from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
        enrichment_with_bigtable,
        enrichment_with_vertex_ai_legacy,
    )
    from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
        enrichment_with_vertex_ai,
    )
    from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
        enrichment_with_bigquery_storage_basic,
        enrichment_with_bigquery_storage_custom_function,
    )
except ImportError:
    raise unittest.SkipTest("RequestResponseIO dependencies are not installed")


def validate_enrichment_with_bigtable():
    expected = (
        """[START enrichment_with_bigtable]
Row(sale_id=1, customer_id=1, product_id=1, quantity=1, """
        """product={'product_id': '1', 'product_name': 'pixel 5', 'product_stock': '2'})
Row(sale_id=3, customer_id=3, product_id=2, quantity=3, """
        """product={'product_id': '2', 'product_name': 'pixel 6', 'product_stock': '4'})
Row(sale_id=5, customer_id=5, product_id=4, quantity=2, """
        """product={'product_id': '4', 'product_name': 'pixel 8', 'product_stock': '10'})
  [END enrichment_with_bigtable]"""
    ).splitlines()[1:-1]
    return expected


def validate_enrichment_with_vertex_ai():
    expected = (
        """[START enrichment_with_vertex_ai]
Row(user_id='2963', product_id=14235, sale_price=15.0, """
        """age=12.0, state='1', gender='1', country='1')
Row(user_id='21422', product_id=11203, sale_price=12.0, """
        """age=12.0, state='0', gender='0', country='0')
Row(user_id='20592', product_id=8579, sale_price=9.0, """
        """age=12.0, state='2', gender='1', country='2')
  [END enrichment_with_vertex_ai]"""
    ).splitlines()[1:-1]
    return expected


def validate_enrichment_with_vertex_ai_legacy():
    expected = """[START enrichment_with_vertex_ai_legacy]
Row(entity_id='movie_01', title='The Shawshank Redemption', genres='Drama')
Row(entity_id='movie_02', title='The Shining', genres='Horror')
Row(entity_id='movie_04', title='The Dark Knight', genres='Action')
  [END enrichment_with_vertex_ai_legacy]""".splitlines()[1:-1]
    return expected


def validate_enrichment_with_bigquery_storage_basic():
    expected = (
        """[START enrichment_with_bigquery_storage_basic]
Row(sale_id=1001, product_id=101, customer_id=501, quantity=2, """
        """product_id=101, product_name='Laptop Pro', category='Electronics', unit_price=999.99)
Row(sale_id=1002, product_id=102, customer_id=502, quantity=1, """
        """product_id=102, product_name='Wireless Mouse', category='Electronics', unit_price=29.99)
Row(sale_id=1003, product_id=103, customer_id=503, quantity=5, """
        """product_id=103, product_name='Office Chair', category='Furniture', unit_price=199.99)
  [END enrichment_with_bigquery_storage_basic]"""
    ).splitlines()[1:-1]
    return expected


def validate_enrichment_with_bigquery_storage_custom_function():
    expected = (
        """[START enrichment_with_bigquery_storage_custom_function]
Row(sale_id=1002, product_id=102, category='Electronics', customer_id=502, """
        """quantity=4, prod_id=102, name='Wireless Mouse', category='Electronics', price=29.99)
Row(sale_id=1004, product_id=101, category='Electronics', customer_id=504, """
        """quantity=6, prod_id=101, name='Laptop Pro', category='Electronics', price=999.99)
  [END enrichment_with_bigquery_storage_custom_function]"""
    ).splitlines()[1:-1]
    return expected


@mock.patch("sys.stdout", new_callable=StringIO)
class EnrichmentTest(unittest.TestCase):
    def test_enrichment_with_bigtable(self, mock_stdout):
        enrichment_with_bigtable()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_bigtable()
        self.assertEqual(output, expected)

    def test_enrichment_with_vertex_ai(self, mock_stdout):
        enrichment_with_vertex_ai()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_vertex_ai()

        for i in range(len(expected)):
            self.assertEqual(set(output[i].split(",")), set(expected[i].split(",")))

    def test_enrichment_with_vertex_ai_legacy(self, mock_stdout):
        enrichment_with_vertex_ai_legacy()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_vertex_ai_legacy()
        self.maxDiff = None
        self.assertEqual(output, expected)

    def test_enrichment_with_bigquery_storage_basic(self, mock_stdout):
        enrichment_with_bigquery_storage_basic()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_bigquery_storage_basic()
        self.maxDiff = None
        self.assertEqual(output, expected)

    def test_enrichment_with_bigquery_storage_custom_function(self, mock_stdout):
        enrichment_with_bigquery_storage_custom_function()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_bigquery_storage_custom_function()
        self.assertEqual(output, expected)


if __name__ == "__main__":
    unittest.main()
