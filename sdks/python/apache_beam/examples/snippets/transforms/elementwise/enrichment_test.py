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
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import enrichment_with_bigtable, \
  enrichment_with_vertex_ai_legacy
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import enrichment_with_vertex_ai
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
      enrichment_with_bigquery_storage_basic,
      enrichment_with_bigquery_storage_batched,
      enrichment_with_bigquery_storage_column_aliasing,
      enrichment_with_bigquery_storage_multiple_fields,
      enrichment_with_bigquery_storage_custom_function,
      enrichment_with_bigquery_storage_performance_tuned)
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


def validate_enrichment_with_vertex_ai():
  expected = '''[START enrichment_with_vertex_ai]
Row(user_id='2963', product_id=14235, sale_price=15.0, age=12.0, state='1', gender='1', country='1')
Row(user_id='21422', product_id=11203, sale_price=12.0, age=12.0, state='0', gender='0', country='0')
Row(user_id='20592', product_id=8579, sale_price=9.0, age=12.0, state='2', gender='1', country='2')
  [END enrichment_with_vertex_ai]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_vertex_ai_legacy():
  expected = '''[START enrichment_with_vertex_ai_legacy]
Row(entity_id='movie_01', title='The Shawshank Redemption', genres='Drama')
Row(entity_id='movie_02', title='The Shining', genres='Horror')
Row(entity_id='movie_04', title='The Dark Knight', genres='Action')
  [END enrichment_with_vertex_ai_legacy]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_bigquery_storage_basic():
  expected = '''[START enrichment_with_bigquery_storage_basic]
Row(sale_id=1001, product_id=101, customer_id=501, quantity=2, product_id=101, product_name='Laptop Pro', category='Electronics', unit_price=999.99)
Row(sale_id=1002, product_id=102, customer_id=502, quantity=1, product_id=102, product_name='Wireless Mouse', category='Electronics', unit_price=29.99)
Row(sale_id=1003, product_id=103, customer_id=503, quantity=5, product_id=103, product_name='Office Chair', category='Furniture', unit_price=199.99)
  [END enrichment_with_bigquery_storage_basic]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_bigquery_storage_column_aliasing():
  expected = '''[START enrichment_with_bigquery_storage_column_aliasing]
Row(transaction_id='TXN-001', customer_id='C123', amount=150.0, customer_id='C123', customer_name='John Smith', contact_email='john.smith@email.com', membership_level='Premium', member_since='2023-01-15')
Row(transaction_id='TXN-002', customer_id='C124', amount=75.5, customer_id='C124', customer_name='Jane Doe', contact_email='jane.doe@email.com', membership_level='Standard', member_since='2023-03-20')
Row(transaction_id='TXN-003', customer_id='C125', amount=200.0, customer_id='C125', customer_name='Bob Johnson', contact_email='bob.johnson@email.com', membership_level='Premium', member_since='2022-11-10')
  [END enrichment_with_bigquery_storage_column_aliasing]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_bigquery_storage_multiple_fields():
  expected = '''[START enrichment_with_bigquery_storage_multiple_fields]
Row(product_id='PROD-001', warehouse_id='WH-NY', date='2024-01-15', product_id='PROD-001', warehouse_id='WH-NY', date='2024-01-15', current_stock=150, reserved_stock=25, available_stock=125, last_updated='2024-01-15T08:30:00')
Row(product_id='PROD-002', warehouse_id='WH-CA', date='2024-01-15', product_id='PROD-002', warehouse_id='WH-CA', date='2024-01-15', current_stock=75, reserved_stock=10, available_stock=65, last_updated='2024-01-15T09:15:00')
Row(product_id='PROD-001', warehouse_id='WH-TX', date='2024-01-15', product_id='PROD-001', warehouse_id='WH-TX', date='2024-01-15', current_stock=200, reserved_stock=30, available_stock=170, last_updated='2024-01-15T10:00:00')
  [END enrichment_with_bigquery_storage_multiple_fields]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_bigquery_storage_batched():
  # For batched processing, we expect a count output
  expected = '''[START enrichment_with_bigquery_storage_batched]
1000
  [END enrichment_with_bigquery_storage_batched]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_bigquery_storage_custom_function():
  expected = '''[START enrichment_with_bigquery_storage_custom_function]
Row(user_id='U123', event_type='purchase', session_id='S456', timestamp='2024-01-15T10:30:00', user_id='U123', user_name='Alice Johnson', subscription_tier='Premium', last_login='2024-01-14T18:22:00', total_purchases=25)
Row(user_id='U124', event_type='view', session_id='S457', timestamp='2024-01-15T11:15:00', user_id='U124', user_name='Bob Smith', subscription_tier='Standard', last_login='2024-01-15T09:45:00', total_purchases=8)
Row(user_id='U125', event_type='purchase', session_id='S458', timestamp='2024-01-15T12:00:00', user_id='U125', user_name='Carol Davis', subscription_tier='Premium', last_login='2024-01-15T11:30:00', total_purchases=42)
  [END enrichment_with_bigquery_storage_custom_function]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_bigquery_storage_performance_tuned():
  # For performance tuned example, we expect account type summaries
  expected = '''[START enrichment_with_bigquery_storage_performance_tuned]
('Savings', 3333)
('Checking', 3334)
('Business', 3333)
  [END enrichment_with_bigquery_storage_performance_tuned]'''.splitlines()[1:-1]
  return expected


@mock.patch('sys.stdout', new_callable=StringIO)
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
      self.assertEqual(set(output[i].split(',')), set(expected[i].split(',')))

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
    self.assertEqual(output, expected)

  def test_enrichment_with_bigquery_storage_batched(self, mock_stdout):
    enrichment_with_bigquery_storage_batched()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigquery_storage_batched()
    self.assertEqual(output, expected)

  def test_enrichment_with_bigquery_storage_column_aliasing(self, mock_stdout):
    enrichment_with_bigquery_storage_column_aliasing()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigquery_storage_column_aliasing()
    self.assertEqual(output, expected)

  def test_enrichment_with_bigquery_storage_multiple_fields(self, mock_stdout):
    enrichment_with_bigquery_storage_multiple_fields()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigquery_storage_multiple_fields()
    self.assertEqual(output, expected)

  def test_enrichment_with_bigquery_storage_custom_function(self, mock_stdout):
    enrichment_with_bigquery_storage_custom_function()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigquery_storage_custom_function()
    self.assertEqual(output, expected)

  def test_enrichment_with_bigquery_storage_performance_tuned(
      self, mock_stdout):
    enrichment_with_bigquery_storage_performance_tuned()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigquery_storage_performance_tuned()
    self.assertEqual(output, expected)


if __name__ == '__main__':
  unittest.main()
