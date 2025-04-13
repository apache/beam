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

import os
import unittest
from io import StringIO

import mock
import pytest

# pylint: disable=unused-import
try:
  from sqlalchemy import Column, Integer, String, Engine
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
      enrichment_with_bigtable, enrichment_with_vertex_ai_legacy)
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
      enrichment_with_vertex_ai, enrichment_with_cloudsql)
  from apache_beam.transforms.enrichment_handlers.cloudsql_it_test import (
      CloudSQLEnrichmentTestHelper, SQLDBContainerInfo)
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


def validate_enrichment_with_cloudsql():
  expected = '''[START enrichment_with_cloudsql]
Row(product_id=1, name='A', quantity=2, region_id=3)
Row(product_id=2, name='B', quantity=3, region_id=1)
Row(product_id=3, name='C', quantity=10, region_id=4)
  [END enrichment_with_cloudsql]'''.splitlines()[1:-1]
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


@mock.patch('sys.stdout', new_callable=StringIO)
@pytest.mark.uses_testcontainer
class EnrichmentTest(unittest.TestCase):
  def test_enrichment_with_bigtable(self, mock_stdout):
    enrichment_with_bigtable()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigtable()
    self.assertEqual(output, expected)

  def test_enrichment_with_cloudsql(self, mock_stdout):
    db, engine = None, None
    try:
      db, engine = self.pre_cloudsql_enrichment_test()
      enrichment_with_cloudsql()
      output = mock_stdout.getvalue().splitlines()
      expected = validate_enrichment_with_cloudsql()
      self.assertEqual(output, expected)
    except Exception as e:
      self.fail(f"Test failed with unexpected error: {e}")
    finally:
      if db and engine:
        self.post_cloudsql_enrichment_test(db, engine)

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

  def pre_cloudsql_enrichment_test(self):
    table_id = "products"
    columns = [
        Column("product_id", Integer, primary_key=True),
        Column("name", String, nullable=False),
        Column("quantity", Integer, nullable=False),
        Column("region_id", Integer, nullable=False),
    ]
    table_data = [
        {
            "product_id": 1, "name": "A", 'quantity': 2, 'region_id': 3
        },
        {
            "product_id": 2, "name": "B", 'quantity': 3, 'region_id': 1
        },
        {
            "product_id": 3, "name": "C", 'quantity': 10, 'region_id': 4
        },
    ]
    db = CloudSQLEnrichmentTestHelper.start_sql_db_container()
    os.environ['SQL_DB_TYPE'] = db.adapter.name
    os.environ['SQL_DB_ADDRESS'] = db.address
    os.environ['SQL_DB_USER'] = db.user
    os.environ['SQL_DB_PASSWORD'] = db.password
    os.environ['SQL_DB_ID'] = db.id
    os.environ['SQL_DB_URL'] = db.url
    engine = CloudSQLEnrichmentTestHelper.create_table(
        db_url=os.environ.get("SQL_DB_URL"),
        table_id=table_id,
        columns=columns,
        table_data=table_data)
    return db, engine

  def post_cloudsql_enrichment_test(
      self, db: SQLDBContainerInfo, engine: Engine):
    engine.dispose(close=True)
    CloudSQLEnrichmentTestHelper.stop_sql_db_container(db.container)
    os.environ.pop('SQL_DB_TYPE', None)
    os.environ.pop('SQL_DB_ADDRESS', None)
    os.environ.pop('SQL_DB_USER', None)
    os.environ.pop('SQL_DB_PASSWORD', None)
    os.environ.pop('SQL_DB_ID', None)
    os.environ.pop('SQL_DB_URL', None)


if __name__ == '__main__':
  unittest.main()
