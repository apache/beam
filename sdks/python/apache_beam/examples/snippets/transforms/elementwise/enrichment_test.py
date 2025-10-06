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
import uuid
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass
from io import StringIO
from typing import Optional

import mock
import pytest
from sqlalchemy.engine import Connection as DBAPIConnection

# pylint: disable=unused-import
try:
  from sqlalchemy import (
      Column, Integer, VARCHAR, Engine, MetaData, create_engine)
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
      enrichment_with_bigtable, enrichment_with_vertex_ai_legacy)
  from apache_beam.examples.snippets.transforms.elementwise.enrichment import (
      enrichment_with_vertex_ai,
      enrichment_with_google_cloudsql_pg,
      enrichment_with_external_pg,
      enrichment_with_external_mysql,
      enrichment_with_external_sqlserver)
  from apache_beam.transforms.enrichment_handlers.cloudsql import (
      DatabaseTypeAdapter)
  from apache_beam.transforms.enrichment_handlers.cloudsql_it_test import (
      SQLEnrichmentTestHelper,
      SQLDBContainerInfo,
      ConnectionConfig,
      CloudSQLConnectionConfig,
      ExternalSQLDBConnectionConfig)
  from apache_beam.io.requestresponse import RequestResponseIO
except ImportError as e:
  raise unittest.SkipTest(f'RequestResponseIO dependencies not installed: {e}')


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


def validate_enrichment_with_google_cloudsql_pg():
  expected = '''[START enrichment_with_google_cloudsql_pg]
Row(product_id=1, name='A', quantity=2, region_id=3)
Row(product_id=2, name='B', quantity=3, region_id=1)
Row(product_id=3, name='C', quantity=10, region_id=4)
  [END enrichment_with_google_cloudsql_pg]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_external_pg():
  expected = '''[START enrichment_with_external_pg]
Row(product_id=1, name='A', quantity=2, region_id=3)
Row(product_id=2, name='B', quantity=3, region_id=1)
Row(product_id=3, name='C', quantity=10, region_id=4)
  [END enrichment_with_external_pg]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_external_mysql():
  expected = '''[START enrichment_with_external_mysql]
Row(product_id=1, name='A', quantity=2, region_id=3)
Row(product_id=2, name='B', quantity=3, region_id=1)
Row(product_id=3, name='C', quantity=10, region_id=4)
  [END enrichment_with_external_mysql]'''.splitlines()[1:-1]
  return expected


def validate_enrichment_with_external_sqlserver():
  expected = '''[START enrichment_with_external_sqlserver]
Row(product_id=1, name='A', quantity=2, region_id=3)
Row(product_id=2, name='B', quantity=3, region_id=1)
Row(product_id=3, name='C', quantity=10, region_id=4)
  [END enrichment_with_external_sqlserver]'''.splitlines()[1:-1]
  return expected


@mock.patch('sys.stdout', new_callable=StringIO)
@pytest.mark.uses_testcontainer
class EnrichmentTest(unittest.TestCase):
  def test_enrichment_with_bigtable(self, mock_stdout):
    enrichment_with_bigtable()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_bigtable()
    self.assertEqual(output, expected)

  def test_enrichment_with_vertex_ai(self, mock_stdout):
    enrichment_with_vertex_ai()
    output = sorted(mock_stdout.getvalue().splitlines())
    expected = sorted(validate_enrichment_with_vertex_ai())

    for i in range(len(expected)):
      self.assertEqual(
          set(output[i][4:-1].split(',')), set(expected[i][4:-1].split(',')))

  def test_enrichment_with_vertex_ai_legacy(self, mock_stdout):
    enrichment_with_vertex_ai_legacy()
    output = mock_stdout.getvalue().splitlines()
    expected = validate_enrichment_with_vertex_ai_legacy()
    self.maxDiff = None
    self.assertEqual(sorted(output), sorted(expected))

  @unittest.skipUnless(
      os.environ.get('ALLOYDB_PASSWORD'),
      "ALLOYDB_PASSWORD environment var is not provided")
  def test_enrichment_with_google_cloudsql_pg(self, mock_stdout):
    db_adapter = DatabaseTypeAdapter.POSTGRESQL
    with EnrichmentTestHelpers.sql_test_context(True, db_adapter):
      try:
        enrichment_with_google_cloudsql_pg()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_google_cloudsql_pg()
        self.assertEqual(output, expected)
      except Exception as e:
        self.fail(f"Test failed with unexpected error: {e}")

  def test_enrichment_with_external_pg(self, mock_stdout):
    db_adapter = DatabaseTypeAdapter.POSTGRESQL
    with EnrichmentTestHelpers.sql_test_context(False, db_adapter):
      try:
        enrichment_with_external_pg()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_external_pg()
        self.assertEqual(output, expected)
      except Exception as e:
        self.fail(f"Test failed with unexpected error: {e}")

  def test_enrichment_with_external_mysql(self, mock_stdout):
    db_adapter = DatabaseTypeAdapter.MYSQL
    with EnrichmentTestHelpers.sql_test_context(False, db_adapter):
      try:
        enrichment_with_external_mysql()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_external_mysql()
        self.assertEqual(output, expected)
      except Exception as e:
        self.fail(f"Test failed with unexpected error: {e}")

  def test_enrichment_with_external_sqlserver(self, mock_stdout):
    db_adapter = DatabaseTypeAdapter.SQLSERVER
    with EnrichmentTestHelpers.sql_test_context(False, db_adapter):
      try:
        enrichment_with_external_sqlserver()
        output = mock_stdout.getvalue().splitlines()
        expected = validate_enrichment_with_external_sqlserver()
        self.assertEqual(output, expected)
      except Exception as e:
        self.fail(f"Test failed with unexpected error: {e}")


@dataclass
class CloudSQLEnrichmentTestDataConstruct:
  client_handler: Callable[[], DBAPIConnection]
  engine: Engine
  metadata: MetaData
  db: SQLDBContainerInfo = None


class EnrichmentTestHelpers:
  @contextmanager
  def sql_test_context(is_cloudsql: bool, db_adapter: DatabaseTypeAdapter):
    result: Optional[CloudSQLEnrichmentTestDataConstruct] = None
    try:
      result = EnrichmentTestHelpers.pre_sql_enrichment_test(
          is_cloudsql, db_adapter)
      yield
    finally:
      if result:
        EnrichmentTestHelpers.post_sql_enrichment_test(result)

  @staticmethod
  def pre_sql_enrichment_test(
      is_cloudsql: bool,
      db_adapter: DatabaseTypeAdapter) -> CloudSQLEnrichmentTestDataConstruct:
    unique_suffix = str(uuid.uuid4())[:8]
    table_id = f"products_{unique_suffix}"
    columns = [
        Column("product_id", Integer, primary_key=True),
        Column("name", VARCHAR(255), nullable=False),
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
    metadata = MetaData()

    connection_config: ConnectionConfig
    db = None
    if is_cloudsql:
      gcp_project_id = "apache-beam-testing"
      region = "us-central1"
      instance_name = "beam-integration-tests"
      instance_connection_uri = f"{gcp_project_id}:{region}:{instance_name}"
      db_id = "postgres"
      user = "postgres"
      password = os.getenv("ALLOYDB_PASSWORD")
      os.environ['GOOGLE_CLOUD_SQL_DB_URI'] = instance_connection_uri
      os.environ['GOOGLE_CLOUD_SQL_DB_ID'] = db_id
      os.environ['GOOGLE_CLOUD_SQL_DB_USER'] = user
      os.environ['GOOGLE_CLOUD_SQL_DB_PASSWORD'] = password
      os.environ['GOOGLE_CLOUD_SQL_DB_TABLE_ID'] = table_id
      connection_config = CloudSQLConnectionConfig(
          db_adapter=db_adapter,
          instance_connection_uri=instance_connection_uri,
          user=user,
          password=password,
          db_id=db_id)
    else:
      db = SQLEnrichmentTestHelper.start_sql_db_container(db_adapter)
      os.environ['EXTERNAL_SQL_DB_HOST'] = db.host
      os.environ['EXTERNAL_SQL_DB_PORT'] = str(db.port)
      os.environ['EXTERNAL_SQL_DB_ID'] = db.id
      os.environ['EXTERNAL_SQL_DB_USER'] = db.user
      os.environ['EXTERNAL_SQL_DB_PASSWORD'] = db.password
      os.environ['EXTERNAL_SQL_DB_TABLE_ID'] = table_id
      connection_config = ExternalSQLDBConnectionConfig(
          db_adapter=db_adapter,
          host=db.host,
          port=db.port,
          user=db.user,
          password=db.password,
          db_id=db.id)

    conenctor = connection_config.get_connector_handler()
    engine = create_engine(
        url=connection_config.get_db_url(), creator=conenctor)

    SQLEnrichmentTestHelper.create_table(
        table_id=table_id,
        engine=engine,
        columns=columns,
        table_data=table_data,
        metadata=metadata)

    result = CloudSQLEnrichmentTestDataConstruct(
        db=db, client_handler=conenctor, engine=engine, metadata=metadata)
    return result

  @staticmethod
  def post_sql_enrichment_test(res: CloudSQLEnrichmentTestDataConstruct):
    # Clean up the data inserted previously.
    res.metadata.drop_all(res.engine)
    res.engine.dispose(close=True)

    # Check if the test used a container-based external SQL database.
    if res.db:
      SQLEnrichmentTestHelper.stop_sql_db_container(res.db)
      os.environ.pop('EXTERNAL_SQL_DB_HOST', None)
      os.environ.pop('EXTERNAL_SQL_DB_PORT', None)
      os.environ.pop('EXTERNAL_SQL_DB_ID', None)
      os.environ.pop('EXTERNAL_SQL_DB_USER', None)
      os.environ.pop('EXTERNAL_SQL_DB_PASSWORD', None)
      os.environ.pop('EXTERNAL_SQL_DB_TABLE_ID', None)
    else:
      os.environ.pop('GOOGLE_CLOUD_SQL_DB_URI', None)
      os.environ.pop('GOOGLE_CLOUD_SQL_DB_ID', None)
      os.environ.pop('GOOGLE_CLOUD_SQL_DB_USER', None)
      os.environ.pop('GOOGLE_CLOUD_SQL_DB_PASSWORD', None)
      os.environ.pop('GOOGLE_CLOUD_SQL_DB_TABLE_ID', None)


if __name__ == '__main__':
  unittest.main()
