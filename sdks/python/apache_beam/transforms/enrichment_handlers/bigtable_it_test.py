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

import datetime
import unittest
from typing import Dict
from typing import List
from typing import NamedTuple
from typing import Tuple

import pytest

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException

# pylint: disable=ungrouped-imports
try:
  from google.api_core.exceptions import NotFound
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.row_filters import ColumnRangeFilter
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.bigtable import ExceptionLevel
except ImportError:
  raise unittest.SkipTest('GCP BigTable dependencies are not installed.')


class ValidateResponse(beam.DoFn):
  """ValidateResponse validates if a PCollection of `beam.Row`
  has the required fields."""
  def __init__(
      self,
      n_fields: int,
      fields: List[str],
      enriched_fields: Dict[str, List[str]],
      include_timestamp: bool = False,
  ):
    self.n_fields = n_fields
    self._fields = fields
    self._enriched_fields = enriched_fields
    self._include_timestamp = include_timestamp

  def process(self, element: beam.Row, *args, **kwargs):
    element_dict = element.as_dict()
    if len(element_dict.keys()) != self.n_fields:
      raise BeamAssertException(
          "Expected %d fields in enriched PCollection:" % self.n_fields)

    for field in self._fields:
      if field not in element_dict or element_dict[field] is None:
        raise BeamAssertException(f"Expected a not None field: {field}")

    for column_family, columns in self._enriched_fields.items():
      if len(element_dict[column_family]) != len(columns):
        raise BeamAssertException(
            "Response from bigtable should contain a %s column_family with "
            "%s keys." % (column_family, columns))

      for key in columns:
        if key not in element_dict[column_family]:
          raise BeamAssertException(
              "Response from bigtable should contain a %s column_family with "
              "%s columns." % (column_family, columns))
        if (self._include_timestamp and
            not isinstance(element_dict[column_family][key][0], Tuple)):  # type: ignore[arg-type]
          raise BeamAssertException(
              "Response from bigtable should contain timestamp associated with "
              "its value.")


class _Currency(NamedTuple):
  s_id: int
  id: str


def create_rows(table):
  product_id = 'product_id'
  product_name = 'product_name'
  product_stock = 'product_stock'

  column_family_id = "product"
  products = [
      {
          'product_id': 1, 'product_name': 'pixel 5', 'product_stock': 2
      },
      {
          'product_id': 2, 'product_name': 'pixel 6', 'product_stock': 4
      },
      {
          'product_id': 3, 'product_name': 'pixel 7', 'product_stock': 20
      },
      {
          'product_id': 4, 'product_name': 'pixel 8', 'product_stock': 10
      },
      {
          'product_id': 5, 'product_name': 'iphone 11', 'product_stock': 3
      },
      {
          'product_id': 6, 'product_name': 'iphone 12', 'product_stock': 7
      },
      {
          'product_id': 7, 'product_name': 'iphone 13', 'product_stock': 8
      },
      {
          'product_id': 8, 'product_name': 'iphone 14', 'product_stock': 3
      },
  ]

  for item in products:
    row_key = str(item[product_id]).encode()
    row = table.direct_row(row_key)
    row.set_cell(
        column_family_id,
        product_id.encode(),
        str(item[product_id]),
        timestamp=datetime.datetime.utcnow())
    row.set_cell(
        column_family_id,
        product_name.encode(),
        item[product_name],
        timestamp=datetime.datetime.utcnow())
    row.set_cell(
        column_family_id,
        product_stock.encode(),
        str(item[product_stock]),
        timestamp=datetime.datetime.utcnow())
    row.commit()


@pytest.mark.it_postcommit
class TestBigTableEnrichment(unittest.TestCase):
  def setUp(self):
    self.project_id = 'apache-beam-testing'
    self.instance_id = 'beam-test'
    self.table_id = 'bigtable-enrichment-test'
    self.req = [
        beam.Row(sale_id=1, customer_id=1, product_id=1, quantity=1),
        beam.Row(sale_id=3, customer_id=3, product_id=2, quantity=3),
        beam.Row(sale_id=5, customer_id=5, product_id=4, quantity=2),
        beam.Row(sale_id=7, customer_id=7, product_id=1, quantity=1),
    ]
    self.row_key = 'product_id'
    self.column_family_id = 'product'
    client = Client(project=self.project_id)
    instance = client.instance(self.instance_id)
    self.table = instance.table(self.table_id)
    create_rows(self.table)

  def tearDown(self) -> None:
    self.table = None

  def test_enrichment_with_bigtable(self):
    expected_fields = [
        'sale_id', 'customer_id', 'product_id', 'quantity', 'product'
    ]
    expected_enriched_fields = {
        'product': ['product_id', 'product_name', 'product_stock'],
    }
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id=self.table_id,
        row_key=self.row_key)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields)))

  def test_enrichment_with_bigtable_row_filter(self):
    expected_fields = [
        'sale_id', 'customer_id', 'product_id', 'quantity', 'product'
    ]
    expected_enriched_fields = {
        'product': ['product_name', 'product_stock'],
    }
    start_column = 'product_name'.encode()
    column_filter = ColumnRangeFilter(self.column_family_id, start_column)
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id=self.table_id,
        row_key=self.row_key,
        row_filter=column_filter)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields)))

  def test_enrichment_with_bigtable_no_enrichment(self):
    # row_key which is product_id=11 doesn't exist, so the enriched field
    # won't be added. Hence, the response is same as the request.
    expected_fields = ['sale_id', 'customer_id', 'product_id', 'quantity']
    expected_enriched_fields = {}
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id=self.table_id,
        row_key=self.row_key)
    req = [beam.Row(sale_id=1, customer_id=1, product_id=11, quantity=1)]
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields)))

  def test_enrichment_with_bigtable_bad_row_filter(self):
    # in case of a bad column filter, that is, incorrect column_family_id and
    # columns, no enrichment is done. If the column_family is correct but not
    # column names then all columns in that column_family are returned.
    start_column = 'car_name'.encode()
    column_filter = ColumnRangeFilter('car_name', start_column)
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id=self.table_id,
        row_key=self.row_key,
        row_filter=column_filter)
    with self.assertRaises(NotFound):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_enrichment_with_bigtable_raises_key_error(self):
    """raises a `KeyError` when the row_key doesn't exist in
    the input PCollection."""
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id=self.table_id,
        row_key='car_name')
    with self.assertRaises(KeyError):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_enrichment_with_bigtable_raises_not_found(self):
    """raises a `NotFound` exception when the GCP BigTable Cluster
    doesn't exist."""
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id='invalid_table',
        row_key=self.row_key)
    with self.assertRaises(NotFound):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_enrichment_with_bigtable_exception_level(self):
    """raises a `ValueError` exception when the GCP BigTable query returns
    an empty row."""
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id=self.table_id,
        row_key=self.row_key,
        exception_level=ExceptionLevel.RAISE)
    req = [beam.Row(sale_id=1, customer_id=1, product_id=11, quantity=1)]
    with self.assertRaises(ValueError):
      test_pipeline = beam.Pipeline()
      _ = (
          test_pipeline
          | "Create" >> beam.Create(req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable))
      res = test_pipeline.run()
      res.wait_until_finish()

  def test_enrichment_with_bigtable_with_timestamp(self):
    """test whether the `(value,timestamp)` is returned when the
    `include_timestamp` is enabled."""
    expected_fields = [
        'sale_id', 'customer_id', 'product_id', 'quantity', 'product'
    ]
    expected_enriched_fields = {
        'product': ['product_id', 'product_name', 'product_stock'],
    }
    bigtable = BigTableEnrichmentHandler(
        project_id=self.project_id,
        instance_id=self.instance_id,
        table_id=self.table_id,
        row_key=self.row_key,
        include_timestamp=True)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create(self.req)
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | "Validate Response" >> beam.ParDo(
              ValidateResponse(
                  len(expected_fields),
                  expected_fields,
                  expected_enriched_fields,
                  include_timestamp=True)))


if __name__ == '__main__':
  unittest.main()
