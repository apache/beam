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
from typing import Dict
from typing import List
from typing import NamedTuple

from google.cloud.bigtable.row_filters import ColumnRangeFilter

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import BeamAssertException
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigtable import EnrichWithBigTable


class ValidateResponse(beam.DoFn):
  """ValidateResponse validates if a PCollection of `beam.Row`
  has the required fields."""
  def __init__(
      self,
      n_fields: int,
      fields: List[str],
      enriched_fields: Dict[str, List[str]]):
    self.n_fields = n_fields
    self._fields = fields
    self._enriched_fields = enriched_fields

  def process(self, element: beam.Row, *args, **kwargs):
    element_dict = element.as_dict()
    if len(element_dict.keys()) != self.n_fields:
      raise BeamAssertException(
          "Expected %d fields in enriched PCollection:" % self.n_fields)

    for field in self._fields:
      if field not in element_dict or element_dict[field] is None:
        raise BeamAssertException(f"Expected a not None field: {field}")

    for column_family, columns in self._enriched_fields.items():
      if not all(key in element_dict[column_family] for key in columns):
        raise BeamAssertException(
            "Response from bigtable should contain a %s column_family with "
            "%s keys." % (column_family, columns))


class _Currency(NamedTuple):
  s_id: int
  id: str


class TestBigTableEnrichment(unittest.TestCase):
  def setUp(self):
    self.project_id = 'google.com:clouddfe'
    self.instance_id = 'beam-test'
    self.table_id = 'riteshghorse-bigtable-test'
    self.req = [
        beam.Row(sale_id=1, customer_id=1, product_id=1, quantity=1),
        beam.Row(sale_id=3, customer_id=3, product_id=2, quantity=3),
        beam.Row(sale_id=5, customer_id=5, product_id=4, quantity=2),
        beam.Row(sale_id=7, customer_id=7, product_id=1, quantity=1),
    ]
    self.row_key = 'product_id'
    self.column_family_id = 'product'

  def test_enrichment_with_bigtable(self):
    expected_fields = [
        'sale_id', 'customer_id', 'product_id', 'quantity', 'product'
    ]
    expected_enriched_fields = {
        'product': ['product_id', 'product_name', 'product_stock'],
    }
    bigtable = EnrichWithBigTable(
        self.project_id, self.instance_id, self.table_id, self.row_key)
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
    bigtable = EnrichWithBigTable(
        self.project_id,
        self.instance_id,
        self.table_id,
        self.row_key,
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


if __name__ == '__main__':
  unittest.main()
