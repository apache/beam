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

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

# pylint: disable=ungrouped-imports
try:
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment_handlers.bigquery import \
    BigQueryEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.vertex_ai_feature_store_it_test import \
    ValidateResponse
except ImportError:
  raise unittest.SkipTest(
      'Google Cloud BigQuery dependencies are not installed.')


def query_fn(row: beam.Row):
  query = (
      "SELECT * FROM "
      "`google.com:clouddfe.my_ecommerce.product_details`"
      " WHERE id = '{}'".format(row.id))  # type: ignore[attr-defined]
  return query


def condition_value_fn(row: beam.Row):
  return [row.id]  # type: ignore[attr-defined]


class TestBigQueryEnrichmentIT(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'google.com:clouddfe'
    self.query_template = (
        "SELECT * FROM "
        "`google.com:clouddfe.my_ecommerce.product_details`"
        " WHERE id = '{}'")
    self.condition_template = "id = '{}'"
    self.table_name = "`google.com:clouddfe.my_ecommerce.product_details`"

  def test_bigquery_enrichment(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    fields = ['id']
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        fields=fields,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_bigquery_enrichment_with_query_fn(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(project=self.project, query_fn=query_fn)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_bigquery_enrichment_with_condition_value_fn(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        condition_value_fn=condition_value_fn,
        min_batch_size=2,
        max_batch_size=100,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))

  def test_bigquery_enrichment_with_condition_without_batch(self):
    expected_fields = [
        'id', 'name', 'quantity', 'category', 'brand', 'cost', 'retail_price'
    ]
    requests = [
        beam.Row(
            id='13842',
            name='low profile dyed cotton twill cap - navy w39s55d',
            quantity=2),
        beam.Row(
            id='15816',
            name='low profile dyed cotton twill cap - putty w39s55d',
            quantity=1),
    ]
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        row_restriction_template=self.condition_template,
        table_name=self.table_name,
        condition_value_fn=condition_value_fn,
    )
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | beam.Create(requests)
          | Enrichment(handler)
          | beam.ParDo(ValidateResponse(expected_fields)))


if __name__ == '__main__':
  unittest.main()
