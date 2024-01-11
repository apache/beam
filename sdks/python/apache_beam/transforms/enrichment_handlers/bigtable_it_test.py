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

import typing
import unittest

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.bigtable import EnrichWithBigTable


class _Currency(typing.NamedTuple):
  s_id: int
  id: str


class TestBigTableEnrichment(unittest.TestCase):
  def __init__(self):
    self.project_id = 'google.com:clouddfe'
    self.instance_id = 'beam-test'
    self.table_id = 'riteshghorse-test'
    self.req = {'s_id': 1, 'id': 'usd'}
    self.row_key = 'id'

  def test_enrichment_with_bigtable(self):
    column_family_ids = ['test-column']
    column_ids = ['id', 'value']
    bigtable = EnrichWithBigTable(
        self.project_id,
        self.instance_id,
        self.table_id,
        self.row_key,
        column_family_ids,
        column_ids)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create([self.req])
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | 'Write' >> WriteToText('1enrich.txt'))

  def test_enrichment_with_bigtable_no_column_family(self):
    column_ids = ['id', 'value']
    bigtable = EnrichWithBigTable(
        self.project_id,
        self.instance_id,
        self.table_id,
        self.row_key,
        column_ids=column_ids)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create([self.req])
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | 'Write' >> WriteToText('1enrich.txt'))

  def test_enrichment_with_bigtable_no_column_ids(self):
    column_family_ids = ['test-column']
    bigtable = EnrichWithBigTable(
        self.project_id,
        self.instance_id,
        self.table_id,
        self.row_key,
        column_family_ids=column_family_ids)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create([self.req])
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | 'Write' >> WriteToText('2enrich.txt'))

  def test_enrichment_with_bigtable_no_hints(self):
    req = {'s_id': 1, 'id': 'usd'}
    bigtable = EnrichWithBigTable(
        self.project_id, self.instance_id, self.table_id, self.row_key)
    with TestPipeline(is_integration_test=True) as test_pipeline:
      _ = (
          test_pipeline
          | "Create" >> beam.Create([req])
          | "Enrich W/ BigTable" >> Enrichment(bigtable)
          | 'Write' >> WriteToText('3enrich.txt'))


if __name__ == '__main__':
  unittest.main()
