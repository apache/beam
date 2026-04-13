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
from unittest import mock

from parameterized import parameterized

import apache_beam as beam

# pylint: disable=ungrouped-imports
try:
  from apache_beam.transforms.enrichment_handlers.bigquery import BigQueryEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.bigquery_it_test import condition_value_fn
  from apache_beam.transforms.enrichment_handlers.bigquery_it_test import query_fn
except ImportError:
  raise unittest.SkipTest(
      'Google Cloud BigQuery dependencies are not installed.')


class TestBigQueryEnrichment(unittest.TestCase):
  def setUp(self) -> None:
    self.project = 'apache-beam-testing'

  @parameterized.expand([
      ("", "", [], None, None, 1, 2),
      ("table", "", ["id"], condition_value_fn, None, 2, 10),
      ("table", "id='{}'", ["id"], condition_value_fn, None, 2, 10),
      ("table", "id='{}'", ["id"], None, query_fn, 2, 10),
  ])
  def test_valid_params(
      self,
      table_name,
      row_restriction_template,
      fields,
      condition_value_fn,
      query_fn,
      min_batch_size,
      max_batch_size):
    """
    TC 1: Only batch size are provided. It should raise an error.
    TC 2: Either of `row_restriction template` or `query_fn` is not provided.
    TC 3: Both `fields` and `condition_value_fn` are provided.
    TC 4: Query construction details are provided along with `query_fn`.
    """
    with self.assertRaises(ValueError):
      _ = BigQueryEnrichmentHandler(
          project=self.project,
          table_name=table_name,
          row_restriction_template=row_restriction_template,
          fields=fields,
          condition_value_fn=condition_value_fn,
          query_fn=query_fn,
          min_batch_size=min_batch_size,
          max_batch_size=max_batch_size,
      )

  def test_batch_mode_fans_out_response_for_duplicate_keys(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}'",
        fields=['id'],
        min_batch_size=2,
        max_batch_size=2,
    )
    requests = [beam.Row(id='1', name='first'), beam.Row(id='1', name='second')]

    with mock.patch.object(handler,
                           '_execute_query',
                           return_value=[{'id': '1', 'value': 'enriched'}]):
      responses = handler(requests)

    self.assertEqual(
        responses,
        [
            (requests[0], beam.Row(id='1', value='enriched')),
            (requests[1], beam.Row(id='1', value='enriched')),
        ],
    )

  def test_batch_mode_emits_empty_rows_for_all_unmatched_duplicate_keys(self):
    handler = BigQueryEnrichmentHandler(
        project=self.project,
        table_name='project.dataset.table',
        row_restriction_template="id='{}'",
        fields=['id'],
        min_batch_size=2,
        max_batch_size=2,
        throw_exception_on_empty_results=False,
    )
    requests = [beam.Row(id='1', name='first'), beam.Row(id='1', name='second')]

    with mock.patch.object(handler, '_execute_query', return_value=None):
      responses = handler(requests)

    self.assertEqual(
        responses,
        [(requests[0], beam.Row()), (requests[1], beam.Row())],
    )


if __name__ == '__main__':
  unittest.main()
