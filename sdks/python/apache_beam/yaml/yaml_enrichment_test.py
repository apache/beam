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

import json
import logging
import os
import tempfile
import unittest

import mock

import apache_beam as beam
from apache_beam import Row
from apache_beam.io.requestresponse import UserCodeTimeoutException
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.enrichment import EnrichmentSourceHandler
from apache_beam.yaml import yaml_provider
from apache_beam.yaml.yaml_transform import YamlTransform

try:
  import pyarrow as pa
  import pyarrow.parquet as pq
except ImportError:
  pa = None  # type: ignore[assignment]
  pq = None  # type: ignore[assignment]


class FakeEnrichmentTransform:
  def __init__(self, enrichment_handler, handler_config, timeout=30):
    self._enrichment_handler = enrichment_handler
    self._handler_config = handler_config
    self._timeout = timeout

  def __call__(self, enrichment_handler, *, handler_config, timeout=30):
    assert enrichment_handler == self._enrichment_handler
    assert handler_config == self._handler_config
    assert timeout == self._timeout
    return beam.Map(lambda x: beam.Row(**x._asdict()))


class _TimeoutMockHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
  def __init__(self, join_key: str = 'sku_id', fail_key: str = 'TIMEOUT'):
    self._join_key = join_key
    self._fail_key = fail_key

  def __call__(self, request: beam.Row, *args, **kwargs):
    key = request._asdict()[self._join_key]
    if key == self._fail_key:
      raise UserCodeTimeoutException(f'Timed out fetching {key}')
    return request, beam.Row(category='books', unit_cost=15.0)


class EnrichmentTransformTest(unittest.TestCase):
  def setUp(self):
    self.temp_dir = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp_dir.cleanup)
    yaml_provider.standard_providers.cache_clear()

  def test_enrichment_with_bigquery(self):
    input_data = [
        Row(label="item1", rank=0),
        Row(label="item2", rank=1),
    ]

    handler = 'BigQuery'
    config = {
        "project": "apache-beam-testing",
        "table_name": "project.database.table",
        "row_restriction_template": "label='item1' or label='item2'",
        "fields": ["label"]
    }

    with beam.Pipeline() as p:
      with mock.patch('apache_beam.yaml.yaml_enrichment.enrichment_transform',
                      FakeEnrichmentTransform(enrichment_handler=handler,
                                              handler_config=config)):
        # Force a reload to respect our mock.
        yaml_provider.standard_providers.cache_clear()
        input_pcoll = p | 'CreateInput' >> beam.Create(input_data)
        result = input_pcoll | YamlTransform(
            f'''
                    type: Enrichment
                    config:
                        enrichment_handler: {handler}
                        handler_config: {config}
                    ''')
        assert_that(result, equal_to(input_data))

  def test_yaml_file_enrichment_parquet_dlq_and_prefix(self):
    if pa is None or pq is None:
      self.skipTest('pyarrow is not installed')
    parquet_path = os.path.join(self.temp_dir.name, 'sku_catalog.parquet')
    pq.write_table(
        pa.table({
            'sku_id': ['SKU-1', 'SKU-2'],
            'category': ['electronics', 'books'],
            'unit_cost': [99.5, 14.25],
            'ignored_col': ['x', 'y'],
        }),
        parquet_path,
    )

    input_data = [
        Row(sku_id='SKU-1', qty=2),
        Row(sku_id='SKU-MISSING', qty=1),
    ]

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        ['--yaml_experimental_features=Enrichment'])) as p:
      input_pcoll = p | 'CreateInput' >> beam.Create(input_data)
      result = input_pcoll | YamlTransform(
          f'''
          type: composite
          input:
            events: input
          transforms:
            - type: Enrichment
              name: EnrichEvents
              input: events
              config:
                enrichment_handler: File
                handler_config:
                  path: '{parquet_path}'
                  join_key: sku_id
                  select_fields: [category, unit_cost]
                prefix: sku_
                on_miss: dlq
                error_handling:
                  output: failed_enrichment
          output:
            enriched: EnrichEvents
            dlq: EnrichEvents.failed_enrichment
          ''')
      assert_that(
          result['enriched'],
          equal_to([
              Row(
                  sku_id='SKU-1',
                  qty=2,
                  sku_category='electronics',
                  sku_unit_cost=99.5)
          ]),
          label='CheckEnriched',
      )
      assert_that(
          result['dlq'] | beam.Map(lambda r: r.element),
          equal_to([Row(sku_id='SKU-MISSING', qty=1)]),
          label='CheckDLQ',
      )

  def test_yaml_file_enrichment_csv_on_miss_null_and_drop(self):
    csv_path = os.path.join(self.temp_dir.name, 'catalog.csv')
    with open(csv_path, 'w', encoding='utf-8') as f:
      f.write('sku_id,category,unit_cost\nSKU-1,books,10.0\n')

    input_data = [
        Row(sku_id='SKU-1', qty=1),
        Row(sku_id='SKU-2', qty=3),
    ]

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        ['--yaml_experimental_features=Enrichment'])) as p:
      input_pcoll = p | 'CreateNullInput' >> beam.Create(input_data)
      res_null = input_pcoll | 'EnrichNull' >> YamlTransform(
          f'''
          type: Enrichment
          config:
            enrichment_handler: File
            handler_config:
              path: '{csv_path}'
              join_key: sku_id
              select_fields: [category, unit_cost]
            prefix: sku_
            on_miss: null
          ''')
      assert_that(
          res_null,
          equal_to([
              Row(
                  sku_id='SKU-1',
                  qty=1,
                  sku_category='books',
                  sku_unit_cost=10.0),
              Row(sku_id='SKU-2', qty=3, sku_category=None, sku_unit_cost=None),
          ]),
          label='CheckLeftJoin',
      )

      res_drop = input_pcoll | 'EnrichDrop' >> YamlTransform(
          f'''
          type: Enrichment
          config:
            enrichment_handler: File
            handler_config:
              path: '{csv_path}'
              join_key: sku_id
              select_fields: [category, unit_cost]
            on_miss: drop
          ''')
      assert_that(
          res_drop,
          equal_to([
              Row(sku_id='SKU-1', qty=1, category='books', unit_cost=10.0),
          ]),
          label='CheckInnerJoin',
      )

  def test_yaml_enrichment_rpc_timeout_routed_to_error_handling(self):
    input_data = [
        Row(sku_id='SKU-1'),
        Row(sku_id='TIMEOUT'),
    ]

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        ['--yaml_experimental_features=Enrichment'])) as p:
      with mock.patch('apache_beam.yaml.yaml_enrichment.FileEnrichmentHandler',
                      _TimeoutMockHandler):
        input_pcoll = p | 'CreateInput' >> beam.Create(input_data)
        result = input_pcoll | YamlTransform(
            '''
            type: composite
            input:
              events: input
            transforms:
              - type: Enrichment
                name: EnrichEvents
                input: events
                config:
                  enrichment_handler: File
                  handler_config:
                    join_key: sku_id
                    fail_key: TIMEOUT
                  timeout: 0.01
                  error_handling:
                    output: failed_enrichment
            output:
              enriched: EnrichEvents
              dlq: EnrichEvents.failed_enrichment
            ''')
        assert_that(
            result['enriched'],
            equal_to([Row(sku_id='SKU-1', category='books', unit_cost=15.0)]),
            label='CheckGood',
        )
        assert_that(
            result['dlq'] | beam.Map(lambda r: r.element),
            equal_to([Row(sku_id='TIMEOUT')]),
            label='CheckTimeoutDLQ',
        )

  def test_yaml_enrichment_unknown_and_missing_handler_errors(self):
    from apache_beam.yaml.yaml_enrichment import enrichment_transform
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        ['--yaml_experimental_features=Enrichment'])) as p:
      pcoll = p | beam.Create([Row(id=1)])
      with self.assertRaises(ValueError):
        _ = pcoll | 'Unknown' >> enrichment_transform(
            enrichment_handler='UnknownSource', handler_config={})
      with mock.patch(
          'apache_beam.yaml.yaml_enrichment.BigTableEnrichmentHandler', None):
        with self.assertRaises(ValueError):
          _ = pcoll | 'MissingDep' >> enrichment_transform(
              enrichment_handler='BigTable', handler_config={})


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
