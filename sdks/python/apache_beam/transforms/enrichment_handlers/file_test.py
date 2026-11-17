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
import os
import tempfile
import unittest
from typing import Any
from typing import Optional

import apache_beam as beam
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.enrichment import Enrichment
from apache_beam.transforms.enrichment_handlers.file import FileEnrichmentHandler
from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
from apache_beam.typehints.row_type import RowTypeConstraint

try:
  import pyarrow as pa
  import pyarrow.parquet as pq
except ImportError:
  pa = None  # type: ignore[assignment]
  pq = None  # type: ignore[assignment]


class TestFileEnrichmentHandler(unittest.TestCase):
  def setUp(self):
    self.temp_dir = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp_dir.cleanup)

  def _write_csv(self, filename: str, content: str) -> str:
    path = os.path.join(self.temp_dir.name, filename)
    with open(path, 'w', encoding='utf-8') as f:
      f.write(content)
    return path

  def _write_jsonl(self, filename: str, rows: list[dict]) -> str:
    path = os.path.join(self.temp_dir.name, filename)
    with open(path, 'w', encoding='utf-8') as f:
      for r in rows:
        f.write(json.dumps(r) + '\n')
    return path

  def _write_json(self, filename: str, rows: list[dict]) -> str:
    path = os.path.join(self.temp_dir.name, filename)
    with open(path, 'w', encoding='utf-8') as f:
      json.dump(rows, f)
    return path

  def _write_parquet(self, filename: str, data: dict) -> str:
    if pa is None or pq is None:
      self.skipTest('pyarrow is not installed')
    path = os.path.join(self.temp_dir.name, filename)
    table = pa.table(data)
    pq.write_table(table, path)
    return path

  def test_validation_errors(self):
    with self.assertRaises(ValueError):
      FileEnrichmentHandler(path='', join_key='sku_id')
    with self.assertRaises(ValueError):
      FileEnrichmentHandler(path='catalog.csv')
    with self.assertRaises(ValueError):
      FileEnrichmentHandler(path='catalog.unknown', join_key='sku_id')

  def test_parquet_enrichment_with_select_fields_and_schema(self):
    path = self._write_parquet(
        'sku_catalog.parquet',
        {
            'sku_id': ['SKU-1', 'SKU-2', 'SKU-3'],
            'category': ['electronics', 'books', 'apparel'],
            'unit_cost': [199.5, 12.0, 35.25],
            'warehouse': ['US-EAST', 'US-WEST', 'EU-CENTRAL'],
        },
    )
    handler = FileEnrichmentHandler(
        path=path,
        join_key='sku_id',
        select_fields=['category', 'unit_cost'],
    )
    self.assertEqual(
        handler.get_output_schema(),
        [('category', str), ('unit_cost', float)],
    )

    inputs = [
        beam.Row(sku_id='SKU-1', qty=2),
        beam.Row(sku_id='SKU-2', qty=1),
    ]
    expected = [
        beam.Row(
            sku_id='SKU-1', qty=2, category='electronics', unit_cost=199.5),
        beam.Row(sku_id='SKU-2', qty=1, category='books', unit_cost=12.0),
    ]
    with beam.Pipeline() as p:
      res = p | beam.Create(inputs) | Enrichment(handler, on_miss='drop')
      self.assertEqual(
          res.element_type,
          RowTypeConstraint.from_fields([
              ('sku_id', str),
              ('qty', int),
              ('category', str),
              ('unit_cost', float),
          ]),
      )
      assert_that(res, equal_to(expected))

  def test_csv_enrichment_type_inference_and_prefix(self):
    path = self._write_csv(
        'merchants.csv',
        'merchant_id,tier,fee_pct,active\n'
        '101,gold,1.5,true\n'
        '102,silver,2.25,false\n',
    )
    handler = FileEnrichmentHandler(path=path, join_key='merchant_id')
    self.assertEqual(
        handler.get_output_schema(),
        [('tier', str), ('fee_pct', float), ('active', bool)],
    )

    inputs = [
        beam.Row(merchant_id=101, tier='event_tier'),
        beam.Row(merchant_id=999, tier='event_tier'),
    ]
    expected = [
        beam.Row(
            merchant_id=101,
            tier='event_tier',
            m_tier='gold',
            m_fee_pct=1.5,
            m_active=True,
        ),
        beam.Row(
            merchant_id=999,
            tier='event_tier',
            m_tier=None,
            m_fee_pct=None,
            m_active=None,
        ),
    ]
    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create(inputs)
          | Enrichment(handler, prefix='m_', on_miss='null'))
      self.assertEqual(
          res.element_type,
          RowTypeConstraint.from_fields([
              ('merchant_id', int),
              ('tier', str),
              ('m_tier', Optional[str]),
              ('m_fee_pct', Optional[float]),
              ('m_active', Optional[bool]),
          ]),
      )
      assert_that(res, equal_to(expected))

  def test_jsonl_and_json_array_enrichment(self):
    jsonl_path = self._write_jsonl(
        'countries.jsonl',
        [
            {
                'code': 'US', 'region': 'NA', 'tax_rate': 0.08
            },
            {
                'code': 'DE', 'region': 'EU', 'tax_rate': 0.19
            },
        ],
    )
    json_path = self._write_json(
        'countries.json',
        [
            {
                'code': 'US', 'region': 'NA', 'tax_rate': 0.08
            },
            {
                'code': 'DE', 'region': 'EU', 'tax_rate': 0.19
            },
        ],
    )
    for path in (jsonl_path, json_path):
      handler = FileEnrichmentHandler(
          path=path,
          join_key='code',
          row_key='country_code',
          select_fields=['region', 'tax_rate'],
      )
      with handler:
        _, resp = handler(beam.Row(country_code='DE'))
        self.assertEqual(resp, beam.Row(region='EU', tax_rate=0.19))

  def test_exception_level_raise(self):
    path = self._write_jsonl(
        'items.jsonl',
        [{
            'id': 1, 'name': 'item1'
        }],
    )
    handler = FileEnrichmentHandler(
        path=path,
        join_key='id',
        exception_level=ExceptionLevel.RAISE,
    )
    with handler:
      with self.assertRaises(ValueError):
        handler(beam.Row(id=999))

  def test_csv_utf8_bom_and_pretty_json(self):
    bom_csv_path = os.path.join(self.temp_dir.name, 'bom_catalog.csv')
    with open(bom_csv_path, 'wb') as f:
      f.write(b'\xef\xbb\xbfsku_id,category\nSKU-1,electronics\n')
    csv_handler = FileEnrichmentHandler(path=bom_csv_path, join_key='sku_id')
    self.assertEqual(csv_handler.get_output_schema(), [('category', str)])
    with csv_handler:
      _, resp = csv_handler(beam.Row(sku_id='SKU-1'))
      self.assertEqual(resp, beam.Row(category='electronics'))

  def test_schema_inference_json_jsonl_and_arrow_types(self):
    jsonl_path = self._write_jsonl(
        'types.jsonl',
        [
            {
                'id': '001',
                'count': 5,
                'flag': True,
                'score': 1.5,
                'tags': ['a', 'b'],
                'empty_col': None,
            },
            {
                'id': '002',
                'count': 10,
                'flag': False,
                'score': 2,
                'tags': ['c'],
                'empty_col': None,
            },
        ],
    )
    jsonl_handler = FileEnrichmentHandler(path=jsonl_path, join_key='id')
    schema = dict(jsonl_handler.get_output_schema())
    self.assertEqual(schema['count'], int)
    self.assertEqual(schema['flag'], bool)
    self.assertEqual(schema['score'], float)

    json_path = self._write_json(
        'single.json',
        [{
            'id': 1, 'label': 'one', 'nullable': None
        }, {
            'id': 2, 'label': 'two', 'nullable': 'val'
        }],
    )
    json_handler = FileEnrichmentHandler(path=json_path, join_key='id')
    self.assertEqual(
        json_handler.get_output_schema(),
        [('label', str), ('nullable', Optional[str])],
    )

    if pa is not None and pq is not None:
      pq_path = self._write_parquet(
          'all_types.parquet',
          {
              'id': [1, 2],
              'flag': [True, False],
              'raw': [b'x', b'y'],
              'items': [[1, 2], [3]],
              'opt_val': [1.0, None],
          },
      )
      pq_handler = FileEnrichmentHandler(path=pq_path, join_key='id')
      pq_schema = dict(pq_handler.get_output_schema())
      self.assertEqual(pq_schema['flag'], bool)
      self.assertEqual(pq_schema['raw'], bytes)
      self.assertEqual(pq_schema['items'], list[int])
      with pq_handler:
        _, resp = pq_handler(beam.Row(id=1))
        self.assertEqual(resp.flag, True)
        self.assertEqual(pq_handler.get_cache_key(beam.Row(id=1)), 'id: 1')

  def test_error_branches_and_warn_level(self):
    with self.assertRaises(ValueError):
      FileEnrichmentHandler(path='no_extension', join_key='id')
    self.assertEqual(
        FileEnrichmentHandler._infer_format('archive.csv.gz'), 'csv')

    missing_handler = FileEnrichmentHandler(
        path=os.path.join(self.temp_dir.name, 'nonexistent.csv'),
        join_key='id',
        select_fields=['name'],
    )
    self.assertEqual(missing_handler.get_output_schema(), [('name', Any)])
    with self.assertRaises(FileNotFoundError):
      missing_handler.__enter__()

    csv_path = self._write_csv('small.csv', 'id,code,val\n1,00123,abc\n')
    bad_key_handler = FileEnrichmentHandler(
        path=csv_path, join_key='missing_col')
    with self.assertRaises(KeyError):
      bad_key_handler.__enter__()

    bad_select_handler = FileEnrichmentHandler(
        path=csv_path, join_key='id', select_fields=['missing_select'])
    with self.assertRaises(KeyError):
      bad_select_handler.__enter__()

    warn_handler = FileEnrichmentHandler(
        path=csv_path,
        join_key='id',
        exception_level=ExceptionLevel.WARN,
    )
    with warn_handler:
      _, miss_resp = warn_handler(beam.Row(id=999))
      self.assertEqual(miss_resp, beam.Row())
      with self.assertRaises(KeyError):
        warn_handler(beam.Row(wrong_field=1))


if __name__ == '__main__':
  unittest.main()
