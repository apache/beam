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

import logging
import unittest

import apache_beam as beam
from apache_beam.io.requestresponse import NoOpsRepeater
from apache_beam.io.requestresponse import UserCodeTimeoutException
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# pylint: disable=ungrouped-imports
try:
  from apache_beam.transforms.enrichment import Enrichment
  from apache_beam.transforms.enrichment import EnrichmentSourceHandler
  from apache_beam.transforms.enrichment import cross_join
except ImportError:
  raise unittest.SkipTest('RequestResponseIO dependencies are not installed.')


class _StaticDictHandler(EnrichmentSourceHandler[beam.Row, beam.Row]):
  def __init__(self, data: dict, key_field: str = 'id', timeout_keys=()):
    self._data = data
    self._key_field = key_field
    self._timeout_keys = set(timeout_keys)

  def get_output_schema(self):
    return [('value', str), ('cost', float)]

  def __call__(self, request: beam.Row, *args, **kwargs):
    key = request._asdict()[self._key_field]
    if key in self._timeout_keys:
      raise UserCodeTimeoutException(f'Simulated RPC timeout for key={key}')
    matched = self._data.get(key)
    if matched is None:
      return request, beam.Row()
    return request, beam.Row(**matched)


class TestEnrichmentTransform(unittest.TestCase):
  def test_cross_join(self):
    left = {'id': 1, 'key': 'city'}
    right = {'id': 1, 'value': 'durham'}
    expected = beam.Row(id=1, key='city', value='durham')
    output = cross_join(left, right)
    self.assertEqual(expected, output)

  def test_cross_join_with_prefix(self):
    left = {'id': 1, 'value': 'original'}
    right = {'value': 'durham', 'cost': 10.5}
    expected = beam.Row(
        id=1, value='original', ref_value='durham', ref_cost=10.5)
    output = cross_join(left, right, prefix='ref_')
    self.assertEqual(expected, output)

  def test_invalid_on_miss_raises(self):
    handler = _StaticDictHandler({1: {'value': 'a', 'cost': 1.0}})
    with self.assertRaises(ValueError):
      Enrichment(handler, on_miss='invalid_mode')

  def test_on_miss_dlq_without_exception_handling_raises(self):
    handler = _StaticDictHandler({1: {'value': 'a', 'cost': 1.0}})
    with self.assertRaises(ValueError):
      with beam.Pipeline() as p:
        _ = p | beam.Create([beam.Row(id=1)]) | Enrichment(
            handler, on_miss='dlq')

  def test_on_miss_null_left_join_and_prefix(self):
    handler = _StaticDictHandler({1: {'value': 'durham', 'cost': 9.5}})
    inputs = [beam.Row(id=1, value='left1'), beam.Row(id=2, value='left2')]
    expected = [
        beam.Row(id=1, value='left1', sku_value='durham', sku_cost=9.5),
        beam.Row(id=2, value='left2', sku_value=None, sku_cost=None),
    ]
    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create(inputs)
          | Enrichment(handler, on_miss='null', prefix='sku_'))
      assert_that(res, equal_to(expected))

  def test_on_miss_drop_inner_join(self):
    handler = _StaticDictHandler({1: {'value': 'durham', 'cost': 9.5}})
    inputs = [beam.Row(id=1), beam.Row(id=2)]
    expected = [beam.Row(id=1, value='durham', cost=9.5)]
    with beam.Pipeline() as p:
      res = p | beam.Create(inputs) | Enrichment(handler, on_miss='drop')
      assert_that(res, equal_to(expected))

  def test_on_miss_fail_raises(self):
    handler = _StaticDictHandler({1: {'value': 'durham', 'cost': 9.5}})
    inputs = [beam.Row(id=1), beam.Row(id=2)]
    with self.assertRaises(Exception):
      with beam.Pipeline() as p:
        _ = p | beam.Create(inputs) | Enrichment(handler, on_miss='fail')

  def test_on_miss_dlq_and_rpc_timeout_routing(self):
    handler = _StaticDictHandler(
        {1: {
            'value': 'durham', 'cost': 9.5
        }},
        timeout_keys={3},
    )
    inputs = [beam.Row(id=1), beam.Row(id=2), beam.Row(id=3)]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(inputs)
          | Enrichment(
              handler,
              on_miss='dlq',
              repeater=NoOpsRepeater(),
          ).with_exception_handling(
              main_tag='good', dead_letter_tag='failed_enrichment'))
      assert_that(
          result['good'],
          equal_to([beam.Row(id=1, value='durham', cost=9.5)]),
          label='CheckGood',
      )
      assert_that(
          result['failed_enrichment'] | beam.Map(lambda x: x[0]),
          equal_to([beam.Row(id=2), beam.Row(id=3)]),
          label='CheckDLQ',
      )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
