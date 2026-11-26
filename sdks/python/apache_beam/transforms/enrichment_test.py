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

  def test_exception_handling_routes_rpc_failures(self):
    handler = _StaticDictHandler(
        {1: {
            'value': 'durham', 'cost': 9.5
        }},
        timeout_keys={2},
    )
    inputs = [beam.Row(id=1), beam.Row(id=2)]
    with beam.Pipeline() as p:
      result = (
          p
          | beam.Create(inputs)
          | Enrichment(
              handler,
              repeater=NoOpsRepeater(),
          ).with_exception_handling('good', 'failed_enrichment'))
      assert_that(
          result['good'] | beam.Map(lambda r: r._asdict()),
          equal_to([{
              'id': 1, 'value': 'durham', 'cost': 9.5
          }]),
          label='CheckGood',
      )
      assert_that(
          result['failed_enrichment'] | beam.Map(lambda x: x[0]._asdict()),
          equal_to([{
              'id': 2
          }]),
          label='CheckDLQ',
      )

  def test_exception_handling_routes_join_failures(self):
    def _failing_join(left, right):
      if left.get('id') == 1:
        raise RuntimeError('join failure')
      return cross_join(left, right)

    handler = _StaticDictHandler({1: {'value': 'ok'}, 2: {'value': 'ok'}})
    with beam.Pipeline() as p:
      res = (
          p
          | beam.Create([beam.Row(id=1), beam.Row(id=2)])
          | Enrichment(
              handler,
              join_fn=_failing_join,
          ).with_exception_handling(main_tag='good', dead_letter_tag='errors'))
      assert_that(
          res['good'] | beam.Map(lambda r: r._asdict()),
          equal_to([{
              'id': 2, 'value': 'ok'
          }]),
          label='CheckGoodJoin',
      )
      assert_that(
          res['errors']
          | beam.Map(
              lambda x: x[0][0]._asdict()
              if isinstance(x[0], tuple) else x[0]._asdict()),
          equal_to([{
              'id': 1
          }]),
          label='CheckJoinDLQ',
      )


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
