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
from unittest import mock

from parameterized import parameterized

import apache_beam as beam

try:
  from apache_beam.transforms.enrichment_handlers.bigtable import BigTableEnrichmentHandler
  from apache_beam.transforms.enrichment_handlers.utils import ExceptionLevel
except ImportError:
  raise unittest.SkipTest('Bigtable test dependencies are not installed.')


def _row_key_fn(request: beam.Row) -> bytes:
  return str(request.product_id).encode('utf-8')  # type: ignore[attr-defined]


class TestBigTableEnrichmentHandler(unittest.TestCase):
  @parameterized.expand([('product_id', _row_key_fn), ('', None)])
  def test_bigtable_enrichment_invalid_args(self, row_key, row_key_fn):
    with self.assertRaises(ValueError):
      _ = BigTableEnrichmentHandler(
          project_id='apache-beam-testing',
          instance_id='beam-test',
          table_id='bigtable-enrichment-test',
          row_key=row_key,
          row_key_fn=row_key_fn)

  def _handler(self, exception_level=ExceptionLevel.WARN):
    handler = BigTableEnrichmentHandler(
        project_id='p',
        instance_id='i',
        table_id='t',
        row_key='user_id',
        exception_level=exception_level)
    handler._table = mock.MagicMock()
    handler._table.read_row.return_value = None
    return handler

  def test_none_row_key_skips_read_warn(self):
    handler = self._handler(ExceptionLevel.WARN)
    with self.assertLogs(level='WARNING') as logs:
      request, response = handler(beam.Row(user_id=None))
    handler._table.read_row.assert_not_called()
    self.assertEqual(request, beam.Row(user_id=None))
    self.assertEqual(response, beam.Row())
    self.assertTrue(any('no matching row found' in msg for msg in logs.output))

  def test_none_row_key_skips_read_quiet(self):
    handler = self._handler(ExceptionLevel.QUIET)
    request, response = handler(beam.Row(user_id=None))
    handler._table.read_row.assert_not_called()
    self.assertEqual(request, beam.Row(user_id=None))
    self.assertEqual(response, beam.Row())

  def test_none_row_key_raises(self):
    handler = self._handler(ExceptionLevel.RAISE)
    with self.assertRaisesRegex(ValueError, 'no matching row found'):
      handler(beam.Row(user_id=None))
    handler._table.read_row.assert_not_called()

  def test_missing_row_key_field_message(self):
    handler = self._handler()
    with self.assertRaisesRegex(KeyError, r"row_key user_id not found"):
      handler(beam.Row(other_col='x'))
    handler._table.read_row.assert_not_called()

  def test_row_key_fn_keyerror_not_rewritten(self):
    def bad_fn(request):
      raise KeyError('from-custom-fn')

    handler = BigTableEnrichmentHandler(
        project_id='p', instance_id='i', table_id='t', row_key_fn=bad_fn)
    handler._table = mock.MagicMock()
    with self.assertRaises(KeyError) as ctx:
      handler(beam.Row(user_id=1))
    self.assertEqual(str(ctx.exception), "'from-custom-fn'")
    handler._table.read_row.assert_not_called()

  def test_get_cache_key_distinguishes_none_from_string(self):
    handler = self._handler()
    self.assertEqual(
        handler.get_cache_key(beam.Row(user_id=None)), 'user_id: None')
    self.assertEqual(
        handler.get_cache_key(beam.Row(user_id='None')), "user_id: 'None'")
    self.assertNotEqual(
        handler.get_cache_key(beam.Row(user_id=None)),
        handler.get_cache_key(beam.Row(user_id='None')))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
