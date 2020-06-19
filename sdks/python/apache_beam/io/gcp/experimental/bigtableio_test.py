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

""" Unit tests for bigtableio."""

# pytype: skip-file

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import unittest

from collections import namedtuple
from mock import patch

# Protect against environments where bigtable library is not available.
try:
  import apache_beam as beam
  from apache_beam.metrics import Metrics
  from google.cloud.bigtable import client
  from google.cloud.bigtable.row_filters import ValueRangeFilter
  from google.cloud.bigtable.table import Table
  from apache_beam.io.gcp.experimental import bigtableio
except (ImportError, TypeError):
  client = None


@unittest.skipIf(client is None, 'Bigtable dependencies are not installed')
class BigtableioTest(unittest.TestCase):
  _PROJECT = 'project'
  _INSTANCE = 'instance'
  _TABLE = 'table'
  _FILTER = ValueRangeFilter()

  def setUp(self):
    self._row_limit = 100000  # 100k, for argument's sake
    self._read_rows = iter(i for i in range(self._row_limit))
    self._row_key_count = 100

  def _make_read_xform(self):
    return bigtableio.ReadFromBigtable(
        self._PROJECT, self._INSTANCE, self._TABLE, self._FILTER)

  def _mock_row_keys(self, key_count):
    key = namedtuple('key', 'row_key')
    return [key(row_key=i) for i in range(key_count)]

  def test_BigtableReadFn_constructor(self):
    counter = Metrics.counter(bigtableio._BigtableReadFn.__class__, 'Rows Read')
    read_fn = bigtableio._BigtableReadFn(
        self._PROJECT, self._INSTANCE, self._TABLE, self._FILTER)

    self.assertEqual(read_fn._options['project_id'], self._PROJECT)
    self.assertEqual(read_fn._options['instance_id'], self._INSTANCE)
    self.assertEqual(read_fn._options['table_id'], self._TABLE)
    self.assertEqual(read_fn._options['filter_'], self._FILTER)
    self.assertEqual(
        read_fn._counter.metric_name.name, counter.metric_name.name)

  def test_BigtableReadFn_start_bundle(self):
    read_fn = bigtableio._BigtableReadFn(
        self._PROJECT, self._INSTANCE, self._TABLE)
    read_fn.start_bundle()

    self.assertEqual(read_fn._table._instance.instance_id, self._INSTANCE)
    self.assertEqual(read_fn._table.table_id, self._TABLE)

  def test_BigtableReadFn_process(self):
    with patch.object(Table, 'read_rows', return_value=self._read_rows):
      read_fn = bigtableio._BigtableReadFn(
          self._PROJECT, self._INSTANCE, self._TABLE)
      read_fn.start_bundle()
      row_count = 0
      for _ in read_fn.process([None, None]):
        row_count += 1
      self.assertEqual(row_count, self._row_limit)

  def test_ReadFromBigtable_constructor(self):
    read_xform = self._make_read_xform()

    self.assertEqual(read_xform._options['project_id'], self._PROJECT)
    self.assertEqual(read_xform._options['instance_id'], self._INSTANCE)
    self.assertEqual(read_xform._options['table_id'], self._TABLE)
    self.assertEqual(read_xform._options['filter_'], self._FILTER)
    self.assertEqual(read_xform._keys, [])

  def test_ReadFromBigtable__chunks(self):
    row_keys = self._mock_row_keys(self._row_key_count)
    read_xform = self._make_read_xform()
    read_xform._keys = row_keys
    chunks = read_xform._chunks()
    self.assertEqual(len(list(chunks)), len(row_keys) - 1)
    for i, key_pair in enumerate(chunks):
      self.assertEqual(key_pair[0], row_keys[i].row_key)
      self.assertEqual(key_pair[1], row_keys[i + 1].row_key)

  def test_ReadFromBigtable_expand(self):
    key_list = self._mock_row_keys(self._row_key_count)
    with patch.object(beam.ptransform.PTransform, '__init__',
                      return_value=None):
      read_xform = self._make_read_xform()
      with patch.object(beam.ptransform.PTransform,
                        '__ror__',
                        return_value=None):
        with patch.object(Table, 'sample_row_keys', return_value=[]):
          with self.assertRaises(ValueError):
            read_xform.expand(None)
        with patch.object(Table, 'sample_row_keys', return_value=key_list):
          read_xform.expand(None)
          self.assertEqual(len(read_xform._keys), len(key_list) + 1)
          self.assertEqual(read_xform._keys[0], (b'', 0))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.DEBUG)
  unittest.main()
