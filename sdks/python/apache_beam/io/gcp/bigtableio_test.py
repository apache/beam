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

"""Unittest for GCP Bigtable Split testing."""
from __future__ import absolute_import
from __future__ import division

import logging
import sys
import unittest
import mock

from beam_bigtable import BigtableSource

try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.table import Table
  from google.cloud.bigtable.row_data import PartialRowData
  # from google.cloud.bigtable_v2.types import SampleRowKeysResponse
  from google.cloud.bigtable_v2.proto.bigtable_pb2 import SampleRowKeysResponse
  from google.cloud.bigtable.row_set import RowRange
  from google.cloud.bigtable.row_set import RowSet
except ImportError:
  Client = None
  Table = None
  PartialRowData = None
  SampleRowKeysResponse = None
  RowRange = None
  RowSet = None

KEYS_1 = [b'beam_key0672496', b'beam_key1582279', b'beam_key22',
          b'beam_key2874203', b'beam_key3475534', b'beam_key4440786',
          b'beam_key51', b'beam_key56', b'beam_key65', b'beam_key7389168',
          b'beam_key8105103', b'beam_key9007992', b'']

KEYS_2 = [b'beam_key0952711', b'beam_key2', b'beam_key2797065',
          b'beam_key3518235', b'beam_key41', b'beam_key4730550',
          b'beam_key54', b'beam_key6404724', b'beam_key7123742',
          b'beam_key7683967', b'beam_key83', b'beam_key8892594',
          b'beam_key943', b'']

RANGES_DICT = {
  '': (0, 1),
  'beam_key0952711': (952711, 952712),
  'beam_key2': (2000000, 2000001),
  'beam_key2797065': (2797065, 2797066),
  'beam_key3518235': (3518235, 3518236),
  'beam_key41': (4100000, 4100001),
  'beam_key4730550': (4730550, 4730551),
  'beam_key54': (5400000, 5400001),
  'beam_key6404724': (6404724, 6404725),
  'beam_key7123742': (7123742, 7123743),
  'beam_key7683967': (7683967, 7683968),
  'beam_key83': (8300000, 8300001),
  'beam_key8892594': (8892594, 8892595),
  'beam_key943': (9430000, 9430001),
}

SIZE_768M = 805306368
SIZE_9984M = 10468982784

@unittest.skipIf(Table is None, 'GCP Bigtable dependencies are not installed')
@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class BigtableSourceTest(unittest.TestCase):
  def setUp(self):
    self.project_id = 'project_id'
    self.instance_id = 'instance_id'
    self.table_id = 'table_id'

    if not hasattr(self, 'client'):
      self.client = Client(project=self.project_id, admin=True)
      self.instance = self.client.instance(self.instance_id)
      self.table = self.instance.table(self.table_id)

  def _mock_sample_keys(self, keys=None):
    if keys is None:
      sample_row = SampleRowKeysResponse()
      sample_row.row_key = b''
      sample_row.offset_bytes = SIZE_768M
      yield sample_row
      return

    for i, key in enumerate(keys):
      sample_row = SampleRowKeysResponse()
      sample_row.row_key = key
      sample_row.offset_bytes = (i + 1) * SIZE_768M
      yield sample_row

  @mock.patch.object(BigtableSource, 'get_sample_row_keys')
  def test_estimate_size(self, mock_sample_row_keys):
    mock_sample_row_keys.return_value = self._mock_sample_keys(KEYS_1)
    self.assertTrue(mock_sample_row_keys)
    self.assertEqual(BigtableSource(self.project_id, self.instance_id, self.table_id)
                     .estimate_size(), SIZE_768M * len(KEYS_1))

  @mock.patch.object(BigtableSource, 'get_sample_row_keys')
  def test_get_range_tracker(self, mock_sample_row_keys):
    mock_sample_row_keys.return_value = self._mock_sample_keys(KEYS_1)
    pos_start = b'beam_key0672496'
    pos_stop = b'beam_key1582279'
    source = BigtableSource(self.project_id, self.instance_id, self.table_id)
    range_tracker = source.get_range_tracker(pos_start, pos_stop)
    self.assertEqual(range_tracker.start_position(), pos_start)
    self.assertEqual(range_tracker.stop_position(), pos_stop)

  @mock.patch.object(BigtableSource, 'get_sample_row_keys')
  def test_split(self, mock_sample_row_keys):
    mock_sample_row_keys.return_value = self._mock_sample_keys(KEYS_1)
    split_list = list(BigtableSource(self.project_id, self.instance_id, self.table_id).split())
    self.assertEqual(len(split_list), len(KEYS_1))

  def _key_bytes(self, key):
    return bytes(key) if sys.version_info < (3, 0) else bytes(key, 'utf8')

  # def __read_list_rebalancing(self):
  #   for i in range(35000, 1214999):
  #     yield PartialRowData(self._key_bytes("beam_key%07d" % i))

  @mock.patch.object(Table, 'read_rows')
  def test_read(self, mock_read_rows):

    pos_start = 672496
    row_count = 400

    def _mock_read_list():
      for i in range(pos_start, pos_start + row_count):
        yield PartialRowData(self._key_bytes('beam_key%07d' % i))

    mock_read_rows.return_value = _mock_read_list()
    bigtable = BigtableSource(self.project_id, self.instance_id, self.table_id)
    rows = list(bigtable.read(bigtable.get_range_tracker()))
    self.assertEqual(len(rows), row_count)
    for row in rows:
      self.assertIsInstance(row, PartialRowData)
      self.assertNotEqual(row.row_key, b'')

  @mock.patch.object(BigtableSource, 'get_sample_row_keys')
  @mock.patch.object(Table, 'read_rows')
  def test_read_small_table(self, mock_read_rows, mock_sample_row_keys):
    # def mocking_sample_row_keys():
    #   sample_row = SampleRowKeysResponse()
    #   sample_row.row_key = b''
    #   sample_row.offset_bytes = SIZE_768M
    #   return [sample_row]

    row_count = 10000

    def _mock_read_rows(): # 12.2 KB
      for i in range(0, row_count):
        yield PartialRowData(self._key_bytes('beam_key%07d' % i))

    # mock_sample_row_keys.return_value = mocking_sample_row_keys()
    mock_sample_row_keys.return_value = self._mock_sample_keys()
    mock_read_rows.return_value = _mock_read_rows()
    bigtable = BigtableSource(self.project_id, self.instance_id, self.table_id)

    for split_bundle in bigtable.split():
      range_tracker = bigtable.get_range_tracker(split_bundle.start_position, split_bundle.stop_position)
      rows = list(bigtable.read(range_tracker))
      self.assertEqual(len(rows), row_count)

      for row in rows:
        self.assertIsInstance(row, PartialRowData)

  def _mocking_read_rows(self, **kwargs):  # 12.2 KB
    current_range = RANGES_DICT[kwargs['start_key']]
    # print current_range[1] - current_range[0]
    for i in range(current_range[0], current_range[1]):
      yield PartialRowData(self._key_bytes("beam_key%07d" % i))

  @mock.patch.object(BigtableSource, 'get_sample_row_keys')
  @mock.patch.object(Table, 'read_rows')
  def test_read_table(self, mock_read_rows, mock_sample_row_keys):
    mock_sample_row_keys.return_value = self._mock_sample_keys(KEYS_2)
    mock_read_rows.side_effect = self._mocking_read_rows
    bigtable = BigtableSource(self.project_id, self.instance_id, self.table_id)

    # TODO: Need to implement mock-reader and row counter

    # add = []
    splits = []
    # counter = 0
    for split in bigtable.split():
      # add.append(len(list(bigtable.read(bigtable.get_range_tracker(split.start_position, split.stop_position)))))
      # for row in list(bigtable.read(bigtable.get_range_tracker(split.start_position, split.stop_position))):
      #   counter += 1
      splits.append(split)
    # print "row count = ", counter

    # print len(splits)

    # self.assertEqual(sum(add), len(KEYS_2)) # Create One Row for each Bundle in the read_rows method.
    self.assertEqual(len(splits), len(KEYS_2)) # Create One Row for each Bundle in the read_rows method.


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
