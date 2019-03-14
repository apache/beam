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
import uuid

import mock

from apache_beam.io.gcp.bigtableio import _BigTableSource as BigTableSource
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker

# Protect against environments where bigquery library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.table import Table
  from google.cloud.bigtable.row_data import PartialRowData
  from google.cloud.bigtable_v2.types import SampleRowKeysResponse
  from google.cloud.bigtable.row_set import RowRange
except ImportError:
  Client = None
  Table = None


@unittest.skipIf(Table is None, 'GCP Bigtable dependencies are not installed')
@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class BigtableSourceTest(unittest.TestCase):
  def setUp(self):
    DEFAULT_TABLE_PREFIX = "pythonreadtest"

    #self.project_id = 'grass-clump-479'
    self.project_id = 'project_id'
    #self.instance_id = 'python-write-2'
    self.instance_id = 'instance_id'
    #self.table_id = 'testmillion7abb2dc3'
    self.table_id = DEFAULT_TABLE_PREFIX + str(uuid.uuid4())[:8]
    if not hasattr(self, 'client'):
      self.client = Client(project=self.project_id, admin=True)
      self.instance = self.client.instance(self.instance_id)
      self.table = self.instance.table(self.table_id)

  def sample_row_keys(self):
    keys = [b'beam_key0672496', b'beam_key1582279', b'beam_key22',
            b'beam_key2874203', b'beam_key3475534', b'beam_key4440786',
            b'beam_key51', b'beam_key56', b'beam_key65', b'beam_key7389168',
            b'beam_key8105103', b'beam_key9007992', b'']

    for (i, key) in enumerate(keys):
      sample_row = SampleRowKeysResponse()
      sample_row.row_key = key
      sample_row.offset_bytes = (i+1)*805306368
      yield sample_row

  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_estimate_size(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    bigtable = BigTableSource(self.project_id, self.instance_id, self.table_id)
    self.assertEqual(bigtable.estimate_size(), 10468982784)
    self.assertTrue(mock_my_method)

  # Split Range Size
  def _split_range_size(self, desired_bundle_size, range_):
    sample_row_keys = self.bigtable.get_sample_row_keys()
    split_range_ = self.bigtable.split_range_size(desired_bundle_size,
                                                  sample_row_keys,
                                                  range_)
    split_list = list(split_range_)
    return len(split_list)

  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_one_bundle_split_range_size(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    self.bigtable = BigTableSource(self.project_id, self.instance_id,
                                   self.table_id)
    desired_bundle_size = 805306368
    range_ = RowRange(b'beam_key0672496', b'beam_key1582279')

    self.assertEqual(self._split_range_size(desired_bundle_size, range_), 1)

  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_split_range_size_two(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    self.bigtable = BigTableSource(self.project_id, self.instance_id,
                                   self.table_id)
    desired_bundle_size = 402653184
    range_ = RowRange(b'beam_key0672496', b'beam_key1582279')

    self.assertEqual(self._split_range_size(desired_bundle_size, range_), 2)

  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_split_range_size_four(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    self.bigtable = BigTableSource(self.project_id, self.instance_id,
                                   self.table_id)
    desired_bundle_size = 201326592
    range_ = RowRange(b'beam_key0672496', b'beam_key1582279')

    sample_row_keys = self.bigtable.get_sample_row_keys()
    split_range_ = self.bigtable.split_range_size(desired_bundle_size,
                                                  sample_row_keys,
                                                  range_)
    split_list = list(split_range_)
    split_range_size = len(split_list)

    self.assertEqual(split_range_size, 4)

   # Range Split Fraction
  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_range_split_fraction(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    self.bigtable = BigTableSource(self.project_id, self.instance_id,
                                   self.table_id)
    current_size = 805306368
    desired_bundle_size = 402653184
    start_key = b'beam_key0672496'
    end_key = b'beam_key1582279'
    count_all = len(list(self.bigtable.range_split_fraction(current_size,
                                                            desired_bundle_size,
                                                            start_key,
                                                            end_key)))
    desired_bundle_count = current_size/desired_bundle_size
    self.assertEqual(count_all, desired_bundle_count)

  # Split Range Sized Subranges
  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_split_rage_sized_subranges(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    self.bigtable = BigTableSource(self.project_id, self.instance_id,
                                   self.table_id)
    sample_size_bytes = 805306368
    desired_bundle_size = 402653184
    start_key = b'beam_key0672496'
    end_key = b'beam_key1582279'
    ranges = self.bigtable.get_range_tracker(start_key, end_key)
    range_subranges = self.bigtable.split_range_subranges(
        sample_size_bytes,
        desired_bundle_size,
        ranges)
    count_all = len(list(range_subranges))
    desired_bundle_count = sample_size_bytes/desired_bundle_size
    self.assertEqual(count_all, desired_bundle_count)

  # Range Tracker
  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_get_range_tracker(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    self.bigtable = BigTableSource(self.project_id, self.instance_id,
                                   self.table_id)
    start_position = b'beam_key0672496'
    stop_position = b'beam_key1582279'
    range_tracker = self.bigtable.get_range_tracker(
        start_position,
        stop_position)

    self.assertEqual(range_tracker.start_position(), start_position)
    self.assertEqual(range_tracker.stop_position(), stop_position)

  @mock.patch.object(BigTableSource, 'get_sample_row_keys')
  def test_split_four_hundred_bytes(self, mock_my_method):
    mock_my_method.return_value = self.sample_row_keys()
    bigtable = BigTableSource(self.project_id, self.instance_id,
                              self.table_id)
    desired_bundle_size = 402653184
    split = bigtable.split(desired_bundle_size)
    split_list = list(split)
    count = len(split_list)
    size = 10468982784
    count_size = size/desired_bundle_size
    count_size += 1
    self.assertEqual(count, count_size)

  def __read_list(self):
    for i in range(72496, 72500):
      key = 'beam_key'+str(i)
      if sys.version_info < (3, 0):
        key = bytes(key)
      else:
        key = bytes(key, 'utf8')
      yield PartialRowData(key)

  @mock.patch.object(Table, 'read_rows')
  def test_read(self, mock_read_rows):
    mock_read_rows.return_value = self.__read_list()
    bigtable = BigTableSource(self.project_id, self.instance_id,
                              self.table_id)
    start_position = b'beam_key0672496'
    stop_position = b'beam_key1582279'
    range_tracker = LexicographicKeyRangeTracker(start_position, stop_position)
    read = bigtable.read(range_tracker)
    new_read = bigtable.read(range_tracker)
    self.assertEqual(len(list(new_read)), 4)
    for i in read:
      self.assertIsInstance(i, PartialRowData)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
