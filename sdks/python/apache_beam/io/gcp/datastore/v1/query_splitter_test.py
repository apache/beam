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

"""Cloud Datastore query splitter test."""

from __future__ import absolute_import

import os
import sys
import unittest

from mock import MagicMock
from mock import call

# pylint: disable=ungrouped-imports
try: # TODO(BEAM-4543): googledatastore dependency does not work on Python 3.
  from apache_beam.io.gcp.datastore.v1 import fake_datastore
  from apache_beam.io.gcp.datastore.v1 import query_splitter
except ImportError:
  pass

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from google.cloud.proto.datastore.v1 import query_pb2
  from google.cloud.proto.datastore.v1.query_pb2 import PropertyFilter
except ImportError:
  datastore_pb2 = None
# pylint: enable=wrong-import-order, wrong-import-position
# pylint: enable=ungrouped-imports


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-4543')
@unittest.skipIf(datastore_pb2 is None, 'GCP dependencies are not installed')
class QuerySplitterTest(unittest.TestCase):

  def test_get_splits_query_with_multiple_kinds(self):
    query = query_pb2.Query()
    query.kind.add()
    query.kind.add()
    self.assertRaises(ValueError, query_splitter.get_splits, None, query, 4)

  def test_get_splits_query_with_order(self):
    query = query_pb2.Query()
    query.kind.add()
    query.order.add()

    self.assertRaises(ValueError, query_splitter.get_splits, None, query, 3)

  def test_get_splits_query_with_unsupported_filter(self):
    query = query_pb2.Query()
    query.kind.add()
    test_filter = query.filter.composite_filter.filters.add()
    test_filter.property_filter.op = PropertyFilter.GREATER_THAN
    self.assertRaises(ValueError, query_splitter.get_splits, None, query, 2)

  def test_get_splits_query_with_limit(self):
    query = query_pb2.Query()
    query.kind.add()
    query.limit.value = 10
    self.assertRaises(ValueError, query_splitter.get_splits, None, query, 2)

  def test_get_splits_query_with_offset(self):
    query = query_pb2.Query()
    query.kind.add()
    query.offset = 10
    self.assertRaises(ValueError, query_splitter.get_splits, None, query, 2)

  def test_create_scatter_query(self):
    query = query_pb2.Query()
    kind = query.kind.add()
    kind.name = 'shakespeare-demo'
    num_splits = 10
    scatter_query = query_splitter._create_scatter_query(query, num_splits)
    self.assertEqual(scatter_query.kind[0], kind)
    self.assertEqual(scatter_query.limit.value,
                     (num_splits -1) * query_splitter.KEYS_PER_SPLIT)
    self.assertEqual(scatter_query.order[0].direction,
                     query_pb2.PropertyOrder.ASCENDING)
    self.assertEqual(scatter_query.projection[0].property.name,
                     query_splitter.KEY_PROPERTY_NAME)

  def test_get_splits_with_two_splits(self):
    query = query_pb2.Query()
    kind = query.kind.add()
    kind.name = 'shakespeare-demo'
    num_splits = 2
    num_entities = 97
    batch_size = 9

    self.check_get_splits(query, num_splits, num_entities, batch_size)

  def test_get_splits_with_multiple_splits(self):
    query = query_pb2.Query()
    kind = query.kind.add()
    kind.name = 'shakespeare-demo'
    num_splits = 4
    num_entities = 369
    batch_size = 12

    self.check_get_splits(query, num_splits, num_entities, batch_size)

  def test_get_splits_with_large_num_splits(self):
    query = query_pb2.Query()
    kind = query.kind.add()
    kind.name = 'shakespeare-demo'
    num_splits = 10
    num_entities = 4
    batch_size = 10

    self.check_get_splits(query, num_splits, num_entities, batch_size)

  def test_get_splits_with_small_num_entities(self):
    query = query_pb2.Query()
    kind = query.kind.add()
    kind.name = 'shakespeare-demo'
    num_splits = 4
    num_entities = 50
    batch_size = 10

    self.check_get_splits(query, num_splits, num_entities, batch_size)

  def test_get_splits_with_batch_size_exact_multiple(self):
    """Test get_splits when num scatter keys is a multiple of batch size."""
    query = query_pb2.Query()
    kind = query.kind.add()
    kind.name = 'shakespeare-demo'
    num_splits = 4
    num_entities = 400
    batch_size = 32

    self.check_get_splits(query, num_splits, num_entities, batch_size)

  def test_get_splits_with_large_batch_size(self):
    """Test get_splits when all scatter keys are retured in a single req."""
    query = query_pb2.Query()
    kind = query.kind.add()
    kind.name = 'shakespeare-demo'
    num_splits = 4
    num_entities = 400
    batch_size = 500

    self.check_get_splits(query, num_splits, num_entities, batch_size)

  def check_get_splits(self, query, num_splits, num_entities, batch_size):
    """A helper method to test the query_splitter get_splits method.

    Args:
      query: the query to be split
      num_splits: number of splits
      num_entities: number of scatter entities contained in the fake datastore.
      batch_size: the number of entities returned by fake datastore in one req.
    """

    # Test for both random long ids and string ids.
    id_or_name = [True, False]

    for id_type in id_or_name:
      entities = fake_datastore.create_entities(num_entities, id_type)
      mock_datastore = MagicMock()
      # Assign a fake run_query method as a side_effect to the mock.
      mock_datastore.run_query.side_effect = \
          fake_datastore.create_run_query(entities, batch_size)

      split_queries = query_splitter.get_splits(
          mock_datastore, query, num_splits)

      # if request num_splits is greater than num_entities, the best it can
      # do is one entity per split.
      expected_num_splits = min(num_splits, num_entities + 1)
      self.assertEqual(len(split_queries), expected_num_splits)

      expected_requests = QuerySplitterTest.create_scatter_requests(
          query, num_splits, batch_size, num_entities)

      expected_calls = []
      for req in expected_requests:
        expected_calls.append(call(req))

      self.assertEqual(expected_calls, mock_datastore.run_query.call_args_list)

  @staticmethod
  def create_scatter_requests(query, num_splits, batch_size, num_entities):
    """Creates a list of expected scatter requests from the query splitter.

    This list of requests returned is used to verify that the query splitter
    made the same number of requests in the same order to datastore.
    """

    requests = []
    count = (num_splits - 1) * query_splitter.KEYS_PER_SPLIT
    start_cursor = ''
    i = 0
    scatter_query = query_splitter._create_scatter_query(query, count)
    while i < count and i < num_entities:
      request = datastore_pb2.RunQueryRequest()
      request.query.CopyFrom(scatter_query)
      request.query.start_cursor = start_cursor
      request.query.limit.value = count - i
      requests.append(request)
      i += batch_size
      start_cursor = str(i)

    return requests


if __name__ == '__main__':
  unittest.main()
