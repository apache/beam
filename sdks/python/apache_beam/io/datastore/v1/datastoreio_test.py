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

from google.datastore.v1 import datastore_pb2
from google.datastore.v1 import query_pb2
from google.protobuf import timestamp_pb2
from googledatastore import helper as datastore_helper
from mock import MagicMock, call, mock

from apache_beam.io.datastore.v1 import helper
# pylint: disable=unused-import
from apache_beam.io.datastore.v1 import query_splitter
# pylint: enable=unused-import
from apache_beam.io.datastore.v1.datastoreio import ReadFromDatastore


class DatastoreioTest(unittest.TestCase):
  _PROJECT = 'project'
  _KIND = 'kind'
  _NAMESPACE = 'namespace'

  def setUp(self):
    self._mock_datastore = MagicMock()
    self._query = query_pb2.Query()
    self._query.kind.add().name = self._KIND

  def test_get_estimated_size_bytes_without_namespace(self):
    entity_bytes = 100
    timestamp = timestamp_pb2.Timestamp(seconds=1234)
    self.check_estimated_size_bytes(entity_bytes, timestamp)

  def test_get_estimated_size_bytes_with_namespace(self):
    entity_bytes = 100
    timestamp = timestamp_pb2.Timestamp(seconds=1234)
    self.check_estimated_size_bytes(entity_bytes, timestamp, self._NAMESPACE)

  def test_SplitQueryFn_with_num_splits(self):
    with mock.patch('__main__.helper.get_datastore',
                    return_value=self._mock_datastore):
      num_splits = 23

      def fake_get_splits(datastore, query, num_splits, partition=None):
        return self.split_query(query, num_splits)

      with mock.patch('__main__.query_splitter.get_splits',
                      side_effect=fake_get_splits):

        split_query_fn = ReadFromDatastore.SplitQueryFn(
            self._PROJECT, self._query, None, num_splits)
        mock_context = MagicMock()
        mock_context.element = self._query
        split_query_fn.start_bundle(mock_context)
        returned_split_queries = []
        for split_query in split_query_fn.process(mock_context):
          returned_split_queries.append(split_query)

        self.assertEqual(len(returned_split_queries), num_splits)
        self.assertEqual(0, len(self._mock_datastore.run_query.call_args_list))
        self.verify_unique_keys(returned_split_queries)

  def test_SplitQueryFn_without_num_splits(self):
    with mock.patch('__main__.helper.get_datastore',
                    return_value=self._mock_datastore):
      # Force SplitQueryFn to compute the number of query splits
      num_splits = 0
      expected_num_splits = 23
      entity_bytes = expected_num_splits * \
          ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES

      with mock.patch('__main__.ReadFromDatastore.get_estimated_size_bytes',
                      return_value=entity_bytes):

        def fake_get_splits(datastore, query, num_splits, partition=None):
          return self.split_query(query, num_splits)

        with mock.patch('__main__.query_splitter.get_splits',
                        side_effect=fake_get_splits):
          split_query_fn = ReadFromDatastore.SplitQueryFn(
              self._PROJECT, self._query, None, num_splits)
          mock_context = MagicMock()
          mock_context.element = self._query
          split_query_fn.start_bundle(mock_context)
          returned_split_queries = []
          for split_query in split_query_fn.process(mock_context):
            returned_split_queries.append(split_query)

          self.assertEqual(len(returned_split_queries), expected_num_splits)
          self.assertEqual(0,
                           len(self._mock_datastore.run_query.call_args_list))
          self.verify_unique_keys(returned_split_queries)

  def test_SplitQueryFn_with_query_limit(self):
    """A test that verifies no split is performed when the query has a limit."""
    self._query.limit.value = 3
    split_query_fn = ReadFromDatastore.SplitQueryFn(
        self._PROJECT, self._query, None, 4)
    mock_context = MagicMock()
    mock_context.element = self._query
    split_query_fn.start_bundle(mock_context)
    returned_split_queries = []
    for split_query in split_query_fn.process(mock_context):
      returned_split_queries.append(split_query)

    self.assertEqual(1, len(returned_split_queries))

  def verify_unique_keys(self, queries):
    """A helper function that verifies if all the queries have unique keys."""
    keys, _ = zip(*queries)
    keys = set(keys)
    self.assertEqual(len(keys), len(queries))

  def check_estimated_size_bytes(self, entity_bytes, timestamp, namespace=None):
    """A helper method to test get_estimated_size_bytes"""

    timestamp_req = helper.make_request(
        self._PROJECT, namespace, helper.make_latest_timestamp_query(namespace))
    timestamp_resp = self.make_stats_response(
        {'timestamp': datastore_helper.from_timestamp(timestamp)})
    kind_stat_req = helper.make_request(
        self._PROJECT, namespace, helper.make_kind_stats_query(
            namespace, self._query.kind[0].name,
            datastore_helper.micros_from_timestamp(timestamp)))
    kind_stat_resp = self.make_stats_response(
        {'entity_bytes': entity_bytes})

    def fake_run_query(req):
      if req == timestamp_req:
        return timestamp_resp
      elif req == kind_stat_req:
        return kind_stat_resp
      else:
        print kind_stat_req
        raise ValueError("Unknown req: %s" % req)

    self._mock_datastore.run_query.side_effect = fake_run_query
    self.assertEqual(entity_bytes, ReadFromDatastore.get_estimated_size_bytes(
        self._PROJECT, namespace, self._query, self._mock_datastore))
    self.assertEqual(self._mock_datastore.run_query.call_args_list,
                     [call(timestamp_req), call(kind_stat_req)])

  def make_stats_response(self, property_map):
    resp = datastore_pb2.RunQueryResponse()
    entity_result = resp.batch.entity_results.add()
    datastore_helper.add_properties(entity_result.entity, property_map)
    return resp

  def split_query(self, query, num_splits):
    """Generate dummy query splits."""
    split_queries = []
    for _ in range(0, num_splits):
      q = query_pb2.Query()
      q.CopyFrom(query)
      split_queries.append(q)
    return split_queries

if __name__ == '__main__':
  unittest.main()
