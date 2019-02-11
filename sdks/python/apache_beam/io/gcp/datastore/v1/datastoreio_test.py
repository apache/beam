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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import unittest
from builtins import map
from builtins import range
from builtins import zip

from mock import MagicMock
from mock import call
from mock import patch

from apache_beam.io.gcp.datastore.v1 import fake_datastore
from apache_beam.io.gcp.datastore.v1 import helper
from apache_beam.io.gcp.datastore.v1 import query_splitter
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import _Mutate


# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position, ungrouped-imports
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from google.cloud.proto.datastore.v1 import query_pb2
  from google.protobuf import timestamp_pb2
  from googledatastore import helper as datastore_helper
except ImportError:
  datastore_pb2 = None
# pylint: enable=wrong-import-order, wrong-import-position, ungrouped-imports


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-4543')
@unittest.skipIf(datastore_pb2 is None, 'GCP dependencies are not installed')
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
    with patch.object(helper, 'get_datastore',
                      return_value=self._mock_datastore):
      num_splits = 23

      def fake_get_splits(datastore, query, num_splits, partition=None):
        return self.split_query(query, num_splits)

      with patch.object(query_splitter, 'get_splits',
                        side_effect=fake_get_splits):

        split_query_fn = ReadFromDatastore.SplitQueryFn(
            self._PROJECT, self._query, None, num_splits)
        split_query_fn.start_bundle()
        returned_split_queries = []
        for split_query in split_query_fn.process(self._query):
          returned_split_queries.append(split_query)

        self.assertEqual(len(returned_split_queries), num_splits)
        self.assertEqual(0, len(self._mock_datastore.run_query.call_args_list))
        self.verify_unique_keys(returned_split_queries)

  def test_SplitQueryFn_without_num_splits(self):
    with patch.object(helper, 'get_datastore',
                      return_value=self._mock_datastore):
      # Force SplitQueryFn to compute the number of query splits
      num_splits = 0
      expected_num_splits = 23
      entity_bytes = (expected_num_splits *
                      ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES)
      with patch.object(ReadFromDatastore, 'get_estimated_size_bytes',
                        return_value=entity_bytes):

        def fake_get_splits(datastore, query, num_splits, partition=None):
          return self.split_query(query, num_splits)

        with patch.object(query_splitter, 'get_splits',
                          side_effect=fake_get_splits):
          split_query_fn = ReadFromDatastore.SplitQueryFn(
              self._PROJECT, self._query, None, num_splits)
          split_query_fn.start_bundle()
          returned_split_queries = []
          for split_query in split_query_fn.process(self._query):
            returned_split_queries.append(split_query)

          self.assertEqual(len(returned_split_queries), expected_num_splits)
          self.assertEqual(0,
                           len(self._mock_datastore.run_query.call_args_list))
          self.verify_unique_keys(returned_split_queries)

  def test_SplitQueryFn_with_query_limit(self):
    """A test that verifies no split is performed when the query has a limit."""
    with patch.object(helper, 'get_datastore',
                      return_value=self._mock_datastore):
      self._query.limit.value = 3
      split_query_fn = ReadFromDatastore.SplitQueryFn(
          self._PROJECT, self._query, None, 4)
      split_query_fn.start_bundle()
      returned_split_queries = []
      for split_query in split_query_fn.process(self._query):
        returned_split_queries.append(split_query)

      self.assertEqual(1, len(returned_split_queries))
      self.assertEqual(0, len(self._mock_datastore.method_calls))

  def test_SplitQueryFn_with_exception(self):
    """A test that verifies that no split is performed when failures occur."""
    with patch.object(helper, 'get_datastore',
                      return_value=self._mock_datastore):
      # Force SplitQueryFn to compute the number of query splits
      num_splits = 0
      expected_num_splits = 1
      entity_bytes = (expected_num_splits *
                      ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES)
      with patch.object(ReadFromDatastore, 'get_estimated_size_bytes',
                        return_value=entity_bytes):

        with patch.object(query_splitter, 'get_splits',
                          side_effect=ValueError("Testing query split error")):
          split_query_fn = ReadFromDatastore.SplitQueryFn(
              self._PROJECT, self._query, None, num_splits)
          split_query_fn.start_bundle()
          returned_split_queries = []
          for split_query in split_query_fn.process(self._query):
            returned_split_queries.append(split_query)

          self.assertEqual(len(returned_split_queries), expected_num_splits)
          self.assertEqual(returned_split_queries[0][1], self._query)
          self.assertEqual(0,
                           len(self._mock_datastore.run_query.call_args_list))
          self.verify_unique_keys(returned_split_queries)

  def test_DatastoreWriteFn_with_emtpy_batch(self):
    self.check_DatastoreWriteFn(0)

  def test_DatastoreWriteFn_with_one_batch(self):
    num_entities_to_write = _Mutate._WRITE_BATCH_INITIAL_SIZE * 1 - 50
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteFn_with_multiple_batches(self):
    num_entities_to_write = _Mutate._WRITE_BATCH_INITIAL_SIZE * 3 + 50
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteFn_with_batch_size_exact_multiple(self):
    num_entities_to_write = _Mutate._WRITE_BATCH_INITIAL_SIZE * 2
    self.check_DatastoreWriteFn(num_entities_to_write)

  def check_DatastoreWriteFn(self, num_entities):
    """A helper function to test DatastoreWriteFn."""

    with patch.object(helper, 'get_datastore',
                      return_value=self._mock_datastore):
      entities = [e.entity for e in
                  fake_datastore.create_entities(num_entities)]

      expected_mutations = list(map(WriteToDatastore.to_upsert_mutation,
                                    entities))
      actual_mutations = []

      self._mock_datastore.commit.side_effect = (
          fake_datastore.create_commit(actual_mutations))

      datastore_write_fn = _Mutate.DatastoreWriteFn(
          self._PROJECT, fixed_batch_size=_Mutate._WRITE_BATCH_INITIAL_SIZE)

      datastore_write_fn.start_bundle()
      for mutation in expected_mutations:
        datastore_write_fn.process(mutation)
      datastore_write_fn.finish_bundle()

      self.assertEqual(actual_mutations, expected_mutations)
      self.assertEqual(
          (num_entities - 1) // _Mutate._WRITE_BATCH_INITIAL_SIZE + 1,
          self._mock_datastore.commit.call_count)

  def test_DatastoreWriteLargeEntities(self):
    """100*100kB entities gets split over two Commit RPCs."""
    with patch.object(helper, 'get_datastore',
                      return_value=self._mock_datastore):
      entities = [e.entity for e in fake_datastore.create_entities(100)]

      datastore_write_fn = _Mutate.DatastoreWriteFn(
          self._PROJECT, fixed_batch_size=_Mutate._WRITE_BATCH_INITIAL_SIZE)
      datastore_write_fn.start_bundle()
      for entity in entities:
        datastore_helper.add_properties(
            entity, {'large': u'A' * 100000}, exclude_from_indexes=True)
        datastore_write_fn.process(WriteToDatastore.to_upsert_mutation(entity))
      datastore_write_fn.finish_bundle()

      self.assertEqual(2, self._mock_datastore.commit.call_count)

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
        print(kind_stat_req)
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


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-4543')
@unittest.skipIf(datastore_pb2 is None, 'GCP dependencies are not installed')
class DynamicWriteBatcherTest(unittest.TestCase):

  def setUp(self):
    self._batcher = _Mutate._DynamicBatchSizer()

  # If possible, keep these test cases aligned with the Java test cases in
  # DatastoreV1Test.java
  def test_no_data(self):
    self.assertEquals(_Mutate._WRITE_BATCH_INITIAL_SIZE,
                      self._batcher.get_batch_size(0))

  def test_fast_queries(self):
    self._batcher.report_latency(0, 1000, 200)
    self._batcher.report_latency(0, 1000, 200)
    self.assertEquals(_Mutate._WRITE_BATCH_MAX_SIZE,
                      self._batcher.get_batch_size(0))

  def test_slow_queries(self):
    self._batcher.report_latency(0, 10000, 200)
    self._batcher.report_latency(0, 10000, 200)
    self.assertEquals(100, self._batcher.get_batch_size(0))

  def test_size_not_below_minimum(self):
    self._batcher.report_latency(0, 30000, 50)
    self._batcher.report_latency(0, 30000, 50)
    self.assertEquals(_Mutate._WRITE_BATCH_MIN_SIZE,
                      self._batcher.get_batch_size(0))

  def test_sliding_window(self):
    self._batcher.report_latency(0, 30000, 50)
    self._batcher.report_latency(50000, 5000, 200)
    self._batcher.report_latency(100000, 5000, 200)
    self.assertEquals(200, self._batcher.get_batch_size(150000))


if __name__ == '__main__':
  unittest.main()
