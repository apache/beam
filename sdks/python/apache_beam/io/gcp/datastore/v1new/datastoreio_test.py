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

"""Unit tests for datastoreio."""

# pytype: skip-file

import datetime
import math
import unittest

from mock import MagicMock
from mock import call
from mock import patch
from mock import ANY

# Protect against environments where datastore library is not available.
try:
  from apache_beam.io.gcp import resource_identifiers
  from apache_beam.io.gcp.datastore.v1new import helper, util
  from apache_beam.io.gcp.datastore.v1new import query_splitter
  from apache_beam.io.gcp.datastore.v1new import datastoreio
  from apache_beam.io.gcp.datastore.v1new.datastoreio import DeleteFromDatastore
  from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
  from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
  from apache_beam.io.gcp.datastore.v1new.types import Key
  from apache_beam.metrics import monitoring_infos
  from apache_beam.metrics.execution import MetricsEnvironment
  from google.cloud.datastore import client
  from google.cloud.datastore import entity
  from google.cloud.datastore import helpers
  from google.cloud.datastore import key
  from google.api_core import exceptions
except ImportError:
  client = None


# used for internal testing only
class FakeMessage:
  def __init__(self, entity, key):
    self.entity = entity
    self.key = key

  def ByteSize(self):
    if self.entity is not None:
      return helpers.entity_to_protobuf(self.entity)._pb.ByteSize()
    else:
      return self.key.to_protobuf()._pb.ByteSize()


# used for internal testing only
class FakeMutation(object):
  def __init__(self, entity=None, key=None):
    """Fake mutation request object.

    Requires exactly one of entity or key to be set.

    Args:
      entity: (``google.cloud.datastore.entity.Entity``) entity representing
        this upsert mutation
      key: (``google.cloud.datastore.key.Key``) key representing
        this delete mutation
    """
    self.entity = entity
    self.key = key
    self._pb = FakeMessage(entity, key)


class FakeBatch(object):
  def __init__(self, all_batch_items=None, commit_count=None):
    """Fake ``google.cloud.datastore.batch.Batch`` object.

    Args:
      all_batch_items: (list) If set, will append all entities/keys added to
        this batch.
      commit_count: (list of int) If set, will increment commit_count[0] on
        each ``commit``.
    """
    self._all_batch_items = all_batch_items
    self._commit_count = commit_count
    self.mutations = []

  def put(self, _entity):
    assert isinstance(_entity, entity.Entity)
    self.mutations.append(FakeMutation(entity=_entity))
    if self._all_batch_items is not None:
      self._all_batch_items.append(_entity)

  def delete(self, _key):
    assert isinstance(_key, key.Key)
    self.mutations.append(FakeMutation(key=_key))
    if self._all_batch_items is not None:
      self._all_batch_items.append(_key)

  def begin(self):
    pass

  def commit(self):
    if self._commit_count:
      self._commit_count[0] += 1


@unittest.skipIf(client is None, 'Datastore dependencies are not installed')
class MutateTest(unittest.TestCase):
  def test_write_mutations_no_errors(self):
    mock_batch = MagicMock()
    mock_throttler = MagicMock()
    rpc_stats_callback = MagicMock()
    mock_throttler.throttle_request.return_value = []
    mutate = datastoreio._Mutate.DatastoreMutateFn("")
    mutate._batch = mock_batch
    mutate.write_mutations(mock_throttler, rpc_stats_callback)
    rpc_stats_callback.assert_has_calls([
        call(successes=1),
    ])

  @patch('time.sleep', return_value=None)
  def test_write_mutations_reconstruct_on_error(self, unused_sleep):
    mock_batch = MagicMock()
    mock_batch.begin.side_effect = [None, ValueError]
    mock_batch.commit.side_effect = [
        exceptions.DeadlineExceeded('retryable'), None
    ]
    mock_throttler = MagicMock()
    rpc_stats_callback = MagicMock()
    mock_throttler.throttle_request.return_value = []
    mutate = datastoreio._Mutate.DatastoreMutateFn("")
    mutate._batch = mock_batch
    mutate._client = MagicMock()
    mutate._batch_elements = [None]
    mock_add_to_batch = MagicMock()
    mutate.add_to_batch = mock_add_to_batch
    mutate.write_mutations(mock_throttler, rpc_stats_callback)
    rpc_stats_callback.assert_has_calls([
        call(successes=1),
    ])
    self.assertEqual(1, mock_add_to_batch.call_count)

  @patch('time.sleep', return_value=None)
  def test_write_mutations_throttle_delay_retryable_error(self, unused_sleep):
    mock_batch = MagicMock()
    mock_batch.commit.side_effect = [
        exceptions.DeadlineExceeded('retryable'), None
    ]
    mock_throttler = MagicMock()
    rpc_stats_callback = MagicMock()
    # First try: throttle once [True, False]
    # Second try: no throttle [False]
    mock_throttler.throttle_request.side_effect = [True, False, False]
    mutate = datastoreio._Mutate.DatastoreMutateFn("")
    mutate._batch = mock_batch
    mutate._batch_elements = []
    mutate._client = MagicMock()
    mutate.write_mutations(mock_throttler, rpc_stats_callback, throttle_delay=0)
    rpc_stats_callback.assert_has_calls([
        call(successes=1),
        call(throttled_secs=ANY),
        call(errors=1),
    ],
                                        any_order=True)
    self.assertEqual(3, rpc_stats_callback.call_count)

  def test_write_mutations_non_retryable_error(self):
    mock_batch = MagicMock()
    mock_batch.commit.side_effect = [
        exceptions.InvalidArgument('non-retryable'),
    ]
    mock_throttler = MagicMock()
    rpc_stats_callback = MagicMock()
    mock_throttler.throttle_request.return_value = False
    mutate = datastoreio._Mutate.DatastoreMutateFn("")
    mutate._batch = mock_batch
    with self.assertRaises(exceptions.InvalidArgument):
      mutate.write_mutations(
          mock_throttler, rpc_stats_callback, throttle_delay=0)
    rpc_stats_callback.assert_called_once_with(errors=1)

  def test_write_mutations_metric_on_failure(self):
    MetricsEnvironment.process_wide_container().reset()
    mock_batch = MagicMock()
    mock_batch.commit.side_effect = [
        exceptions.DeadlineExceeded("Deadline Exceeded"), []
    ]
    mock_throttler = MagicMock()
    rpc_stats_callback = MagicMock()
    mock_throttler.throttle_request.return_value = False
    mutate = datastoreio._Mutate.DatastoreMutateFn("my_project")
    mutate._batch = mock_batch
    mutate._batch_elements = []
    mutate._client = MagicMock()
    mutate.write_mutations(mock_throttler, rpc_stats_callback, throttle_delay=0)
    self.verify_write_call_metric("my_project", "", "deadline_exceeded", 1)
    self.verify_write_call_metric("my_project", "", "ok", 1)

  def verify_write_call_metric(self, project_id, namespace, status, count):
    """Check if a metric was recorded for the Datastore IO write API call."""
    process_wide_monitoring_infos = list(
        MetricsEnvironment.process_wide_container().
        to_runner_api_monitoring_infos(None).values())
    resource = resource_identifiers.DatastoreNamespace(project_id, namespace)
    labels = {
        monitoring_infos.SERVICE_LABEL: 'Datastore',
        monitoring_infos.METHOD_LABEL: 'BatchDatastoreWrite',
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.DATASTORE_NAMESPACE_LABEL: namespace,
        monitoring_infos.DATASTORE_PROJECT_ID_LABEL: project_id,
        monitoring_infos.STATUS_LABEL: status
    }
    expected_mi = monitoring_infos.int64_counter(
        monitoring_infos.API_REQUEST_COUNT_URN, count, labels=labels)
    expected_mi.ClearField("start_time")

    found = False
    for actual_mi in process_wide_monitoring_infos:
      actual_mi.ClearField("start_time")
      if expected_mi == actual_mi:
        found = True
        break
    self.assertTrue(
        found, "Did not find write call metric with status: %s" % status)


@unittest.skipIf(client is None, 'Datastore dependencies are not installed')
class DatastoreioTest(unittest.TestCase):
  _PROJECT = 'project'
  _KIND = 'kind'
  _NAMESPACE = 'namespace'

  def setUp(self):
    self._WRITE_BATCH_INITIAL_SIZE = util.WRITE_BATCH_INITIAL_SIZE
    self._mock_client = MagicMock()
    self._mock_client.project = self._PROJECT
    self._mock_client.namespace = self._NAMESPACE
    self._mock_query = MagicMock()
    self._mock_query.limit = None
    self._mock_query.order = None

    self._real_client = client.Client(
        project=self._PROJECT,
        namespace=self._NAMESPACE,
        # Don't do any network requests.
        _http=MagicMock())

  def test_SplitQueryFn_with_num_splits(self):
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      num_splits = 23
      expected_num_splits = 23

      def fake_get_splits(unused_client, query, num_splits):
        return [query] * num_splits

      with patch.object(query_splitter,
                        'get_splits',
                        side_effect=fake_get_splits):
        split_query_fn = ReadFromDatastore._SplitQueryFn(num_splits)
        split_queries = split_query_fn.process(self._mock_query)

        self.assertEqual(expected_num_splits, len(split_queries))

  def test_SplitQueryFn_without_num_splits(self):
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      # Force _SplitQueryFn to compute the number of query splits
      num_splits = 0
      expected_num_splits = 23
      entity_bytes = (
          expected_num_splits * ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES)
      with patch.object(ReadFromDatastore._SplitQueryFn,
                        'get_estimated_size_bytes',
                        return_value=entity_bytes):

        def fake_get_splits(unused_client, query, num_splits):
          return [query] * num_splits

        with patch.object(query_splitter,
                          'get_splits',
                          side_effect=fake_get_splits):
          split_query_fn = ReadFromDatastore._SplitQueryFn(num_splits)
          split_queries = split_query_fn.process(self._mock_query)

          self.assertEqual(expected_num_splits, len(split_queries))

  def test_SplitQueryFn_with_query_limit(self):
    """A test that verifies no split is performed when the query has a limit."""
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      num_splits = 4
      expected_num_splits = 1
      self._mock_query.limit = 3
      split_query_fn = ReadFromDatastore._SplitQueryFn(num_splits)
      split_queries = split_query_fn.process(self._mock_query)

      self.assertEqual(expected_num_splits, len(split_queries))

  def test_SplitQueryFn_with_exception(self):
    """A test that verifies that no split is performed when failures occur."""
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      # Force _SplitQueryFn to compute the number of query splits
      num_splits = 0
      expected_num_splits = 1
      entity_bytes = (
          expected_num_splits * ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES)
      with patch.object(ReadFromDatastore._SplitQueryFn,
                        'get_estimated_size_bytes',
                        return_value=entity_bytes):

        with patch.object(query_splitter,
                          'get_splits',
                          side_effect=query_splitter.QuerySplitterError(
                              "Testing query split error")):
          split_query_fn = ReadFromDatastore._SplitQueryFn(num_splits)
          split_queries = split_query_fn.process(self._mock_query)

          self.assertEqual(expected_num_splits, len(split_queries))
          self.assertEqual(self._mock_query, split_queries[0])

  def test_QueryFn_metric_on_failure(self):
    MetricsEnvironment.process_wide_container().reset()
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      self._mock_query.project = self._PROJECT
      self._mock_query.namespace = self._NAMESPACE
      _query_fn = ReadFromDatastore._QueryFn()
      client_query = self._mock_query._to_client_query()
      # Test with exception
      client_query.fetch.side_effect = [
          exceptions.DeadlineExceeded("Deadline exceed")
      ]
      try:
        list(_query_fn.process(self._mock_query))
      except Exception:
        self.verify_read_call_metric(
            self._PROJECT, self._NAMESPACE, "deadline_exceeded", 1)
        # Test success
        client_query.fetch.side_effect = [[]]
        list(_query_fn.process(self._mock_query))
        self.verify_read_call_metric(self._PROJECT, self._NAMESPACE, "ok", 1)
      else:
        raise Exception('Excepted  _query_fn.process call to raise an error')

  def verify_read_call_metric(self, project_id, namespace, status, count):
    """Check if a metric was recorded for the Datastore IO read API call."""
    process_wide_monitoring_infos = list(
        MetricsEnvironment.process_wide_container().
        to_runner_api_monitoring_infos(None).values())
    resource = resource_identifiers.DatastoreNamespace(project_id, namespace)
    labels = {
        monitoring_infos.SERVICE_LABEL: 'Datastore',
        monitoring_infos.METHOD_LABEL: 'BatchDatastoreRead',
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.DATASTORE_NAMESPACE_LABEL: namespace,
        monitoring_infos.DATASTORE_PROJECT_ID_LABEL: project_id,
        monitoring_infos.STATUS_LABEL: status
    }
    expected_mi = monitoring_infos.int64_counter(
        monitoring_infos.API_REQUEST_COUNT_URN, count, labels=labels)
    expected_mi.ClearField("start_time")

    found = False
    for actual_mi in process_wide_monitoring_infos:
      actual_mi.ClearField("start_time")
      if expected_mi == actual_mi:
        found = True
        break
    self.assertTrue(
        found, "Did not find read call metric with status: %s" % status)

  def check_DatastoreWriteFn(self, num_entities):
    """A helper function to test _DatastoreWriteFn."""
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      entities = helper.create_entities(num_entities)
      expected_entities = [entity.to_client_entity() for entity in entities]

      # Infer project from write fn project arg.
      if num_entities:
        key = Key(['k1', 1234], project=self._PROJECT)
        expected_key = key.to_client_key()
        key.project = None
        entities[0].key = key
        expected_entities[0].key = expected_key

      all_batch_entities = []
      commit_count = [0]
      self._mock_client.batch.side_effect = (
          lambda: FakeBatch(
              all_batch_items=all_batch_entities, commit_count=commit_count))

      datastore_write_fn = WriteToDatastore._DatastoreWriteFn(self._PROJECT)

      datastore_write_fn.start_bundle()
      for entity in entities:
        datastore_write_fn.process(entity)
      datastore_write_fn.finish_bundle()

      self.assertListEqual([e.key for e in all_batch_entities],
                           [e.key for e in expected_entities])
      batch_count = math.ceil(num_entities / util.WRITE_BATCH_MAX_SIZE)
      self.assertLessEqual(batch_count, commit_count[0])

  def test_DatastoreWriteFn_with_empty_batch(self):
    self.check_DatastoreWriteFn(0)

  def test_DatastoreWriteFn_with_one_batch(self):
    num_entities_to_write = self._WRITE_BATCH_INITIAL_SIZE * 1 - 50
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteFn_with_multiple_batches(self):
    num_entities_to_write = self._WRITE_BATCH_INITIAL_SIZE * 3 + 50
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteFn_with_batch_size_exact_multiple(self):
    num_entities_to_write = self._WRITE_BATCH_INITIAL_SIZE * 2
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteFn_with_dynamic_batch_sizes(self):
    num_entities_to_write = self._WRITE_BATCH_INITIAL_SIZE * 3 + 50
    self.check_DatastoreWriteFn(num_entities_to_write)

  def test_DatastoreWriteLargeEntities(self):
    """100*100kB entities gets split over two Commit RPCs."""
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      entities = helper.create_entities(100)
      commit_count = [0]
      self._mock_client.batch.side_effect = (
          lambda: FakeBatch(commit_count=commit_count))

      datastore_write_fn = WriteToDatastore._DatastoreWriteFn(self._PROJECT)
      datastore_write_fn.start_bundle()
      for entity in entities:
        entity.set_properties({'large': 'A' * 100000})
        datastore_write_fn.process(entity)
      datastore_write_fn.finish_bundle()

      self.assertEqual(2, commit_count[0])

  def check_estimated_size_bytes(self, entity_bytes, timestamp, namespace=None):
    """A helper method to test get_estimated_size_bytes"""
    self._mock_client.namespace = namespace
    self._mock_client.query.return_value = self._mock_query
    self._mock_query.project = self._PROJECT
    self._mock_query.namespace = namespace
    self._mock_query.fetch.side_effect = [
        [{
            'timestamp': timestamp
        }],
        [{
            'entity_bytes': entity_bytes
        }],
    ]
    self._mock_query.kind = self._KIND

    split_query_fn = ReadFromDatastore._SplitQueryFn(num_splits=0)
    self.assertEqual(
        entity_bytes,
        split_query_fn.get_estimated_size_bytes(
            self._mock_client, self._mock_query))

    if namespace is None:
      ns_keyword = '_'
    else:
      ns_keyword = '_Ns_'
    self._mock_client.query.assert_has_calls([
        call(kind='__Stat%sTotal__' % ns_keyword, order=['-timestamp']),
        call().fetch(limit=1),
        call(kind='__Stat%sKind__' % ns_keyword),
        call().add_filter('kind_name', '=', self._KIND),
        call().add_filter('timestamp', '=', timestamp),
        call().fetch(limit=1),
    ])

  def get_timestamp(self):
    return datetime.datetime(2019, 3, 14, 15, 9, 26, 535897)

  def test_get_estimated_size_bytes_without_namespace(self):
    entity_bytes = 100
    timestamp = self.get_timestamp()
    self.check_estimated_size_bytes(entity_bytes, timestamp)

  def test_get_estimated_size_bytes_with_namespace(self):
    entity_bytes = 100
    timestamp = self.get_timestamp()
    self.check_estimated_size_bytes(entity_bytes, timestamp, self._NAMESPACE)

  def test_DatastoreDeleteFn(self):
    with patch.object(helper, 'get_client', return_value=self._mock_client):
      keys = [entity.key for entity in helper.create_entities(10)]
      expected_keys = [key.to_client_key() for key in keys]

      # Infer project from delete fn project arg.
      key = Key(['k1', 1234], project=self._PROJECT)
      expected_key = key.to_client_key()
      key.project = None
      keys.append(key)
      expected_keys.append(expected_key)

      all_batch_keys = []
      self._mock_client.batch.side_effect = (
          lambda: FakeBatch(all_batch_items=all_batch_keys))

      datastore_delete_fn = DeleteFromDatastore._DatastoreDeleteFn(
          self._PROJECT)

      datastore_delete_fn.start_bundle()
      for key in keys:
        datastore_delete_fn.process(key)
        datastore_delete_fn.finish_bundle()

      self.assertListEqual(all_batch_keys, expected_keys)


if __name__ == '__main__':
  unittest.main()
