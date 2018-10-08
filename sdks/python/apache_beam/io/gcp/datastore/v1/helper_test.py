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

"""Tests for datastore helper."""
from __future__ import absolute_import

import errno
import os
import random
import sys
import unittest
from builtins import map
from socket import error as SocketError

from mock import MagicMock

# pylint: disable=ungrouped-imports
try: # TODO(BEAM-4543): googledatastore dependency does not work on Python 3.
  from apache_beam.io.gcp.datastore.v1 import fake_datastore
  from apache_beam.io.gcp.datastore.v1 import helper
  from apache_beam.testing.test_utils import patch_retry
except ImportError:
  pass

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from google.cloud.proto.datastore.v1 import entity_pb2
  from google.cloud.proto.datastore.v1 import query_pb2
  from google.cloud.proto.datastore.v1.entity_pb2 import Key
  from google.rpc import code_pb2
  from googledatastore.connection import RPCError
  from googledatastore import helper as datastore_helper
except ImportError:
  datastore_helper = None
# pylint: enable=wrong-import-order, wrong-import-position
# pylint: enable=ungrouped-imports


@unittest.skipIf(sys.version_info[0] == 3 and
                 os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                 'This test still needs to be fixed on Python 3'
                 'TODO: BEAM-4543')
@unittest.skipIf(datastore_helper is None, 'GCP dependencies are not installed')
class HelperTest(unittest.TestCase):

  def setUp(self):
    self._mock_datastore = MagicMock()
    self._query = query_pb2.Query()
    self._query.kind.add().name = 'dummy_kind'
    patch_retry(self, helper)
    self._retriable_errors = [
        RPCError("dummy", code_pb2.INTERNAL, "failed"),
        SocketError(errno.ECONNRESET, "Connection Reset"),
        SocketError(errno.ETIMEDOUT, "Timed out")
    ]

    self._non_retriable_errors = [
        RPCError("dummy", code_pb2.UNAUTHENTICATED, "failed"),
        SocketError(errno.EADDRNOTAVAIL, "Address not available")
    ]

  def permanent_retriable_datastore_failure(self, req):
    raise RPCError("dummy", code_pb2.UNAVAILABLE, "failed")

  def transient_retriable_datastore_failure(self, req):
    if self._transient_fail_count:
      self._transient_fail_count -= 1
      raise random.choice(self._retriable_errors)
    else:
      return datastore_pb2.RunQueryResponse()

  def non_retriable_datastore_failure(self, req):
    raise random.choice(self._non_retriable_errors)

  def test_query_iterator(self):
    self._mock_datastore.run_query.side_effect = (
        self.permanent_retriable_datastore_failure)
    query_iterator = helper.QueryIterator("project", None, self._query,
                                          self._mock_datastore)
    self.assertRaises(RPCError, iter(query_iterator).next)
    self.assertEqual(6, len(self._mock_datastore.run_query.call_args_list))

  def test_query_iterator_with_transient_failures(self):
    self._mock_datastore.run_query.side_effect = (
        self.transient_retriable_datastore_failure)
    query_iterator = helper.QueryIterator("project", None, self._query,
                                          self._mock_datastore)
    fail_count = 5
    self._transient_fail_count = fail_count
    for _ in query_iterator:
      pass

    self.assertEqual(fail_count + 1,
                     len(self._mock_datastore.run_query.call_args_list))

  def test_query_iterator_with_non_retriable_failures(self):
    self._mock_datastore.run_query.side_effect = (
        self.non_retriable_datastore_failure)
    query_iterator = helper.QueryIterator("project", None, self._query,
                                          self._mock_datastore)
    self.assertRaises(tuple(map(type, self._non_retriable_errors)),
                      iter(query_iterator).next)
    self.assertEqual(1, len(self._mock_datastore.run_query.call_args_list))

  def test_query_iterator_with_single_batch(self):
    num_entities = 100
    batch_size = 500
    self.check_query_iterator(num_entities, batch_size, self._query)

  def test_query_iterator_with_multiple_batches(self):
    num_entities = 1098
    batch_size = 500
    self.check_query_iterator(num_entities, batch_size, self._query)

  def test_query_iterator_with_exact_batch_multiple(self):
    num_entities = 1000
    batch_size = 500
    self.check_query_iterator(num_entities, batch_size, self._query)

  def test_query_iterator_with_query_limit(self):
    num_entities = 1098
    batch_size = 500
    self._query.limit.value = 1004
    self.check_query_iterator(num_entities, batch_size, self._query)

  def test_query_iterator_with_large_query_limit(self):
    num_entities = 1098
    batch_size = 500
    self._query.limit.value = 10000
    self.check_query_iterator(num_entities, batch_size, self._query)

  def check_query_iterator(self, num_entities, batch_size, query):
    """A helper method to test the QueryIterator.

    Args:
      num_entities: number of entities contained in the fake datastore.
      batch_size: the number of entities returned by fake datastore in one req.
      query: the query to be executed

    """
    entities = fake_datastore.create_entities(num_entities)
    self._mock_datastore.run_query.side_effect = \
        fake_datastore.create_run_query(entities, batch_size)
    query_iterator = helper.QueryIterator("project", None, self._query,
                                          self._mock_datastore)

    i = 0
    for entity in query_iterator:
      self.assertEqual(entity, entities[i].entity)
      i += 1

    limit = query.limit.value if query.HasField('limit') else sys.maxsize
    self.assertEqual(i, min(num_entities, limit))

  def test_is_key_valid(self):
    key = entity_pb2.Key()
    # Complete with name, no ancestor
    datastore_helper.add_key_path(key, 'kind', 'name')
    self.assertTrue(helper.is_key_valid(key))

    key = entity_pb2.Key()
    # Complete with id, no ancestor
    datastore_helper.add_key_path(key, 'kind', 12)
    self.assertTrue(helper.is_key_valid(key))

    key = entity_pb2.Key()
    # Incomplete, no ancestor
    datastore_helper.add_key_path(key, 'kind')
    self.assertFalse(helper.is_key_valid(key))

    key = entity_pb2.Key()
    # Complete with name and ancestor
    datastore_helper.add_key_path(key, 'kind', 'name', 'kind2', 'name2')
    self.assertTrue(helper.is_key_valid(key))

    key = entity_pb2.Key()
    # Complete with id and ancestor
    datastore_helper.add_key_path(key, 'kind', 'name', 'kind2', 123)
    self.assertTrue(helper.is_key_valid(key))

    key = entity_pb2.Key()
    # Incomplete with ancestor
    datastore_helper.add_key_path(key, 'kind', 'name', 'kind2')
    self.assertFalse(helper.is_key_valid(key))

    key = entity_pb2.Key()
    self.assertFalse(helper.is_key_valid(key))

  def test_compare_path_with_different_kind(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy1'

    p2 = Key.PathElement()
    p2.kind = 'dummy2'

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_compare_path_with_different_id(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy'
    p1.id = 10

    p2 = Key.PathElement()
    p2.kind = 'dummy'
    p2.id = 15

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_compare_path_with_different_name(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy'
    p1.name = "dummy1"

    p2 = Key.PathElement()
    p2.kind = 'dummy'
    p2.name = 'dummy2'

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_compare_path_of_different_type(self):
    p1 = Key.PathElement()
    p1.kind = 'dummy'
    p1.id = 10

    p2 = Key.PathElement()
    p2.kind = 'dummy'
    p2.name = 'dummy'

    self.assertLess(helper.compare_path(p1, p2), 0)

  def test_key_comparator_with_different_partition(self):
    k1 = Key()
    k1.partition_id.namespace_id = 'dummy1'
    k2 = Key()
    k2.partition_id.namespace_id = 'dummy2'
    self.assertRaises(ValueError, helper.key_comparator, k1, k2)

  def test_key_comparator_with_single_path(self):
    k1 = Key()
    k2 = Key()
    p1 = k1.path.add()
    p2 = k2.path.add()
    p1.kind = p2.kind = 'dummy'
    self.assertEqual(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_1(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p12 = k1.path.add()
    p21 = k2.path.add()
    p11.kind = p12.kind = p21.kind = 'dummy'
    self.assertGreater(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_2(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p21 = k2.path.add()
    p22 = k2.path.add()
    p11.kind = p21.kind = p22.kind = 'dummy'
    self.assertLess(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_3(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p12 = k1.path.add()
    p21 = k2.path.add()
    p22 = k2.path.add()
    p11.kind = p12.kind = p21.kind = p22.kind = 'dummy'
    self.assertEqual(helper.key_comparator(k1, k2), 0)

  def test_key_comparator_with_multiple_paths_4(self):
    k1 = Key()
    k2 = Key()
    p11 = k1.path.add()
    p12 = k2.path.add()
    p21 = k2.path.add()
    p11.kind = p12.kind = 'dummy'
    # make path2 greater than path1
    p21.kind = 'dummy1'
    self.assertLess(helper.key_comparator(k1, k2), 0)


if __name__ == '__main__':
  unittest.main()
