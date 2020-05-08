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

# pytype: skip-file

from __future__ import absolute_import

import unittest

# patches unittest.TestCase to be python3 compatible
import future.tests.base  # pylint: disable=unused-import
import mock

# Protect against environments where datastore library is not available.
try:
  from apache_beam.io.gcp.datastore.v1new import helper
  from apache_beam.io.gcp.datastore.v1new import query_splitter
  from apache_beam.io.gcp.datastore.v1new import types
  from apache_beam.io.gcp.datastore.v1new.query_splitter import SplitNotPossibleError
  from google.cloud.datastore import key
except ImportError:
  query_splitter = None  # type: ignore


@unittest.skipIf(query_splitter is None, 'GCP dependencies are not installed')
class QuerySplitterTest(unittest.TestCase):
  _PROJECT = 'project'
  _NAMESPACE = 'namespace'

  def create_query(
      self,
      kinds=(),
      order=False,
      limit=None,
      offset=None,
      inequality_filter=False):

    kind = None
    filters = []
    if kinds:
      kind = kinds[0]
    if order:
      order = ['prop1']
    if inequality_filter:
      filters = [('prop1', '>', 'value1')]

    return types.Query(kind=kind, filters=filters, order=order, limit=limit)

  def test_get_splits_query_with_order(self):
    query = self.create_query(kinds=['a'], order=True)
    with self.assertRaisesRegex(SplitNotPossibleError, r'sort orders'):
      query_splitter.get_splits(None, query, 3)

  def test_get_splits_query_with_unsupported_filter(self):
    query = self.create_query(kinds=['a'], inequality_filter=True)
    with self.assertRaisesRegex(SplitNotPossibleError, r'inequality filters'):
      query_splitter.get_splits(None, query, 2)

  def test_get_splits_query_with_limit(self):
    query = self.create_query(kinds=['a'], limit=10)
    with self.assertRaisesRegex(SplitNotPossibleError, r'limit set'):
      query_splitter.get_splits(None, query, 2)

  def test_get_splits_query_with_num_splits_of_one(self):
    query = self.create_query()
    with self.assertRaisesRegex(SplitNotPossibleError, r'num_splits'):
      query_splitter.get_splits(None, query, 1)

  def test_create_scatter_query(self):
    query = types.Query(kind='shakespeare-demo')
    num_splits = 10
    scatter_query = query_splitter._create_scatter_query(query, num_splits)
    self.assertEqual(scatter_query.kind, query.kind)
    self.assertEqual(
        scatter_query.limit, (num_splits - 1) * query_splitter.KEYS_PER_SPLIT)
    self.assertEqual(
        scatter_query.order, [query_splitter.SCATTER_PROPERTY_NAME])
    self.assertEqual(
        scatter_query.projection, [query_splitter.KEY_PROPERTY_NAME])

  def check_get_splits(self, query, num_splits, num_entities):
    """A helper method to test the query_splitter get_splits method.

    Args:
      query: the query to be split
      num_splits: number of splits
      num_entities: number of scatter entities returned to the splitter.
    """
    # Test for random long ids, string ids, and a mix of both.
    for id_or_name in [True, False, None]:
      if id_or_name is None:
        client_entities = helper.create_client_entities(num_entities, False)
        client_entities.extend(
            helper.create_client_entities(num_entities, True))
        num_entities *= 2
      else:
        client_entities = helper.create_client_entities(
            num_entities, id_or_name)

      mock_client = mock.MagicMock()
      mock_client_query = mock.MagicMock()
      mock_client_query.fetch.return_value = client_entities
      with mock.patch.object(types.Query,
                             '_to_client_query',
                             return_value=mock_client_query):
        split_queries = query_splitter.get_splits(
            mock_client, query, num_splits)

      mock_client_query.fetch.assert_called_once()
      # if request num_splits is greater than num_entities, the best it can
      # do is one entity per split.
      expected_num_splits = min(num_splits, num_entities + 1)
      self.assertEqual(len(split_queries), expected_num_splits)

      # Verify no gaps in key ranges. Filters should look like:
      # query1: (__key__ < key1)
      # query2: (__key__ >= key1), (__key__ < key2)
      # ...
      # queryN: (__key__ >=keyN-1)
      prev_client_key = None
      last_query_seen = False
      for split_query in split_queries:
        self.assertFalse(last_query_seen)
        lt_key = None
        gte_key = None
        for _filter in split_query.filters:
          self.assertEqual(query_splitter.KEY_PROPERTY_NAME, _filter[0])
          if _filter[1] == '<':
            lt_key = _filter[2]
          elif _filter[1] == '>=':
            gte_key = _filter[2]

        # Case where the scatter query has no results.
        if lt_key is None and gte_key is None:
          self.assertEqual(1, len(split_queries))
          break

        if prev_client_key is None:
          self.assertIsNone(gte_key)
          self.assertIsNotNone(lt_key)
          prev_client_key = lt_key
        else:
          self.assertEqual(prev_client_key, gte_key)
          prev_client_key = lt_key
          if lt_key is None:
            last_query_seen = True

  def test_get_splits_with_two_splits(self):
    query = self.create_query(kinds=['shakespeare-demo'])
    num_splits = 2
    num_entities = 97

    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_multiple_splits(self):
    query = self.create_query(kinds=['shakespeare-demo'])
    num_splits = 4
    num_entities = 369

    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_large_num_splits(self):
    query = self.create_query(kinds=['shakespeare-demo'])
    num_splits = 10
    num_entities = 4

    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_small_num_entities(self):
    query = self.create_query(kinds=['shakespeare-demo'])
    num_splits = 4
    num_entities = 50

    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_batch_size_exact_multiple(self):
    """Test get_splits when num scatter keys is a multiple of batch size."""
    query = self.create_query(kinds=['shakespeare-demo'])
    num_splits = 4
    num_entities = 400

    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_large_batch_size(self):
    """Test get_splits when all scatter keys are retured in a single req."""
    query = self.create_query(kinds=['shakespeare-demo'])
    num_splits = 4
    num_entities = 400

    self.check_get_splits(query, num_splits, num_entities)

  def test_get_splits_with_num_splits_gt_entities(self):
    query = self.create_query(kinds=['shakespeare-demo'])
    num_splits = 10
    num_entities = 4

    self.check_get_splits(query, num_splits, num_entities)

  def test_id_or_name(self):
    id_ = query_splitter.IdOrName(1)
    self.assertEqual(1, id_.id)
    self.assertIsNone(id_.name)
    name = query_splitter.IdOrName('1')
    self.assertIsNone(name.id)
    self.assertEqual('1', name.name)
    self.assertEqual(query_splitter.IdOrName(1), query_splitter.IdOrName(1))
    self.assertEqual(query_splitter.IdOrName('1'), query_splitter.IdOrName('1'))
    self.assertLess(query_splitter.IdOrName(2), query_splitter.IdOrName('1'))
    self.assertLess(query_splitter.IdOrName(1), query_splitter.IdOrName(2))
    self.assertLess(query_splitter.IdOrName('1'), query_splitter.IdOrName('2'))

  def test_client_key_sort_key(self):
    k = key.Key('kind1', 1, project=self._PROJECT, namespace=self._NAMESPACE)
    k2 = key.Key('kind2', 'a', parent=k)
    k3 = key.Key('kind2', 'b', parent=k)
    k4 = key.Key('kind1', 'a', project=self._PROJECT, namespace=self._NAMESPACE)
    k5 = key.Key('kind1', 'a', project=self._PROJECT)
    keys = [k5, k, k4, k3, k2, k2, k]
    expected_sort = [k5, k, k, k2, k2, k3, k4]
    keys.sort(key=query_splitter.client_key_sort_key)
    self.assertEqual(expected_sort, keys)

  def test_client_key_sort_key_ids(self):
    k1 = key.Key('kind', 2, project=self._PROJECT)
    k2 = key.Key('kind', 1, project=self._PROJECT)
    keys = [k1, k2]
    expected_sort = [k2, k1]
    keys.sort(key=query_splitter.client_key_sort_key)
    self.assertEqual(expected_sort, keys)

  def test_client_key_sort_key_names(self):
    k1 = key.Key('kind', '2', project=self._PROJECT)
    k2 = key.Key('kind', '1', project=self._PROJECT)
    keys = [k1, k2]
    expected_sort = [k2, k1]
    keys.sort(key=query_splitter.client_key_sort_key)
    self.assertEqual(expected_sort, keys)

  def test_client_key_sort_key_ids_vs_names(self):
    # Keys with IDs always come before keys with names.
    k1 = key.Key('kind', '1', project=self._PROJECT)
    k2 = key.Key('kind', 2, project=self._PROJECT)
    keys = [k1, k2]
    expected_sort = [k2, k1]
    keys.sort(key=query_splitter.client_key_sort_key)
    self.assertEqual(expected_sort, keys)


if __name__ == '__main__':
  unittest.main()
