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

"""
Implements a Cloud Datastore query splitter.

For internal use only. No backwards compatibility guarantees.
"""
# pytype: skip-file

from apache_beam.io.gcp.datastore.v1new import types
from apache_beam.options.value_provider import ValueProvider

__all__ = ['QuerySplitterError', 'SplitNotPossibleError', 'get_splits']

SCATTER_PROPERTY_NAME = '__scatter__'
KEY_PROPERTY_NAME = '__key__'
# The number of keys to sample for each split.
KEYS_PER_SPLIT = 32


class QuerySplitterError(Exception):
  """Top-level error type."""


class SplitNotPossibleError(QuerySplitterError):
  """Raised when some parameter of the query does not allow splitting."""


def get_splits(client, query, num_splits):
  """Returns a list of sharded queries for the given Cloud Datastore query.

  This will create up to the desired number of splits, however it may return
  less splits if the desired number of splits is unavailable. This will happen
  if the number of split points provided by the underlying Datastore is less
  than the desired number, which will occur if the number of results for the
  query is too small.

  This implementation of the QuerySplitter uses the __scatter__ property to
  gather random split points for a query.

  Note: This implementation is derived from the java query splitter in
  https://github.com/GoogleCloudPlatform/google-cloud-datastore/blob/master/java/datastore/src/main/java/com/google/datastore/v1/client/QuerySplitterImpl.java

  Args:
    client: the datastore client.
    query: the query to split.
    num_splits: the desired number of splits.

  Returns:
    A list of split queries, of a max length of `num_splits`

  Raises:
    QuerySplitterError: if split could not be performed owing to query or split
      parameters.
  """
  if num_splits <= 1:
    raise SplitNotPossibleError('num_splits must be > 1, got: %d' % num_splits)
  validate_split(query)

  splits = []
  client_scatter_keys = _get_scatter_keys(client, query, num_splits)
  last_client_key = None
  for next_client_key in _get_split_key(client_scatter_keys, num_splits):
    splits.append(_create_split(last_client_key, next_client_key, query))
    last_client_key = next_client_key

  splits.append(_create_split(last_client_key, None, query))
  return splits


def validate_split(query):
  """
  Verifies that the given query can be properly scattered.

  Note that equality and ancestor filters are allowed, however they may result
  in inefficient sharding.

  Raises:
    QuerySplitterError if split could not be performed owing to query
      parameters.
  """
  if query.order:
    raise SplitNotPossibleError('Query cannot have any sort orders.')

  if query.limit is not None:
    raise SplitNotPossibleError('Query cannot have a limit set.')

  for filter in query.filters:
    if isinstance(filter[1], ValueProvider):
      filter_operator = filter[1].get()
    else:
      filter_operator = filter[1]
    if filter_operator in ['<', '<=', '>', '>=']:
      raise SplitNotPossibleError('Query cannot have any inequality filters.')


def _create_scatter_query(query, num_splits):
  """Creates a scatter query from the given user query."""
  # There is a split containing entities before and after each scatter entity:
  # ||---*------*------*------*------*------*------*---||  * = scatter entity
  # If we represent each split as a region before a scatter entity, there is an
  # extra region following the last scatter point. Thus, we do not need the
  # scatter entity for the last region.
  limit = (num_splits - 1) * KEYS_PER_SPLIT
  scatter_query = types.Query(
      kind=query.kind,
      project=query.project,
      namespace=query.namespace,
      order=[SCATTER_PROPERTY_NAME],
      projection=[KEY_PROPERTY_NAME],
      limit=limit)
  return scatter_query


class IdOrName(object):
  """Represents an ID or name of a Datastore key,

   Implements sort ordering: by ID, then by name, keys with IDs before those
   with names.
   """
  def __init__(self, id_or_name):
    self.id_or_name = id_or_name
    if isinstance(id_or_name, str):
      self.id = None
      self.name = id_or_name
    elif isinstance(id_or_name, int):
      self.id = id_or_name
      self.name = None
    else:
      raise TypeError('Unexpected type of id_or_name: %s' % id_or_name)

  def __lt__(self, other):
    if not isinstance(other, IdOrName):
      return super().__lt__(other)

    if self.id is not None:
      if other.id is None:
        return True
      else:
        return self.id < other.id

    if other.id is not None:
      return False

    return self.name < other.name

  def __eq__(self, other):
    if not isinstance(other, IdOrName):
      return super().__eq__(other)
    return self.id == other.id and self.name == other.name

  def __hash__(self):
    return hash((self.id, self.other))


def client_key_sort_key(client_key):
  """Key function for sorting lists of ``google.cloud.datastore.key.Key``."""
  sort_key = [client_key.project, client_key.namespace or '']
  # A key path is made up of (kind, id_or_name) pairs. The last pair might be
  # missing an id_or_name.
  flat_path = list(client_key.flat_path)
  while flat_path:
    sort_key.append(flat_path.pop(0))  # kind
    if flat_path:
      sort_key.append(IdOrName(flat_path.pop(0)))

  return sort_key


def _get_scatter_keys(client, query, num_splits):
  """Gets a list of split keys given a desired number of splits.

  This list will contain multiple split keys for each split. Only a single split
  key will be chosen as the split point, however providing multiple keys allows
  for more uniform sharding.

  Args:
    client: the client to datastore containing the data.
    query: the user query.
    num_splits: the number of desired splits.

  Returns:
    A list of scatter keys returned by Datastore.
  """
  scatter_point_query = _create_scatter_query(query, num_splits)
  client_query = scatter_point_query._to_client_query(client)
  client_key_splits = [
      client_entity.key for client_entity in client_query.fetch(
          client=client, limit=scatter_point_query.limit)
  ]
  client_key_splits.sort(key=client_key_sort_key)
  return client_key_splits


def _get_split_key(client_keys, num_splits):
  """Given a list of keys and a number of splits find the keys to split on.

  Args:
    client_keys: the list of keys.
    num_splits: the number of splits.

  Returns:
    A list of keys to split on.

  """

  # If the number of keys is less than the number of splits, we are limited
  # in the number of splits we can make.
  if not client_keys or (len(client_keys) < (num_splits - 1)):
    return client_keys

  # Calculate the number of keys per split. This should be KEYS_PER_SPLIT,
  # but may be less if there are not KEYS_PER_SPLIT * (numSplits - 1) scatter
  # entities.
  #
  # Consider the following dataset, where - represents an entity and
  # * represents an entity that is returned as a scatter entity:
  # ||---*-----*----*-----*-----*------*----*----||
  # If we want 4 splits in this data, the optimal split would look like:
  # ||---*-----*----*-----*-----*------*----*----||
  #            |          |            |
  # The scatter keys in the last region are not useful to us, so we never
  # request them:
  # ||---*-----*----*-----*-----*------*---------||
  #            |          |            |
  # With 6 scatter keys we want to set scatter points at indexes: 1, 3, 5.
  #
  # We keep this as a float so that any "fractional" keys per split get
  # distributed throughout the splits and don't make the last split
  # significantly larger than the rest.

  num_keys_per_split = max(1.0, float(len(client_keys)) / (num_splits - 1))

  split_client_keys = []

  # Grab the last sample for each split, otherwise the first split will be too
  # small.
  for i in range(1, num_splits):
    split_index = int(round(i * num_keys_per_split) - 1)
    split_client_keys.append(client_keys[split_index])

  return split_client_keys


def _create_split(last_client_key, next_client_key, query):
  """Create a new {@link Query} given the query and range.

  Args:
    last_client_key: the previous key. If null then assumed to be the beginning.
    next_client_key: the next key. If null then assumed to be the end.
    query: query to base the split query on.

  Returns:
    A split query with fetches entities in the range [last_key, next_client_key)
  """
  if not (last_client_key or next_client_key):
    return query

  split_query = query.clone()
  # Copy filters and possible convert the default empty tuple to empty list.
  filters = list(split_query.filters)

  if last_client_key:
    filters.append((KEY_PROPERTY_NAME, '>=', last_client_key))
  if next_client_key:
    filters.append((KEY_PROPERTY_NAME, '<', next_client_key))

  split_query.filters = filters
  return split_query
