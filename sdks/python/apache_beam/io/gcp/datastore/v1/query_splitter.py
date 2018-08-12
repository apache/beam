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

"""Implements a Cloud Datastore query splitter."""
from __future__ import absolute_import
from __future__ import division

from builtins import range
from builtins import round

from apache_beam.io.gcp.datastore.v1 import helper

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from google.cloud.proto.datastore.v1 import query_pb2
  from google.cloud.proto.datastore.v1.query_pb2 import PropertyFilter
  from google.cloud.proto.datastore.v1.query_pb2 import CompositeFilter
  from googledatastore import helper as datastore_helper
  UNSUPPORTED_OPERATORS = [PropertyFilter.LESS_THAN,
                           PropertyFilter.LESS_THAN_OR_EQUAL,
                           PropertyFilter.GREATER_THAN,
                           PropertyFilter.GREATER_THAN_OR_EQUAL]
except ImportError:
  UNSUPPORTED_OPERATORS = None
# pylint: enable=wrong-import-order, wrong-import-position


__all__ = [
    'get_splits',
]

SCATTER_PROPERTY_NAME = '__scatter__'
KEY_PROPERTY_NAME = '__key__'
# The number of keys to sample for each split.
KEYS_PER_SPLIT = 32


def get_splits(datastore, query, num_splits, partition=None):
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
    datastore: the datastore client.
    query: the query to split.
    num_splits: the desired number of splits.
    partition: the partition the query is running in.

  Returns:
    A list of split queries, of a max length of `num_splits`
  """

  # Validate that the number of splits is not out of bounds.
  if num_splits < 1:
    raise ValueError('The number of splits must be greater than 0.')

  if num_splits == 1:
    return [query]

  _validate_query(query)

  splits = []
  scatter_keys = _get_scatter_keys(datastore, query, num_splits, partition)
  last_key = None
  for next_key in _get_split_key(scatter_keys, num_splits):
    splits.append(_create_split(last_key, next_key, query))
    last_key = next_key

  splits.append(_create_split(last_key, None, query))
  return splits


def _validate_query(query):
  """ Verifies that the given query can be properly scattered."""

  if len(query.kind) != 1:
    raise ValueError('Query must have exactly one kind.')

  if query.order:
    raise ValueError('Query cannot have any sort orders.')

  if query.HasField('limit'):
    raise ValueError('Query cannot have a limit set.')

  if query.offset > 0:
    raise ValueError('Query cannot have an offset set.')

  _validate_filter(query.filter)


def _validate_filter(filter):
  """Validates that we only have allowable filters.

  Note that equality and ancestor filters are allowed, however they may result
  in inefficient sharding.
  """

  if filter.HasField('composite_filter'):
    for sub_filter in filter.composite_filter.filters:
      _validate_filter(sub_filter)
  elif filter.HasField('property_filter'):
    if filter.property_filter.op in UNSUPPORTED_OPERATORS:
      raise ValueError('Query cannot have any inequality filters.')
  else:
    pass


def _create_scatter_query(query, num_splits):
  """Creates a scatter query from the given user query."""

  scatter_query = query_pb2.Query()
  for kind in query.kind:
    scatter_kind = scatter_query.kind.add()
    scatter_kind.CopyFrom(kind)

  # ascending order
  datastore_helper.add_property_orders(scatter_query, SCATTER_PROPERTY_NAME)

  # There is a split containing entities before and after each scatter entity:
  # ||---*------*------*------*------*------*------*---||  * = scatter entity
  # If we represent each split as a region before a scatter entity, there is an
  # extra region following the last scatter point. Thus, we do not need the
  # scatter entity for the last region.
  scatter_query.limit.value = (num_splits - 1) * KEYS_PER_SPLIT
  datastore_helper.add_projection(scatter_query, KEY_PROPERTY_NAME)

  return scatter_query


def _get_scatter_keys(datastore, query, num_splits, partition):
  """Gets a list of split keys given a desired number of splits.

  This list will contain multiple split keys for each split. Only a single split
  key will be chosen as the split point, however providing multiple keys allows
  for more uniform sharding.

  Args:
    numSplits: the number of desired splits.
    query: the user query.
    partition: the partition to run the query in.
    datastore: the client to datastore containing the data.

  Returns:
    A list of scatter keys returned by Datastore.
  """
  scatter_point_query = _create_scatter_query(query, num_splits)

  key_splits = []
  while True:
    req = datastore_pb2.RunQueryRequest()
    if partition:
      req.partition_id.CopyFrom(partition)

    req.query.CopyFrom(scatter_point_query)

    resp = datastore.run_query(req)
    for entity_result in resp.batch.entity_results:
      key_splits.append(entity_result.entity.key)

    if resp.batch.more_results != query_pb2.QueryResultBatch.NOT_FINISHED:
      break

    scatter_point_query.start_cursor = resp.batch.end_cursor
    scatter_point_query.limit.value -= len(resp.batch.entity_results)

  key_splits.sort(helper.key_comparator)
  return key_splits


def _get_split_key(keys, num_splits):
  """Given a list of keys and a number of splits find the keys to split on.

  Args:
    keys: the list of keys.
    num_splits: the number of splits.

  Returns:
    A list of keys to split on.

  """

  # If the number of keys is less than the number of splits, we are limited
  # in the number of splits we can make.
  if not keys or (len(keys) < (num_splits - 1)):
    return keys

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

  num_keys_per_split = max(1.0, float(len(keys)) / (num_splits - 1))

  split_keys = []

  # Grab the last sample for each split, otherwise the first split will be too
  # small.
  for i in range(1, num_splits):
    split_index = int(round(i * num_keys_per_split) - 1)
    split_keys.append(keys[split_index])

  return split_keys


def _create_split(last_key, next_key, query):
  """Create a new {@link Query} given the query and range..

  Args:
    last_key: the previous key. If null then assumed to be the beginning.
    next_key: the next key. If null then assumed to be the end.
    query: the desired query.

  Returns:
    A split query with fetches entities in the range [last_key, next_key)
  """
  if not (last_key or next_key):
    return query

  split_query = query_pb2.Query()
  split_query.CopyFrom(query)
  composite_filter = split_query.filter.composite_filter
  composite_filter.op = CompositeFilter.AND

  if query.HasField('filter'):
    composite_filter.filters.add().CopyFrom(query.filter)

  if last_key:
    lower_bound = composite_filter.filters.add()
    lower_bound.property_filter.property.name = KEY_PROPERTY_NAME
    lower_bound.property_filter.op = PropertyFilter.GREATER_THAN_OR_EQUAL
    lower_bound.property_filter.value.key_value.CopyFrom(last_key)

  if next_key:
    upper_bound = composite_filter.filters.add()
    upper_bound.property_filter.property.name = KEY_PROPERTY_NAME
    upper_bound.property_filter.op = PropertyFilter.LESS_THAN
    upper_bound.property_filter.value.key_value.CopyFrom(next_key)

  return split_query
