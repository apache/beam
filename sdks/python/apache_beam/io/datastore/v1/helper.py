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

"""Cloud Datastore helper functions."""
import sys

from apache_beam.utils import retry
from google.datastore.v1 import datastore_pb2
from google.datastore.v1 import entity_pb2
from google.datastore.v1 import query_pb2
import googledatastore
from googledatastore.connection import Datastore
from googledatastore.connection import RPCError


def key_comparator(k1, k2):
  """A comparator for Datastore keys.

  Comparison is only valid for keys in the same partition. The comparison here
  is between the list of paths for each key.
  """

  if k1.partition_id != k2.partition_id:
    raise ValueError('Cannot compare keys with different partition ids.')

  k2_iter = iter(k2.path)

  for k1_path in k1.path:
    k2_path = next(k2_iter, None)
    if not k2_path:
      return 1

    result = compare_path(k1_path, k2_path)

    if result != 0:
      return result

  k2_path = next(k2_iter, None)
  if k2_path:
    return -1
  else:
    return 0


def compare_path(p1, p2):
  """A comparator for key path.

  A path has either an `id` or a `name` field defined. The
  comparison works with the following rules:

  1. If one path has `id` defined while the other doesn't, then the
  one with `id` defined is considered smaller.
  2. If both paths have `id` defined, then their ids are compared.
  3. If no `id` is defined for both paths, then their `names` are compared.
  """

  result = str_compare(p1.kind, p2.kind)
  if result != 0:
    return result

  if p1.HasField('id'):
    if not p2.HasField('id'):
      return -1

    return p1.id - p2.id

  if p2.HasField('id'):
    return 1

  return str_compare(p1.name, p2.name)


def str_compare(s1, s2):
  if s1 == s2:
    return 0
  elif s1 < s2:
    return -1
  else:
    return 1


def get_datastore(project):
  """Returns a Cloud Datastore client."""

  credentials = googledatastore.helper.get_credentials_from_env()
  datastore = Datastore(project, credentials)
  return datastore


def make_request(project, namespace, query):
  """Make a Cloud Datastore request for the given query."""

  req = datastore_pb2.RunQueryRequest()
  req.partition_id.CopyFrom(make_partition(project, namespace))

  req.query.CopyFrom(query)
  return req


def make_partition(project, namespace):
  partition = entity_pb2.PartitionId()
  partition.project_id = project
  if namespace is not None:
    partition.namespace_id = namespace

  return partition


def retry_on_rpc_error(exception):
  """A retry filter for Cloud Datastore RPCErrors."""

  if isinstance(exception, RPCError):
    if exception.code >= 500:
      return True
    else:
      return False
  else:
    # TODO(vikasrk): Figure out what other errors should be retried.
    return False


def fetch_entities(project, namespace, query, datastore):
  """ A helper method to fetch entities from Cloud Datastore.

  Args:
    project:
    namespace:
    query:
    datastore:

  Returns:
    An iterator of entities.
  """
  return QueryIterator(project, namespace, query, datastore)


class QueryIterator(object):
  """A iterator class for entities of a given query.

  Entities are read in batches. Retries on failures.
  """

  _NOT_FINISHED = query_pb2.QueryResultBatch.NOT_FINISHED

  _BATCH_SIZE = 500

  def __init__(self, project, namespace, query, datastore):
    self._query = query
    self._datastore = datastore
    self._project = project
    self._namespace = namespace
    self._start_cursor = None
    self._limit = self._query.limit.value or sys.maxint
    self._req = make_request(project, namespace, query)

  @retry.with_exponential_backoff(num_retries=5,
                                  retry_filter=retry_on_rpc_error)
  def _next_batch(self):
    """Fetches the next batch of entities."""

    if self._start_cursor is not None:
      self._req.query.start_cursor = self._start_cursor

    # set batch size
    self._req.query.limit.value = min(self._BATCH_SIZE, self._limit)
    resp = self._datastore.run_query(self._req)
    return resp

  def __iter__(self):
    more_results = True
    while more_results:
      resp = self._next_batch()
      for entity_result in resp.batch.entity_results:
        yield entity_result.entity

      self._start_cursor = resp.batch.end_cursor
      num_results = len(resp.batch.entity_results)
      self._limit -= num_results

      # TODO: Add comments
      more_results = (self._limit > 0) and \
                     ((num_results == self._BATCH_SIZE) or
                      (resp.batch.more_results == self._NOT_FINISHED))
