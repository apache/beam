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

"""A connector for reading from and writing to Google Cloud Datastore"""
from __future__ import absolute_import
from __future__ import division

import logging
import sys
import time
import warnings
from builtins import object
from builtins import round

from apache_beam.io.gcp.datastore.v1 import helper
from apache_beam.io.gcp.datastore.v1 import query_splitter
from apache_beam.io.gcp.datastore.v1 import util
from apache_beam.io.gcp.datastore.v1.adaptive_throttler import AdaptiveThrottler
from apache_beam.metrics.metric import Metrics
from apache_beam.transforms import Create
from apache_beam.transforms import DoFn
from apache_beam.transforms import FlatMap
from apache_beam.transforms import GroupByKey
from apache_beam.transforms import Map
from apache_beam.transforms import ParDo
from apache_beam.transforms import PTransform
from apache_beam.transforms.util import Values

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from googledatastore import helper as datastore_helper
except ImportError:
  if sys.version_info[0] == 3:
    warnings.warn('Datastore IO will support Python 3 after replacing '
                  'googledatastore by google-cloud-datastore, '
                  'see: BEAM-4543.')
# pylint: enable=wrong-import-order, wrong-import-position


__all__ = ['ReadFromDatastore', 'WriteToDatastore', 'DeleteFromDatastore']


class ReadFromDatastore(PTransform):
  """A ``PTransform`` for reading from Google Cloud Datastore.

  To read a ``PCollection[Entity]`` from a Cloud Datastore ``Query``, use
  ``ReadFromDatastore`` transform by providing a `project` id and a `query` to
  read from. You can optionally provide a `namespace` and/or specify how many
  splits you want for the query through `num_splits` option.

  Note: Normally, a runner will read from Cloud Datastore in parallel across
  many workers. However, when the `query` is configured with a `limit` or if the
  query contains inequality filters like `GREATER_THAN, LESS_THAN` etc., then
  all the returned results will be read by a single worker in order to ensure
  correct data. Since data is read from a single worker, this could have
  significant impact on the performance of the job.

  The semantics for the query splitting is defined below:
    1. If `num_splits` is equal to 0, then the number of splits will be chosen
    dynamically at runtime based on the query data size.

    2. Any value of `num_splits` greater than
    `ReadFromDatastore._NUM_QUERY_SPLITS_MAX` will be capped at that value.

    3. If the `query` has a user limit set, or contains inequality filters, then
    `num_splits` will be ignored and no split will be performed.

    4. Under certain cases Cloud Datastore is unable to split query to the
    requested number of splits. In such cases we just use whatever the Cloud
    Datastore returns.

  See https://developers.google.com/datastore/ for more details on Google Cloud
  Datastore.
  """

  # An upper bound on the number of splits for a query.
  _NUM_QUERY_SPLITS_MAX = 50000
  # A lower bound on the number of splits for a query. This is to ensure that
  # we parellelize the query even when Datastore statistics are not available.
  _NUM_QUERY_SPLITS_MIN = 12
  # Default bundle size of 64MB.
  _DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024

  def __init__(self, project, query, namespace=None, num_splits=0):
    """Initialize the `ReadFromDatastore` transform.

    Args:
      project: The ID of the project to read from.
      query: Cloud Datastore query to be read from.
      namespace: An optional namespace.
      num_splits: Number of splits for the query.
    """

    # Import here to avoid adding the dependency for local running scenarios.
    try:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apitools.base import py  # pylint: disable=unused-variable
    except ImportError:
      raise ImportError(
          'Google Cloud IO not available, '
          'please install apache_beam[gcp]')

    logging.warning('datastoreio read transform is experimental.')
    super(ReadFromDatastore, self).__init__()

    if not project:
      raise ValueError("Project cannot be empty")
    if not query:
      raise ValueError("Query cannot be empty")
    if num_splits < 0:
      raise ValueError("num_splits must be greater than or equal 0")

    self._project = project
    # using _namespace conflicts with DisplayData._namespace
    self._datastore_namespace = namespace
    self._query = query
    self._num_splits = num_splits

  def expand(self, pcoll):
    # This is a composite transform involves the following:
    #   1. Create a singleton of the user provided `query` and apply a ``ParDo``
    #   that splits the query into `num_splits` and assign each split query a
    #   unique `int` as the key. The resulting output is of the type
    #   ``PCollection[(int, Query)]``.
    #
    #   If the value of `num_splits` is less than or equal to 0, then the
    #   number of splits will be computed dynamically based on the size of the
    #   data for the `query`.
    #
    #   2. The resulting ``PCollection`` is sharded using a ``GroupByKey``
    #   operation. The queries are extracted from the (int, Iterable[Query]) and
    #   flattened to output a ``PCollection[Query]``.
    #
    #   3. In the third step, a ``ParDo`` reads entities for each query and
    #   outputs a ``PCollection[Entity]``.

    queries = (pcoll.pipeline
               | 'UserQuery' >> Create([self._query])
               | 'SplitQuery' >> ParDo(ReadFromDatastore.SplitQueryFn(
                   self._project, self._query, self._datastore_namespace,
                   self._num_splits)))

    sharded_queries = (queries
                       | GroupByKey()
                       | Values()
                       | 'Flatten' >> FlatMap(lambda x: x))

    entities = sharded_queries | 'Read' >> ParDo(
        ReadFromDatastore.ReadFn(self._project, self._datastore_namespace))
    return entities

  def display_data(self):
    disp_data = {'project': self._project,
                 'query': str(self._query),
                 'num_splits': self._num_splits}

    if self._datastore_namespace is not None:
      disp_data['namespace'] = self._datastore_namespace

    return disp_data

  class SplitQueryFn(DoFn):
    """A `DoFn` that splits a given query into multiple sub-queries."""
    def __init__(self, project, query, namespace, num_splits):
      super(ReadFromDatastore.SplitQueryFn, self).__init__()
      self._datastore = None
      self._project = project
      self._datastore_namespace = namespace
      self._query = query
      self._num_splits = num_splits

    def start_bundle(self):
      self._datastore = helper.get_datastore(self._project)

    def process(self, query, *args, **kwargs):
      # distinct key to be used to group query splits.
      key = 1

      # If query has a user set limit, then the query cannot be split.
      if query.HasField('limit'):
        return [(key, query)]

      # Compute the estimated numSplits if not specified by the user.
      if self._num_splits == 0:
        estimated_num_splits = ReadFromDatastore.get_estimated_num_splits(
            self._project, self._datastore_namespace, self._query,
            self._datastore)
      else:
        estimated_num_splits = self._num_splits

      logging.info("Splitting the query into %d splits", estimated_num_splits)
      try:
        query_splits = query_splitter.get_splits(
            self._datastore, query, estimated_num_splits,
            helper.make_partition(self._project, self._datastore_namespace))
      except Exception:
        logging.warning("Unable to parallelize the given query: %s", query,
                        exc_info=True)
        query_splits = [query]

      sharded_query_splits = []
      for split_query in query_splits:
        sharded_query_splits.append((key, split_query))
        key += 1

      return sharded_query_splits

    def display_data(self):
      disp_data = {'project': self._project,
                   'query': str(self._query),
                   'num_splits': self._num_splits}

      if self._datastore_namespace is not None:
        disp_data['namespace'] = self._datastore_namespace

      return disp_data

  class ReadFn(DoFn):
    """A DoFn that reads entities from Cloud Datastore, for a given query."""
    def __init__(self, project, namespace=None):
      super(ReadFromDatastore.ReadFn, self).__init__()
      self._project = project
      self._datastore_namespace = namespace
      self._datastore = None

    def start_bundle(self):
      self._datastore = helper.get_datastore(self._project)

    def process(self, query, *args, **kwargs):
      # Returns an iterator of entities that reads in batches.
      entities = helper.fetch_entities(self._project, self._datastore_namespace,
                                       query, self._datastore)
      return entities

    def display_data(self):
      disp_data = {'project': self._project}

      if self._datastore_namespace is not None:
        disp_data['namespace'] = self._datastore_namespace

      return disp_data

  @staticmethod
  def query_latest_statistics_timestamp(project, namespace, datastore):
    """Fetches the latest timestamp of statistics from Cloud Datastore.

    Cloud Datastore system tables with statistics are periodically updated.
    This method fethes the latest timestamp (in microseconds) of statistics
    update using the `__Stat_Total__` table.
    """
    query = helper.make_latest_timestamp_query(namespace)
    req = helper.make_request(project, namespace, query)
    resp = datastore.run_query(req)
    if not resp.batch.entity_results:
      raise RuntimeError("Datastore total statistics unavailable.")

    entity = resp.batch.entity_results[0].entity
    return datastore_helper.micros_from_timestamp(
        entity.properties['timestamp'].timestamp_value)

  @staticmethod
  def get_estimated_size_bytes(project, namespace, query, datastore):
    """Get the estimated size of the data returned by the given query.

    Cloud Datastore provides no way to get a good estimate of how large the
    result of a query is going to be. Hence we use the __Stat_Kind__ system
    table to get size of the entire kind as an approximate estimate, assuming
    exactly 1 kind is specified in the query.
    See https://cloud.google.com/datastore/docs/concepts/stats.
    """
    kind = query.kind[0].name
    latest_timestamp = ReadFromDatastore.query_latest_statistics_timestamp(
        project, namespace, datastore)
    logging.info('Latest stats timestamp for kind %s is %s',
                 kind, latest_timestamp)

    kind_stats_query = (
        helper.make_kind_stats_query(namespace, kind, latest_timestamp))

    req = helper.make_request(project, namespace, kind_stats_query)
    resp = datastore.run_query(req)
    if not resp.batch.entity_results:
      raise RuntimeError("Datastore statistics for kind %s unavailable" % kind)

    entity = resp.batch.entity_results[0].entity
    return datastore_helper.get_value(entity.properties['entity_bytes'])

  @staticmethod
  def get_estimated_num_splits(project, namespace, query, datastore):
    """Computes the number of splits to be performed on the given query."""
    try:
      estimated_size_bytes = ReadFromDatastore.get_estimated_size_bytes(
          project, namespace, query, datastore)
      logging.info('Estimated size bytes for query: %s', estimated_size_bytes)
      num_splits = int(min(ReadFromDatastore._NUM_QUERY_SPLITS_MAX, round(
          (float(estimated_size_bytes) /
           ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES))))

    except Exception as e:
      logging.warning('Failed to fetch estimated size bytes: %s', e)
      # Fallback in case estimated size is unavailable.
      num_splits = ReadFromDatastore._NUM_QUERY_SPLITS_MIN

    return max(num_splits, ReadFromDatastore._NUM_QUERY_SPLITS_MIN)


class _Mutate(PTransform):
  """A ``PTransform`` that writes mutations to Cloud Datastore.

  Only idempotent Datastore mutation operations (upsert and delete) are
  supported, as the commits are retried when failures occur.
  """

  _WRITE_BATCH_INITIAL_SIZE = 200
  # Max allowed Datastore writes per batch, and max bytes per batch.
  # Note that the max bytes per batch set here is lower than the 10MB limit
  # actually enforced by the API, to leave space for the CommitRequest wrapper
  # around the mutations.
  _WRITE_BATCH_MAX_SIZE = 500
  _WRITE_BATCH_MAX_BYTES_SIZE = 9000000
  _WRITE_BATCH_MIN_SIZE = 10
  _WRITE_BATCH_TARGET_LATENCY_MS = 5000

  def __init__(self, project, mutation_fn):
    """Initializes a Mutate transform.

     Args:
       project: The Project ID
       mutation_fn: A function that converts `entities` or `keys` to
         `mutations`.
     """
    self._project = project
    self._mutation_fn = mutation_fn
    logging.warning('datastoreio write transform is experimental.')

  def expand(self, pcoll):
    return (pcoll
            | 'Convert to Mutation' >> Map(self._mutation_fn)
            | 'Write Mutation to Datastore' >> ParDo(_Mutate.DatastoreWriteFn(
                self._project)))

  def display_data(self):
    return {'project': self._project,
            'mutation_fn': self._mutation_fn.__class__.__name__}

  class _DynamicBatchSizer(object):
    """Determines request sizes for future Datastore RPCS."""
    def __init__(self):
      self._commit_time_per_entity_ms = util.MovingSum(window_ms=120000,
                                                       bucket_ms=10000)

    def get_batch_size(self, now):
      """Returns the recommended size for datastore RPCs at this time."""
      if not self._commit_time_per_entity_ms.has_data(now):
        return _Mutate._WRITE_BATCH_INITIAL_SIZE

      recent_mean_latency_ms = (self._commit_time_per_entity_ms.sum(now)
                                // self._commit_time_per_entity_ms.count(now))
      return max(_Mutate._WRITE_BATCH_MIN_SIZE,
                 min(_Mutate._WRITE_BATCH_MAX_SIZE,
                     _Mutate._WRITE_BATCH_TARGET_LATENCY_MS
                     // max(recent_mean_latency_ms, 1)
                    ))

    def report_latency(self, now, latency_ms, num_mutations):
      """Reports the latency of an RPC to Datastore.

      Args:
        now: double, completion time of the RPC as seconds since the epoch.
        latency_ms: double, the observed latency in milliseconds for this RPC.
        num_mutations: int, number of mutations contained in the RPC.
      """
      self._commit_time_per_entity_ms.add(now, latency_ms / num_mutations)

  class DatastoreWriteFn(DoFn):
    """A ``DoFn`` that write mutations to Datastore.

    Mutations are written in batches, where the maximum batch size is
    `_Mutate._WRITE_BATCH_SIZE`.

    Commits are non-transactional. If a commit fails because of a conflict over
    an entity group, the commit will be retried. This means that the mutation
    should be idempotent (`upsert` and `delete` mutations) to prevent duplicate
    data or errors.
    """
    def __init__(self, project, fixed_batch_size=None):
      """
      Args:
        project: str, the cloud project id.
        fixed_batch_size: int, for testing only, this forces all batches of
           writes to be a fixed size, for easier unittesting.
      """
      self._project = project
      self._datastore = None
      self._fixed_batch_size = fixed_batch_size
      self._rpc_successes = Metrics.counter(
          _Mutate.DatastoreWriteFn, "datastoreRpcSuccesses")
      self._rpc_errors = Metrics.counter(
          _Mutate.DatastoreWriteFn, "datastoreRpcErrors")
      self._throttled_secs = Metrics.counter(
          _Mutate.DatastoreWriteFn, "cumulativeThrottlingSeconds")
      self._throttler = AdaptiveThrottler(window_ms=120000, bucket_ms=1000,
                                          overload_ratio=1.25)

    def _update_rpc_stats(self, successes=0, errors=0, throttled_secs=0):
      self._rpc_successes.inc(successes)
      self._rpc_errors.inc(errors)
      self._throttled_secs.inc(throttled_secs)

    def start_bundle(self):
      self._mutations = []
      self._mutations_size = 0
      self._datastore = helper.get_datastore(self._project)
      if self._fixed_batch_size:
        self._target_batch_size = self._fixed_batch_size
      else:
        self._batch_sizer = _Mutate._DynamicBatchSizer()
        self._target_batch_size = self._batch_sizer.get_batch_size(
            time.time()*1000)

    def process(self, element):
      size = element.ByteSize()
      if (self._mutations and
          size + self._mutations_size > _Mutate._WRITE_BATCH_MAX_BYTES_SIZE):
        self._flush_batch()
      self._mutations.append(element)
      self._mutations_size += size
      if len(self._mutations) >= self._target_batch_size:
        self._flush_batch()

    def finish_bundle(self):
      if self._mutations:
        self._flush_batch()

    def _flush_batch(self):
      # Flush the current batch of mutations to Cloud Datastore.
      _, latency_ms = helper.write_mutations(
          self._datastore, self._project, self._mutations,
          self._throttler, self._update_rpc_stats,
          throttle_delay=_Mutate._WRITE_BATCH_TARGET_LATENCY_MS//1000)
      logging.debug("Successfully wrote %d mutations in %dms.",
                    len(self._mutations), latency_ms)

      if not self._fixed_batch_size:
        now = time.time()*1000
        self._batch_sizer.report_latency(now, latency_ms, len(self._mutations))
        self._target_batch_size = self._batch_sizer.get_batch_size(now)

      self._mutations = []
      self._mutations_size = 0


class WriteToDatastore(_Mutate):
  """A ``PTransform`` to write a ``PCollection[Entity]`` to Cloud Datastore."""

  def __init__(self, project):
    """Initialize the `WriteToDatastore` transform.

    Args:
      project: The ID of the project to write to.
    """

    # Import here to avoid adding the dependency for local running scenarios.
    try:
      # pylint: disable=wrong-import-order, wrong-import-position
      from apitools.base import py  # pylint: disable=unused-variable
    except ImportError:
      raise ImportError(
          'Google Cloud IO not available, '
          'please install apache_beam[gcp]')

    super(WriteToDatastore, self).__init__(
        project, WriteToDatastore.to_upsert_mutation)

  @staticmethod
  def to_upsert_mutation(entity):
    if not helper.is_key_valid(entity.key):
      raise ValueError('Entities to be written to the Cloud Datastore must '
                       'have complete keys:\n%s' % entity)
    mutation = datastore_pb2.Mutation()
    mutation.upsert.CopyFrom(entity)
    return mutation


class DeleteFromDatastore(_Mutate):
  """A ``PTransform`` to delete a ``PCollection[Key]`` from Cloud Datastore."""
  def __init__(self, project):
    """Initialize the `DeleteFromDatastore` transform.

    Args:
      project: The ID of the project from which the entities will be deleted.
    """

    super(DeleteFromDatastore, self).__init__(
        project, DeleteFromDatastore.to_delete_mutation)

  @staticmethod
  def to_delete_mutation(key):
    if not helper.is_key_valid(key):
      raise ValueError('Keys to be deleted from the Cloud Datastore must be '
                       'complete:\n%s", key')
    mutation = datastore_pb2.Mutation()
    mutation.delete.CopyFrom(key)
    return mutation
