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

import logging

from apache_beam.io.datastore.v1 import helper
from apache_beam.io.datastore.v1 import query_splitter
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import Create
from apache_beam.transforms import FlatMap
from apache_beam.transforms import GroupByKey
from apache_beam.transforms import ParDo
from apache_beam.transforms.util import Values
from googledatastore import helper as datastore_helper
from google.datastore.v1 import query_pb2
from google.datastore.v1.query_pb2 import CompositeFilter
from google.datastore.v1.query_pb2 import PropertyFilter

__all__ = ['ReadFromDatastore']


class ReadFromDatastore(PTransform):
  """A ``PTransform`` for reading from Google Cloud Datastore."""

  _NUM_QUERY_SPLITS_MAX = 50000
  _NUM_QUERY_SPLITS_MIN = 12
  _DEFAULT_BUNDLE_SIZE_BYTES = 64 * 1024 * 1024

  def __init__(self, project, query, namespace=None):
    """Initialize the ReadFromDatastore transform.

    Args:
      project_id: The Project ID
      query: The Cloud Datastore query to be read from.
      namespace: An optional namespace.
    """
    super(ReadFromDatastore, self).__init__()

    self._project = project
    # using _namespace conflicts with DisplayData._namespace
    self._datastore_namespace = namespace
    # TODO: Validate Query
    self._query = query

  class SplitQueryFn(DoFn):
    def __init__(self, project, query, num_splits, namespace=None):
      super(ReadFromDatastore.SplitQueryFn, self).__init__()
      self._project = project
      # using _namespace conflicts with DisplayData._namespace
      self._datastore_namespace = namespace
      self._query = query
      self._num_splits = num_splits

    def start_bundle(self, context):
      self._datastore = helper.get_datastore(self._project)

    def process(self, p_context, *args, **kwargs):
      # random key to be used to group query splits.
      key = 1
      query = p_context.element

      # If query has a user set limit, then the query cannot be split.
      if query.HasField('limit'):
        return [(key, query)]

      # Compute the estimated numSplits if not specified by the user.
      if self._num_splits <= 0:
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

  class ReadFn(DoFn):
    def __init__(self, project, namespace=None):
      super(ReadFromDatastore.ReadFn, self).__init__()
      self._project = project
      # using _namespace conflicts with DisplayData._namespace
      self._datastore_namespace = namespace
      self._client = None

    def start_bundle(self, context):
      self._client = helper.get_datastore(self._project)

    def process(self, p_context, *args, **kwargs):
      query = p_context.element
      # Returns a iterator of entities that reads is batches.
      entities = helper.fetch_entities(self._project, self._datastore_namespace,
                                       query, self._client)
      return entities

  def apply(self, pcoll):

    queries = (pcoll.pipeline
               | 'User Query' >> Create([self._query])
               | 'Split Query' >> ParDo(ReadFromDatastore.SplitQueryFn(
                   self._project, self._query, self._datastore_namespace)))

    sharded_queries = queries | GroupByKey() | Values() | FlatMap('flatten',
                                                                  lambda x: x)

    entities = sharded_queries | 'Read' >> ParDo(
        ReadFromDatastore.ReadFn(self._project, self._datastore_namespace))
    return entities

  @staticmethod
  def query_latest_statistics_timestamp(project, namespace, datastore):
    query = query_pb2.Query()
    if namespace is None:
      query.kind.add().name = '__Stat_Total__'
    else:
      query.kind.add().name = '__Stat_Ns_Total__'

    # Descending order of `timestamp`
    datastore_helper.add_property_orders(query, "-timestamp")
    # Only get the latest entity
    query.limit.value = 1

    req = helper.make_request(project, namespace, query)
    resp = datastore.run_query(req)
    if len(resp.batch.entity_results) == 0:
      raise RuntimeError("Datastore total statistics unavailable.")

    entity = resp.batch.entity_results[0].entity
    return datastore_helper.micros_from_timestamp(
        entity.properties['timestamp'].timestamp_value)

  @staticmethod
  def get_estimated_size_bytes(project, namespace, query, datastore):
    kind = query.kind[0].name
    latest_timestamp = ReadFromDatastore.query_latest_statistics_timestamp(
        project, namespace, datastore)
    logging.info('Latest stats timestamp for kind %s is %s',
                 kind, latest_timestamp)

    estimated_size_query = query_pb2.Query()
    if namespace is None:
      estimated_size_query.kind.add().name = '__Stat_Kind__'
    else:
      estimated_size_query.kind.add().name = '__Stat_Ns_Kind__'

    kind_filter = datastore_helper.set_property_filter(
        query_pb2.Filter(), 'kind_name', PropertyFilter.EQUAL, unicode(kind))
    timestamp_filter = datastore_helper.set_property_filter(
        query_pb2.Filter(), 'timestamp', PropertyFilter.EQUAL,
        latest_timestamp)

    datastore_helper.set_composite_filter(estimated_size_query.filter,
                                          CompositeFilter.AND, kind_filter,
                                          timestamp_filter)

    req = helper.make_request(project, namespace, estimated_size_query)
    resp = datastore.run_query(req)
    if len(resp.batch.entity_results) == 0:
      raise RuntimeError("Datastore statistics for kind %s unavailable" % kind)

    entity = resp.batch.entity_results[0].entity
    return datastore_helper.get_value(entity.properties['entity_bytes'])

  @staticmethod
  def get_estimated_num_splits(project, namespace, query, datastore):
    num_splits = 0
    try:
      estimated_size_bytes = \
        ReadFromDatastore.get_estimated_size_bytes(project, namespace, query,
                                                   datastore)
      logging.info('Estimated size bytes for query: %s', estimated_size_bytes)
      num_splits = int(min(ReadFromDatastore._NUM_QUERY_SPLITS_MAX, round(
          ((float(estimated_size_bytes)) /
           ReadFromDatastore._DEFAULT_BUNDLE_SIZE_BYTES))))

    except Exception as e:
      logging.warning('Failed to fetch estimated size bytes:\n%s', e)
      # Fallback in case estimated size is unavailable.
      num_splits = ReadFromDatastore._NUM_QUERY_SPLITS_MIN

    return max(num_splits, ReadFromDatastore._NUM_QUERY_SPLITS_MIN)

    # class WriteToDatastore(PTransform):
    #   DATASTORE_BATCH_SIZE = 500

    #   def __init__(self, project_id, namespace=None):
    #     self._project_id = project_id
    #     self._namespace = namespace

    #   class WriteFn(DoFn):
    #     def __init__(self, project_id, namespace=None):
    #       self._project_id = project_id
    #       self._namespace = namespace
    #       self._client = None
    #       self._current_batch = []

    #     def start_bundle(self, context):
    #       self._client = datastore.Client(self._project_id, self._namespace)

    #     def process(self, p_context, *args, **kwargs):
    #       entity = p_context.element
    #       if entity.key.is_partial:
    #         msg = ("Entities to be written to the Cloud Datastore"
    #                "must have complete keys: \n{0}".format(entity))
    #         raise ValueError(msg)

    #       self._current_batch.append(p_context.element)
    #       print p_context.element

    #       if len(self._current_batch) ==WriteToDatastore.DATASTORE_BATCH_SIZE:
    #         self.flush_batch()

    #     def finish_bundle(self, context):
    #       if len(self._current_batch) > 0:
    #         self.flush_batch()

    #     def flush_batch(self):
    #       self._client.put_multi(self._current_batch)
    #       self._current_batch = []

    #   def apply(self, pcoll):
    #     return (pcoll
    #             | 'Write to Datastore' >> ParDo(WriteToDatastore.WriteFn(
    #                                       self._project_id, self._namespace)))
    #
