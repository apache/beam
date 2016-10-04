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

import os

from google.cloud import datastore
from google.cloud.datastore.query import Query

import apache_beam as beam
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import Create
from apache_beam.transforms import ParDo

__all__ = ['ReadFromDatastore', 'WriteToDatastore']

class ReadFromDatastore(PTransform):
  """A ``PTransform`` for reading from Google Cloud Datastore."""

  def __init__(self, project_id, query, namespace=None):
    """Initialize the ReadFromDatastore transform.

    Args:
      project_id: The Project ID
      query: The Cloud Datastore query to be read from.
      namespace: An optional namespace.
    """
    super(ReadFromDatastore, self).__init__()

    self._project_id = project_id
    self._namespace = namespace

    # Validate Query
    self._query = query
    self._query._namespace = namespace

  class SplitQueryFn(DoFn):
    def __init__(self, project_id, query, namespace=None):
      super(ReadFromDatastore.SplitQueryFn, self).__init__()
      self._project_id = project_id
      self._namespace = namespace
      self._query = query

    def process(self, p_context, *args, **kwargs):
      query = p_context.element

      # TODO: Fix this when query splitter is available.
      # for split_query in query_splitter.split(query, num_splits):
      #   yield split_query

      return [query]

  class ReadFn(DoFn):
    def __init__(self, project_id, namespace=None):
      super(ReadFromDatastore.ReadFn, self).__init__()
      self._project_id = project_id
      self._namespace = namespace
      self._client = None

    def start_bundle(self, context):
      # Will the same DoFn be serialized / deserialized again?
      self._client = datastore.Client(self._project_id, self._namespace)


    def process(self, p_context, *args, **kwargs):
      query = p_context.element
      # TODO: Accessing the client variable directly is because of a limitation
      # in the current gcloud python lib and will be fixed soon.
      query._client = self._client
      return query.fetch()

  def apply(self, pcoll):
    return (pcoll.pipeline
            | 'User Query' >> Create([self._query])
            | 'Split Query' >> ParDo(ReadFromDatastore.SplitQueryFn(
                                    self._project_id, self._query,
                                    self._namespace))
            | 'Read' >> ParDo(ReadFromDatastore.ReadFn(self._project_id,
                              self._namespace)))

  @staticmethod
  def query(project, namespace):
    if not namespace:
      namespace = 'dummy'
    return Query(None, project=project, namespace=namespace)


class WriteToDatastore(PTransform):
  DATASTORE_BATCH_SIZE = 500

  def __init__(self, project_id, namespace=None):
    self._project_id = project_id
    self._namespace = namespace

  class WriteFn(DoFn):
    def __init__(self, project_id, namespace=None):
      self._project_id = project_id
      self._namespace = namespace
      self._client = None
      self._current_batch = []

    def start_bundle(self, context):
      self._client = datastore.Client(self._project_id, self._namespace)

    def process(self, p_context, *args, **kwargs):
      entity = p_context.element
      if entity.key.is_partial:
        msg = ("Entities to be written to the Cloud Datastore"
               "must have complete keys: \n{0}".format(entity))
        raise ValueError(msg)

      self._current_batch.append(p_context.element)
      print p_context.element

      if len(self._current_batch) == WriteToDatastore.DATASTORE_BATCH_SIZE:
        self.flush_batch()

    def finish_bundle(self, context):
      if len(self._current_batch) > 0:
        self.flush_batch()

    def flush_batch(self):
      self._client.put_multi(self._current_batch)
      self._current_batch = []

  def apply(self, pcoll):
    return (pcoll
            | 'Write to Datastore' >> ParDo(WriteToDatastore.WriteFn(
                                            self._project_id, self._namespace)))