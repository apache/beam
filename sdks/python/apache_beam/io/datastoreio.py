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
"""Implements a source for reading Avro files."""

import os

from google.cloud import datastore
from google.cloud.datastore.query import Query

import apache_beam as beam
from apache_beam.io.iobase import Read
from apache_beam.transforms import PTransform
from apache_beam.transforms import DoFn
from apache_beam.transforms import Create
from apache_beam.transforms import ParDo

__all__ = ['ReadFromDatastore']

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

  def apply(self, pcoll):
    return (pcoll.pipeline
            | 'User Query' >> Create([self._query])
            | 'Split Query' >> ParDo(ReadFromdDatastore.SplitQueryFn(self._project_id, self._query, self._namespace))
            | 'Read' >> ParDo(ReadFromdDatastore.ReadFn(self._project_id, self._namespace)))

  @staticmethod
  def query(project, namespace):
    if not namespace:
      namespace = 'dummy'
    return Query(None, project=project, namespace=namespace)

class SplitQueryFn(DoFn):
  def __init__(self, project_id, query, namespace=None):
    super(SplitQueryFn, self).__init__()
    self._project_id = project_id
    self._namespace = namespace
    self._query = query

  def process(self, p_context, *args, **kwargs):
    # TODO: split the query
    print 'Query is'
    print p_context.element
    return [p_context.element]  

  class ReadFn(DoFn):
    def __init__(self, project_id, namespace=None):
      super(ReadFn, self).__init__()
      self._project_id = project_id
      self._namespace = namespace
      self._client = None

    def start_bundle(self, context):
      print self._project_id
      print self._namespace
      # Will the same DoFn be serialized / deserialized again?
      self._client = datastore.Client(self._project_id, self._namespace)


    def process(self, p_context, *args, **kwargs):
      query = p_context.element
      print 'Read Fn Query is'
      print query.kind
      query._client = self._client
      return query.fetch()




