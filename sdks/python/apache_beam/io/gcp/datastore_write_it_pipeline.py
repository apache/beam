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

"""A job that write Entries into Datastore.

The pipelines behave in the steps below.

  1. Create and write Entities to Datastore
  2. (Optional) If read limit was provided,
     read it and confirm that the expected Entities were read.
  3. Query the written Entities and verify result.
  4. Delete Entries.
  5. Query the written Entities, verify no results.
"""

from __future__ import absolute_import

import argparse
import hashlib
import logging
import uuid

import apache_beam as beam
from apache_beam.io.gcp.datastore.v1.datastoreio import DeleteFromDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import entity_pb2
  from google.cloud.proto.datastore.v1 import query_pb2
  from googledatastore import helper as datastore_helper
  from googledatastore import PropertyFilter
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position
# pylint: enable=ungrouped-imports


def new_pipeline_with_job_name(pipeline_options, job_name, suffix):
  """Create a pipeline with the given job_name and a suffix."""
  gcp_options = pipeline_options.view_as(GoogleCloudOptions)
  # DirectRunner doesn't have a job name.
  if job_name:
    gcp_options.job_name = job_name + suffix

  return TestPipeline(options=pipeline_options)


class EntityWrapper(object):
  """Create a Cloud Datastore entity from the given string."""

  def __init__(self, kind, namespace, ancestor):
    self._kind = kind
    self._namespace = namespace
    self._ancestor = ancestor

  def make_entity(self, content):
    """Create entity from given string."""
    entity = entity_pb2.Entity()
    if self._namespace is not None:
      entity.key.partition_id.namespace_id = self._namespace

    # All entities created will have the same ancestor
    datastore_helper.add_key_path(entity.key, self._kind, self._ancestor,
                                  self._kind, hashlib.sha1(content).hexdigest())

    datastore_helper.add_properties(entity, {'content': str(content)})
    return entity


def make_ancestor_query(kind, namespace, ancestor):
  """Creates a Cloud Datastore ancestor query."""
  ancestor_key = entity_pb2.Key()
  datastore_helper.add_key_path(ancestor_key, kind, ancestor)
  if namespace is not None:
    ancestor_key.partition_id.namespace_id = namespace

  query = query_pb2.Query()
  query.kind.add().name = kind

  datastore_helper.set_property_filter(
      query.filter, '__key__', PropertyFilter.HAS_ANCESTOR, ancestor_key)

  return query


def run(argv=None):
  """Main entry point."""

  parser = argparse.ArgumentParser()

  parser.add_argument('--kind',
                      dest='kind',
                      default='writereadtest',
                      help='Datastore Kind')
  parser.add_argument('--num_entities',
                      dest='num_entities',
                      type=int,
                      required=True,
                      help='Number of entities to write')
  parser.add_argument('--limit',
                      dest='limit',
                      type=int,
                      help='Limit of number of entities to write')

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  gcloud_options = pipeline_options.view_as(GoogleCloudOptions)
  job_name = gcloud_options.job_name
  kind = known_args.kind
  num_entities = known_args.num_entities
  project = gcloud_options.project
  # a random ancesor key
  ancestor = str(uuid.uuid4())
  query = make_ancestor_query(kind, None, ancestor)

  # Pipeline 1: Create and write the specified number of Entities to the
  # Cloud Datastore.
  logging.info('Writing %s entities to %s', num_entities, project)
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-write')

  # pylint: disable=expression-not-assigned
  (p
   | 'Input' >> beam.Create(list(range(known_args.num_entities)))
   | 'To String' >> beam.Map(str)
   | 'To Entity' >> beam.Map(EntityWrapper(kind, None, ancestor).make_entity)
   | 'Write to Datastore' >> WriteToDatastore(project))

  p.run()

  # Optional Pipeline 2: If a read limit was provided, read it and confirm
  # that the expected entities were read.
  if known_args.limit is not None:
    logging.info('Querying a limited set of %s entities and verifying count.',
                 known_args.limit)
    p = new_pipeline_with_job_name(pipeline_options, job_name, '-verify-limit')
    query_with_limit = query_pb2.Query()
    query_with_limit.CopyFrom(query)
    query_with_limit.limit.value = known_args.limit
    entities = p | 'read from datastore' >> ReadFromDatastore(project,
                                                              query_with_limit)
    assert_that(
        entities | beam.combiners.Count.Globally(),
        equal_to([known_args.limit]))

    p.run()

  # Pipeline 3: Query the written Entities and verify result.
  logging.info('Querying entities, asserting they match.')
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-verify')
  entities = p | 'read from datastore' >> ReadFromDatastore(project, query)

  assert_that(
      entities | beam.combiners.Count.Globally(),
      equal_to([num_entities]))

  p.run()

  # Pipeline 4: Delete Entities.
  logging.info('Deleting entities.')
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-delete')
  entities = p | 'read from datastore' >> ReadFromDatastore(project, query)
  # pylint: disable=expression-not-assigned
  (entities
   | 'To Keys' >> beam.Map(lambda entity: entity.key)
   | 'Delete keys' >> DeleteFromDatastore(project))

  p.run()

  # Pipeline 5: Query the written Entities, verify no results.
  logging.info('Querying for the entities to make sure there are none present.')
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-verify-deleted')
  entities = p | 'read from datastore' >> ReadFromDatastore(project, query)

  assert_that(
      entities | beam.combiners.Count.Globally(),
      equal_to([0]))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
