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

# pytype: skip-file

import argparse
import hashlib
import logging
import uuid

import apache_beam as beam
from apache_beam.io.gcp.datastore.v1new.datastoreio import DeleteFromDatastore
from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1new.datastoreio import WriteToDatastore
from apache_beam.io.gcp.datastore.v1new.types import Entity
from apache_beam.io.gcp.datastore.v1new.types import Key
from apache_beam.io.gcp.datastore.v1new.types import Query
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

_LOGGER = logging.getLogger(__name__)


def new_pipeline_with_job_name(pipeline_options, job_name, suffix):
  """Create a pipeline with the given job_name and a suffix."""
  gcp_options = pipeline_options.view_as(GoogleCloudOptions)
  # DirectRunner doesn't have a job name.
  if job_name:
    gcp_options.job_name = job_name + suffix

  return TestPipeline(options=pipeline_options)


class EntityWrapper(object):
  """
  Create a Cloud Datastore entity from the given string.

  Namespace and project are taken from the parent key.
  """
  def __init__(self, kind, parent_key):
    self._kind = kind
    self._parent_key = parent_key

  def make_entity(self, content):
    """Create entity from given string."""
    key = Key([self._kind, hashlib.sha1(content.encode('utf-8')).hexdigest()],
              parent=self._parent_key)
    entity = Entity(key)
    entity.set_properties({'content': str(content)})
    return entity


def run(argv=None):
  """Main entry point."""

  parser = argparse.ArgumentParser()

  parser.add_argument(
      '--kind', dest='kind', default='writereadtest', help='Datastore Kind')
  parser.add_argument(
      '--num_entities',
      dest='num_entities',
      type=int,
      required=True,
      help='Number of entities to write')
  parser.add_argument(
      '--limit',
      dest='limit',
      type=int,
      help='Limit of number of entities to write')

  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_options = PipelineOptions(pipeline_args)
  gcloud_options = pipeline_options.view_as(GoogleCloudOptions)
  job_name = gcloud_options.job_name
  kind = known_args.kind
  num_entities = known_args.num_entities
  project = gcloud_options.project

  # Pipeline 1: Create and write the specified number of Entities to the
  # Cloud Datastore.
  ancestor_key = Key([kind, str(uuid.uuid4())], project=project)
  _LOGGER.info('Writing %s entities to %s', num_entities, project)
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-write')
  _ = (
      p
      | 'Input' >> beam.Create(list(range(num_entities)))
      | 'To String' >> beam.Map(str)
      | 'To Entity' >> beam.Map(EntityWrapper(kind, ancestor_key).make_entity)
      | 'Write to Datastore' >> WriteToDatastore(project, hint_num_workers=1))
  p.run()

  query = Query(kind=kind, project=project, ancestor=ancestor_key)
  # Optional Pipeline 2: If a read limit was provided, read it and confirm
  # that the expected entities were read.
  if known_args.limit is not None:
    _LOGGER.info(
        'Querying a limited set of %s entities and verifying count.',
        known_args.limit)
    p = new_pipeline_with_job_name(pipeline_options, job_name, '-verify-limit')
    query.limit = known_args.limit
    entities = p | 'read from datastore' >> ReadFromDatastore(query)
    assert_that(
        entities | beam.combiners.Count.Globally(),
        equal_to([known_args.limit]))

    p.run()
    query.limit = None

  # Pipeline 3: Query the written Entities and verify result.
  _LOGGER.info('Querying entities, asserting they match.')
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-verify')
  entities = p | 'read from datastore' >> ReadFromDatastore(query)

  assert_that(
      entities | beam.combiners.Count.Globally(), equal_to([num_entities]))

  p.run()

  # Pipeline 4: Delete Entities.
  _LOGGER.info('Deleting entities.')
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-delete')
  entities = p | 'read from datastore' >> ReadFromDatastore(query)
  _ = (
      entities
      | 'To Keys' >> beam.Map(lambda entity: entity.key)
      | 'delete entities' >> DeleteFromDatastore(project, hint_num_workers=1))

  p.run()

  # Pipeline 5: Query the written Entities, verify no results.
  _LOGGER.info('Querying for the entities to make sure there are none present.')
  p = new_pipeline_with_job_name(pipeline_options, job_name, '-verify-deleted')
  entities = p | 'read from datastore' >> ReadFromDatastore(query)

  assert_that(entities | beam.combiners.Count.Globally(), equal_to([0]))

  p.run()


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
