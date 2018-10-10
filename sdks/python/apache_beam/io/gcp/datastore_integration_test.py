
"""Datastore integration test that tests read, write and delete transforms.

This test creates entities and writes them to Cloud Datastore. Subsequently,
these entities are read from Cloud Datastore, compared to the expected value
for the entity, and deleted.

There is no output; instead, we use `assert_that` transform to verify that
results are as expected.
"""

from __future__ import absolute_import

import argparse
import hashlib
import logging
import unittest
import uuid

from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.gcp.datastore.v1.datastoreio import DeleteFromDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import ReadFromDatastore
from apache_beam.io.gcp.datastore.v1.datastoreio import WriteToDatastore
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from google.cloud.proto.datastore.v1 import entity_pb2
  from google.cloud.proto.datastore.v1 import query_pb2

  from googledatastore import helper as datastore_helper
  from googledatastore import PropertyFilter
except ImportError:
  datastore_pb2 = None


NUM_ENTITIES = 1001
LIMIT_ENTITIES = 500


@unittest.skipIf(datastore_pb2 is None, 'GCP dependencies are not installed')
class DatastoreIT(unittest.TestCase):

  def get_known_args(self,
                     entities=NUM_ENTITIES,
                     limit=LIMIT_ENTITIES,
                     kind=None):
    """Main entry point."""

    parser = argparse.ArgumentParser()

    parser.add_argument('--kind',
                        dest='kind',
                        default='writereadtest',
                        help='Datastore Kind')
    parser.add_argument('--num_entities',
                        dest='num_entities',
                        type=int,
                        default=NUM_ENTITIES,
                        required=True,
                        help='Number of entities to write')
    parser.add_argument('--limit',
                        dest='limit',
                        default=LIMIT_ENTITIES,
                        type=int,
                        help='Limit of number of entities to write')

    known_args, _ = parser.parse_known_args(
        ['--num_entities', str(entities),
         '--limit', str(limit),
         '--kind', str(kind)])
    return known_args

  @attr('IT')
  def test_datastore_functionality(self):
    self.known_args = self.get_known_args()
    self.ancestor = str(uuid.uuid4())
    self.query = make_ancestor_query(self.known_args.kind, None, self.ancestor)
    self._create_and_write_entities()
    self._read_with_limit()
    self._query_written_and_verify()
    self._delete_entities()
    self._verify_deleted()

  def _create_and_write_entities(self):
    # Pipeline 1: Create and write the specified number of Entities to the
    # Cloud Datastore.

    # pylint: disable=expression-not-assigned
    with TestPipeline(is_integration_test=True) as p:
      project = p.get_option('project')
      logging.info('Writing %s entities to %s',
                   self.known_args.num_entities,
                   project)
      (p
       | 'Input' >> beam.Create(list(range(self.known_args.num_entities)))
       | 'To String' >> beam.Map(str)
       | 'To Entity' >> beam.Map(
           EntityWrapper(self.known_args.kind,
                         None,
                         self.ancestor).make_entity)
       | 'Write to Datastore' >> WriteToDatastore(project))

  def _read_with_limit(self):
    # Optional Pipeline 2: If a read limit was provided, read it and confirm
    # that the expected entities were read.
    if self.known_args.limit is not None:
      logging.info('Querying a limited set of %s entities and verifying count.',
                   self.known_args.limit)
      with TestPipeline(is_integration_test=True) as p:
        project = p.get_option('project')
        query_with_limit = query_pb2.Query()
        query_with_limit.CopyFrom(self.query)
        query_with_limit.limit.value = self.known_args.limit
        entities = p | 'read from datastore' >> ReadFromDatastore(
            project, query_with_limit)
        assert_that(
            entities | beam.combiners.Count.Globally(),
            equal_to([self.known_args.limit]))

  def _query_written_and_verify(self):
    # Pipeline 3: Query the written Entities and verify result.
    logging.info('Querying entities, asserting they match.')
    with TestPipeline(is_integration_test=True) as p:
      project = p.get_option('project')
      entities = p | 'read from datastore' >> ReadFromDatastore(project,
                                                                self.query)

      assert_that(
          entities | beam.combiners.Count.Globally(),
          equal_to([self.known_args.num_entities]))

  def _delete_entities(self):
    # Pipeline 4: Delete Entities.
    logging.info('Deleting entities.')
    with TestPipeline(is_integration_test=True) as p:
      project = p.get_option('project')
      entities = p | 'read from datastore' >> ReadFromDatastore(project,
                                                                self.query)
      # pylint: disable=expression-not-assigned
      (entities
       | 'To Keys' >> beam.Map(lambda entity: entity.key)
       | 'Delete keys' >> DeleteFromDatastore(project))

  def _verify_deleted(self):
    # Pipeline 5: Query the written Entities, verify no results.
    logging.info(
        'Querying for the entities to make sure there are none present.')
    with TestPipeline(is_integration_test=True) as p:
      project = p.get_option('project')
      entities = p | 'read from datastore' >> ReadFromDatastore(project,
                                                                self.query)
      assert_that(
          entities | beam.combiners.Count.Globally(),
          equal_to([0]))


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


if __name__ == '__main__':
  unittest.main()
