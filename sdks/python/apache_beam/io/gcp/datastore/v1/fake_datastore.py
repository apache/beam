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

"""Fake datastore used for unit testing.

For internal use only; no backwards-compatibility guarantees.
"""

from __future__ import absolute_import

import uuid
from builtins import range

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
  from google.cloud.proto.datastore.v1 import query_pb2
except ImportError:
  pass
# pylint: enable=wrong-import-order, wrong-import-position


def create_run_query(entities, batch_size):
  """A fake datastore run_query method that returns entities in batches.

  Note: the outer method is needed to make the `entities` and `batch_size`
  available in the scope of fake_run_query method.

  Args:
    entities: list of entities supposed to be contained in the datastore.
    batch_size: the number of entities that run_query method returns in one
                request.
  """
  def run_query(req):
    start = int(req.query.start_cursor) if req.query.start_cursor else 0
    # if query limit is less than batch_size, then only return that much.
    count = min(batch_size, req.query.limit.value)
    # cannot go more than the number of entities contained in datastore.
    end = min(len(entities), start + count)
    finish = False
    # Finish reading when there are no more entities to return,
    # or request query limit has been satisfied.
    if end == len(entities) or count == req.query.limit.value:
      finish = True
    return create_response(entities[start:end], str(end), finish)
  return run_query


def create_commit(mutations):
  """A fake Datastore commit method that writes the mutations to a list.

  Args:
    mutations: A list to write mutations to.

  Returns:
    A fake Datastore commit method
  """

  def commit(req):
    for mutation in req.mutations:
      mutations.append(mutation)

  return commit


def create_response(entities, end_cursor, finish):
  """Creates a query response for a given batch of scatter entities."""
  resp = datastore_pb2.RunQueryResponse()
  if finish:
    resp.batch.more_results = query_pb2.QueryResultBatch.NO_MORE_RESULTS
  else:
    resp.batch.more_results = query_pb2.QueryResultBatch.NOT_FINISHED

  resp.batch.end_cursor = end_cursor
  for entity_result in entities:
    resp.batch.entity_results.add().CopyFrom(entity_result)

  return resp


def create_entities(count, id_or_name=False):
  """Creates a list of entities with random keys."""
  entities = []

  for _ in range(count):
    entity_result = query_pb2.EntityResult()
    if id_or_name:
      entity_result.entity.key.path.add().id = (
          uuid.uuid4().int & ((1 << 63) - 1))
    else:
      entity_result.entity.key.path.add().name = str(uuid.uuid4())
    entities.append(entity_result)

  return entities
