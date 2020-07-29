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

"""
Cloud Datastore client and test functions.

For internal use only; no backwards-compatibility guarantees.
"""

# pytype: skip-file

from __future__ import absolute_import

import os
import uuid
from builtins import range
from typing import List
from typing import Union

from cachetools.func import ttl_cache
from google.api_core import exceptions
from google.cloud import environment_vars
from google.cloud.datastore import client

from apache_beam.io.gcp.datastore.v1new import types

# https://cloud.google.com/datastore/docs/concepts/errors#error_codes
_RETRYABLE_DATASTORE_ERRORS = (
    exceptions.Aborted,
    exceptions.DeadlineExceeded,
    exceptions.InternalServerError,
    exceptions.ServiceUnavailable,
)


@ttl_cache(maxsize=128, ttl=3600)
def get_client(project, namespace):
  """Returns a Cloud Datastore client."""
  _client = client.Client(project=project, namespace=namespace)
  # Avoid overwriting user setting. BEAM-7608
  if not os.environ.get(environment_vars.GCD_HOST, None):
    _client.base_url = 'https://batch-datastore.googleapis.com'  # BEAM-1387
  return _client


def retry_on_rpc_error(exception):
  """A retry filter for Cloud Datastore RPCErrors."""
  return isinstance(exception, _RETRYABLE_DATASTORE_ERRORS)


def create_entities(count, id_or_name=False):
  """Creates a list of entities with random keys."""
  if id_or_name:
    ids_or_names = [
        uuid.uuid4().int & ((1 << 63) - 1) for _ in range(count)
    ]  # type: List[Union[str, int]]
  else:
    ids_or_names = [str(uuid.uuid4()) for _ in range(count)]

  keys = [types.Key(['EntityKind', x], project='project') for x in ids_or_names]
  return [types.Entity(key) for key in keys]


def create_client_entities(count, id_or_name=False):
  """Creates a list of client-style entities with random keys."""
  return [
      entity.to_client_entity()
      for entity in create_entities(count, id_or_name=id_or_name)
  ]
